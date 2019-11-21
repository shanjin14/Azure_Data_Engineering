import xlrd
import unicodecsv
from datetime import datetime,timedelta
import re
import logging
import os

#helper to convert excel to csv
async def xls2csv (xls_filename, csv_filepath,strSheetList):
    # xls_filename: source excel
    # csv_filepath: csv output filepath
    # strSheetList: all for all sheets, or list of sheets split by comma
    # Converts an Excel file to a CSV file.
    # If the excel file has multiple worksheets, only the first worksheet is converted.
    # Uses unicodecsv, so it will handle Unicode characters.
    # Uses a recent version of xlrd, so it should handle old .xls and new .xlsx equally well.

    CSVFIleNamePrefix = os.path.splitext(os.path.basename(xls_filename))[0]
    CSVFIleNamePrefix = re.sub('[^A-Za-z0-9]+', '', CSVFIleNamePrefix)
    wb = xlrd.open_workbook(xls_filename)
    logging.info("create file in {}".format(csv_filepath))
    if strSheetList =="all":
        lstSheetList =wb.sheet_names()
    else:
        lstSheetList = strSheetList.split(",")
    
    for tab in lstSheetList:
        logging.info('Process Excel Tab {}'.format(str(tab)))
        sh = wb.sheet_by_name(str(tab))
        fh = open(csv_filepath+"/"+str(CSVFIleNamePrefix)+"_"+str(tab)+".csv","wb")
        logging.info('Create CSV File :'+csv_filepath+"/"+str(CSVFIleNamePrefix)+"_"+str(tab)+".csv")
        csv_out = unicodecsv.writer(fh, encoding='utf-8')

        for row_number in range (sh.nrows):
            parsed_item = sh.row_values(row_number)
            parsed_item = [str(x).replace(",", "") if isinstance(x,str) else x for x in parsed_item]
            parsed_item = [str(x).replace("'", "") if isinstance(x,str) else x for x in parsed_item]
            parsed_item = [str(x).replace("\n", " ") if isinstance(x,str) else x for x in parsed_item]
            parsed_item = [str(x).replace("\\", " ") if isinstance(x,str) else x for x in parsed_item]
            parsed_item = [str(x).replace("/", "-") if isinstance(x,str) else x for x in parsed_item]    
            parsed_item = [str(x).encode('ascii', 'ignore').decode() if isinstance(x,str) else x for x in parsed_item]
            csv_out.writerow(parsed_item)
        fh.close()

def generate_progress_callback(blob_name):
    def progress_callback(current, total):
        logging.info('({}, {}, {})'.format(blob_name, current, total))
    return progress_callback

#Helper for move file from one container to another
async def copy_azure_files(blob_service,blob_name,copy_from_container,copy_to_container):
    strdatetime=datetime.today().strftime('%Y%m%d%H%M')
    strFilePathList = re.findall(r'.*/', blob_name)
    strFileName= re.sub(r'.*/','',blob_name)
    updated_name=str(strFilePathList[0])+"_"+strdatetime+"/"+strFileName
    logging.info("{} | {} | {}".format(strFileName,str(strFilePathList[0]),updated_name))
    blob_url = blob_service.make_blob_url(copy_from_container, blob_name)
    blob_service.copy_blob(copy_to_container, updated_name, blob_url)
        #for move the file use this line
    blob_service.delete_blob(copy_from_container, blob_name)

async def process_excel_in_blob (blob_service,CONTAINERNAME,BLOBNAME,strFileNameInTemp,EXCELINPUTTEMPPATH,CSVOUTPUTTEMPPATH,strSheetList,TARGETFOLDERNAME,UPLOADCONTAINERNAME):
    logging.info('Download blob {}'.format(str(BLOBNAME)))
    try:
        blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,EXCELINPUTTEMPPATH+"/"+strFileNameInTemp,
                        progress_callback=generate_progress_callback(BLOBNAME),timeout=15)
    except AzureException as AzureEx:
        logging.info ('Azure Ex{}'.format(str(AzureEx)))

    # convert to csv
    await xls2csv(EXCELINPUTTEMPPATH+"/"+strFileNameInTemp,CSVOUTPUTTEMPPATH,strSheetList)
            
    #upload the file to blob storage
    for files in os.listdir(CSVOUTPUTTEMPPATH):
        logging.info('file to be uploaded {}'.format(TARGETFOLDERNAME+"/"+str(files)))
        blob_service.create_blob_from_path(UPLOADCONTAINERNAME, TARGETFOLDERNAME+"/"+files , CSVOUTPUTTEMPPATH+"/"+files)
        #Remove the file locally
        os.remove(CSVOUTPUTTEMPPATH+"/"+files)
        #Remove the xlsx file  downloaded
    os.remove(EXCELINPUTTEMPPATH+"/"+strFileNameInTemp)
         # Copy file from rawinput to archive
    logging.info("copy the file to archive folder")
    await copy_azure_files(blob_service,BLOBNAME,CONTAINERNAME,"db-input-archive")


#Helper for delete all files in one container
def delete_azure_files_in_container(blob_service,target_container,target_folder):
    generator = blob_service.list_blobs(target_container,prefix=target_folder)
    for blob in generator:
        if target_folder in blob.name:
            blob_service.delete_blob(target_container, blob.name)

#helper for checking if the container is created by ondemand hdinsight:
def CheckHDInsightContainer(inputstring,linkedservicename,datafactoryname,daydiff):
    if linkedservicename in inputstring and datafactoryname in inputstring:
        outputlist = re.findall(r'\d{14}', inputstring)
        outputint = int(outputlist[0])

        TodayDate = datetime.today()
        RngEndDate = TodayDate  + timedelta(days=daydiff)
        StartDateDiff = -60+daydiff
        RngStartDate = TodayDate  + timedelta(days=StartDateDiff)
        startdateint =int(RngStartDate.strftime('%Y%m%d000000')) 
        enddateint =int(RngEndDate.strftime('%Y%m%d999999'))

        if outputint>=startdateint and outputint <=enddateint:
            return True
        else:
            return False

#helper for remove the historical temporary hdinisight container
def RemoveHDInsightContainer (blob_service,ContainerName,Prefix,linkedservicename,datafactoryname,daydiff):
    if ContainerName is None:
        # Run standard removal any hdinsight container created that are -7 days and before
        ContainerList = blob_service.list_containers(prefix=Prefix)
        for container in ContainerList:
            blnCheckResult = CheckHDInsightContainer(container.name,linkedservicename,datafactoryname,daydiff)
            print(container.name+" "+str(blnCheckResult))
            if blnCheckResult == True:
                blob_service.delete_container(container.name)

    else:
        blob_service.delete_container(ContainerName)
