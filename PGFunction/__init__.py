import logging
import azure.functions as func
import time
import psycopg2 as pg
import tempfile
import os
import json
from datetime import datetime,timedelta
import re
import shutil
from azure.storage.blob import BlockBlobService
from SharedCode.HelperFunction import delete_azure_files_in_container, \
    xls2csv,copy_azure_files, \
        CheckHDInsightContainer,RemoveHDInsightContainer
from SharedCode.PGHelperFunction import upsert


# Csv in blob to Postgres
async def BlobToPostgres(From_Container,From_Folder,To_PGTable,To_DBName,UpdateType="upsert"):
    try:
        output=""
        logging.info('BlobToPostgres Method Starts')
        STORAGEACCOUNTNAME= os.environ['BLOB_ACCOUNT_NAME']
        STORAGEACCOUNTKEY= os.environ['BLOB_FUNCTION_KEY']
        CONTAINERNAME= From_Container
        FOLDERNAME=From_Folder
        TEMPPATH= str(tempfile.gettempdir())
        HIVEINPUTTEMPPATH = TEMPPATH+"/hiveinput/"+FOLDERNAME
        logging.info('set initial blob parameters done')

        # Update connection string information obtained from the portal
        host = os.environ['PGSQL_HOST']
        user = os.environ['PGSQL_USER']
        dbname = To_DBName
        password = os.environ['PGSQL_PASSWORD']
        sslmode = "require"
        # Construct connection string
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        logging.info('set initial connection string parameters done')
        
        # Connect to postgres to upload
        conn = pg.connect(conn_string) 
        logging.info('Complete Connection')
        cursor = conn.cursor()

        #download from blob
        t1=time.time()
        blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
        logging.info('Connected to blob')
        generator = blob_service.list_blobs(CONTAINERNAME,prefix=FOLDERNAME)
        blobcount = 0
        for blob in generator:
            BLOBNAME= blob.name
            # Check if it's the specified folder we are looking for to upload
            # If it's not move to next one
            if "placeholder.txt" in blob.name:
                continue
            strFileNameInTemp= re.sub(r'.*/','',BLOBNAME)
            #create folder in temp path
            shutil.rmtree(HIVEINPUTTEMPPATH, ignore_errors=True)
            os.makedirs(HIVEINPUTTEMPPATH)
            logging.info('hiveinput temp folder created: {}'.format(HIVEINPUTTEMPPATH))
            #create folder in temp path
            LOCALFILENAME= HIVEINPUTTEMPPATH+"/"+strFileNameInTemp
            logging.info("Blob Name is {} ; Local File Name is {}".format(BLOBNAME,LOCALFILENAME))
            blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
            
            if str(UpdateType).lower() == "insert":
                with open(LOCALFILENAME, 'r+') as f:
                    cursor.copy_from(f, To_PGTable, sep='|')
            elif str(UpdateType).lower() == "upsert":
                with open(LOCALFILENAME, 'r+') as f:
                    #(cursor, table_name,csvinput)
                    await upsert(cursor,To_PGTable,f,separator = '|')
            logging.info('Complete Upload to Postgres')
            os.remove(LOCALFILENAME)
            copy_azure_files(blob_service,BLOBNAME,CONTAINERNAME,"db-input-archive")
            logging.info('Complete move file to archive')
            blobcount = blobcount+1
        t2=time.time()
        conn.commit()
        cursor.close()
        conn.close()

        if blobcount > 0:
            output = "success. Time Taken: "+str(t2-t1)+"."
        else:
        # no blob being processed
            output ="Error : No file being generated in folder for postgres upload"
        # Cleanup
    except pg.Error as e:
        output = "Postgres Error :"+str(e) +output #Append the time recorded
    except Exception as Ex :
        output = "error :"+str(Ex) +output #Append the time recorded
    return output

async def CallStoredProc(DBName,ProcedureName):
    try:
        # the code is designed for stored procedure with no input. 
        # All parameters are encapsulated in stored proc and managed within Postgres
        output=""
        t1=time.time()
        logging.info('CallStoredProc Method Starts')
        # Update connection string information obtained from the portal
        host = os.environ['PGSQL_HOST']
        user = os.environ['PGSQL_USER']
        dbname = DBName
        password = os.environ['PGSQL_PASSWORD']
        sslmode = "require"
        # Construct connection string
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        logging.info('set initial connection string parameters done')
        # Connect to postgres to upload
        conn = pg.connect(conn_string) 
        logging.info('Complete Connection')
        cursor = conn.cursor() 
        cursor.callproc(ProcedureName)
        # process the result set
        row = cursor.fetchone()
        if row is not None:
            output = str(row)
        cursor.close()
        conn.close()
        t2=time.time()
        output = "success. Time Taken: "+str(t2-t1)+". "+output
    except pg.Error as e:
        output = "Postgres Error :"+str(e) +output #Append the error recorded
    except Exception as Ex :
        output = "error :"+str(Ex) +output #Append the error recorded
    return output

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request for upload to Azure Postgres.')

    step_param = req.params.get('step')
    try:
        if step_param is not None:
            if step_param == "blobtopostgres":
                # read parameter from post request body 
                step_body = req.get_json()
                step_param_from_container=step_body["from_container"]
                step_param_from_folder=step_body["from_folder"]
                step_param_to_pgtable=step_body["to_pgtable"]
                step_param_to_pgdb=step_body["to_DBName"]
                step_param_update_type = step_body["update_type"]
                StepExecutionResult = await BlobToPostgres(step_param_from_container,step_param_from_folder,step_param_to_pgtable,step_param_to_pgdb,step_param_update_type)
                returnstring =StepExecutionResult
                jsonresponse = {'out': returnstring }
                if "success" in returnstring:
                    return func.HttpResponse(json.dumps(jsonresponse), mimetype="application/json")
                else:
                    return func.HttpResponse(json.dumps(jsonresponse),status_code=400, mimetype="application/json")

            elif step_param == "callstoredproc":
                step_body = req.get_json()
                step_param_dbname=step_body["dbname"]
                step_param_procname=step_body["procedurename"]
                StepExecutionResult = await CallStoredProc(step_param_dbname,step_param_procname)
                # return the output
                returnstring =StepExecutionResult
                jsonresponse = {'out': returnstring }
                if "success" in returnstring:
                    return func.HttpResponse(json.dumps(jsonresponse), mimetype="application/json")
                else:
                    return func.HttpResponse(json.dumps(jsonresponse),status_code=400, mimetype="application/json")
                           
    except Exception as Ex:
        #return the exception message
        returnstring =str(Ex)
        jsonresponse = {'out': returnstring }
        return func.HttpResponse(json.dumps(jsonresponse),status_code=400, mimetype="application/json")

    returnstring ="Please pass the body parameter correctly. Input value is "+str(step_param)
    jsonresponse = {'out': returnstring }
    return func.HttpResponse(
         json.dumps(jsonresponse),
         status_code=400
         , mimetype="application/json"
    )
