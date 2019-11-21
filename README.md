# Azure_Data_Engineering
Azure Function created to be called in ADF

# Azure Function Code Introduction
It is using the ADF Blob Storage package and few other packages to read the data from Azure Blob Storage, perform excel to csv conversion and method to move the data into Postgres SQL (Upsert or Insert)


# Azure Data Factory 
After all the Azure Function is created, you can subsequently call them inside the ADF using the activity -"Azure Function"


![ADF Pipeline calling Azure Function Example](https://github.com/shanjin14/Azure_Data_Engineering_Azure_Function/blob/master/ADF_Pipeline.JPG)
