# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "c00d0e72-cdbc-b1bf-4088-b2ba795d144b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

# Fetch password from Azure Key Vault
password = mssparkutils.credentials.getSecret('https://kv-odl-maq.vault.azure.net/', 'Azure-SQLDB-Password')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(password)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc
import pandas as pd

servername = 'pragmaticworkspublic.database.windows.net'
databasename = 'AdventureWorksDW'
username = 'PWStudent'

connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{servername},1433;Database={databasename};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

try:
    conn = pyodbc.connect(connection_string)
    query_string = 'SELECT * FROM DimCustomer'  # Replace with your query
    df = pd.read_sql(query_string, conn)
    conn.close()
    display(df)
except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    if sqlstate == 'FA004':
        print("Authentication error. Check credentials or firewall rules.")
    else:
        print(f"Connection failed: {ex}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
