import pyodbc
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# SQL Server Configuration
sql_server = 'YOUR_SQL_SERVER'
database = 'YOUR_DATABASE'
username = 'YOUR_USERNAME'
password = 'YOUR_PASSWORD'
table_name = 'YOUR_TABLE_NAME'
file_column = 'YOUR_FILE_COLUMN'  # Column containing file data (VARBINARY(MAX))
file_name_column = 'YOUR_FILE_NAME_COLUMN'  # Column containing file names

# Azure Storage Configuration
azure_connection_string = "YOUR_AZURE_STORAGE_CONNECTION_STRING"
container_name = "YOUR_CONTAINER_NAME"

# Connect to SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={database};UID={username};PWD={password}"
)
cursor = conn.cursor()

# Query to fetch file data and names
query = f"SELECT {file_column}, {file_name_column} FROM {table_name}"
cursor.execute(query)

# Azure Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Ensure the container exists
try:
    container_client.create_container()
except Exception as e:
    print(f"Container already exists or could not be created: {e}")

# Upload each file to Azure Blob Storage
for row in cursor.fetchall():
    file_data = row[0]
    file_name = row[1]

    if file_data and file_name:
        # Create a BlobClient for the file
        blob_client = container_client.get_blob_client(file_name)
        try:
            # Upload the file to Azure Blob Storage
            blob_client.upload_blob(file_data, overwrite=True)
            print(f"Uploaded: {file_name}")
        except Exception as e:
            print(f"Failed to upload {file_name}: {e}")

# Close connections
cursor.close()
conn.close()

print("All files uploaded successfully.")
