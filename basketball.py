import boto3
import json
import os
import requests
import time
from botocore.exceptions import ClientError

# Load API Key from .env
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("SPORTS_DATA_API_KEY")
NBA_ENDPOINT = os.getenv("NBA_ENDPOINT")

# AWS Configurations
bucket_name = "sports-analytics-data-lake-osagie123"
database_name = "glue_nba_data_lake"
table_name = "nba_players"
s3_client = boto3.client("s3")
glue_client = boto3.client("glue")
athena_client = boto3.client("athena")
s3_output_location = f"s3://{bucket_name}/athena-results/"

# Step 1: Create S3 Bucket (if not exists)
def create_s3_bucket():
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"S3 bucket '{bucket_name}' already exists. Skipping creation.")
        else:
            print(f"Error creating S3 bucket: {e}")

# Step 2: Fetch NBA Data
def fetch_nba_data():
    try:
        headers = {"Ocp-Apim-Subscription-Key": API_KEY}
        response = requests.get(NBA_ENDPOINT, headers=headers)
        response.raise_for_status()
        print("Fetched NBA data successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NBA data: {e}")
        return None

# Step 3: Upload Data to S3
def upload_to_s3(data):
    try:
        file_key = "raw-data/nba_player_data.jsonl"
        json_lines = "\n".join(json.dumps(record) for record in data)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json_lines)
        print(f"Uploaded data to S3: {file_key}")
    except ClientError as e:
        print(f"Error uploading data to S3: {e}")

# Step 4: Create Glue Database (if not exists)
def create_glue_database():
    existing_databases = glue_client.get_databases()["DatabaseList"]
    if not any(db["Name"] == database_name for db in existing_databases):
        glue_client.create_database(DatabaseInput={"Name": database_name, "Description": "NBA Data Lake"})
        print(f"Glue database '{database_name}' created successfully.")
    else:
        print(f"Glue database '{database_name}' already exists. Skipping creation.")

# Step 5: Create Glue Table (if not exists)
def create_glue_table():
    existing_tables = glue_client.get_tables(DatabaseName=database_name)["TableList"]
    if not any(tbl["Name"] == table_name for tbl in existing_tables):
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table '{table_name}' created successfully.")
    else:
        print(f"Glue table '{table_name}' already exists. Skipping creation.")

# Step 6: Configure Athena Query Output
def configure_athena():
    try:
        athena_client.start_query_execution(
            QueryString="SELECT 1", 
            QueryExecutionContext={"Database": database_name},
            ResultConfiguration={"OutputLocation": s3_output_location}
        )
        print("Athena output location configured successfully.")
    except ClientError as e:
        print(f"Error configuring Athena: {e}")

# Run the Steps
print("\nSetting up data lake for NBA sports analytics...\n")

create_s3_bucket()
nba_data = fetch_nba_data()
if nba_data:
    upload_to_s3(nba_data)
create_glue_database()
create_glue_table()
configure_athena()

print("\nData lake setup complete.\n")
