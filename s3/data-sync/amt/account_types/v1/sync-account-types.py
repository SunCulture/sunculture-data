import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
from io import BytesIO

# Load AWS credentials and config from .env
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_DATA_WAREHOUSE_BUCKET")

# MySQL connection config from .env
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_AMT_DB_HOST"),
    'user': os.getenv("MYSQL_AMT_DB_USER"),
    'password': os.getenv("MYSQL_AMT_DB_PASSWORD"),
    'database': os.getenv("MYSQL_AMT_DB_NAME"),
    'port': 3306
}

# S3 client
s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Table and batch config
TABLE_NAME = "account_types"
BATCH_SIZE = 5000

# Table fields
TABLE_FIELDS = [
    "id",
    "accountType",
    "createdAt",
    "updatedAt"
]

def establish_mysql_db_connection():
    """Establish a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise

def establish_connection_to_s3():
    """Establish a connection to S3."""
    try:
        # Test S3 connection by listing buckets (optional)
        s3.list_buckets()
        print("S3 connection established successfully.")
        return s3
    except Exception as e:
        print(f"Error connecting to S3: {e}")
        raise

def fetch_data_from_mysql():
    """Fetch data from MySQL database in batches."""
    connection = establish_mysql_db_connection()
    try:
        cursor = connection.cursor(dictionary=True)
        offset = 0
        while True:
            query = f"SELECT {', '.join(TABLE_FIELDS)} FROM {TABLE_NAME} LIMIT {BATCH_SIZE} OFFSET {offset}"
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                break
            yield pd.DataFrame(rows, columns=TABLE_FIELDS)
            offset += BATCH_SIZE
    except mysql.connector.Error as e:
        print(f"Error fetching data from MySQL: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def sync_data_to_s3():
    """Sync Data from MySQL to S3 in Parquet format."""
    try:
        # Verify S3 connection
        establish_connection_to_s3()
        
        # Sync data in batches
        batch_number = 1
        for df in fetch_data_from_mysql():
            # Convert to Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
            
            # Upload to S3
            s3_key = f"{TABLE_NAME}/batch_{batch_number}.parquet"
            parquet_buffer.seek(0)
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            print(f"Uploaded batch {batch_number} to s3://{S3_BUCKET}/{s3_key}")
            
            # Clean up
            parquet_buffer.close()
            batch_number += 1
        
        if batch_number == 1:
            print("No data found to sync.")
        else:
            print("Sync completed successfully.")
        
    except Exception as e:
        print(f"Error during sync: {e}")
        raise

if __name__ == "__main__":
    sync_data_to_s3()