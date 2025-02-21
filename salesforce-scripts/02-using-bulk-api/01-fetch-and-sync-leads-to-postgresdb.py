from dotenv import load_dotenv
import os
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from salesforce_bulk import SalesforceBulk
import warnings  # Import warnings module

# Suppress FutureWarning messages
warnings.filterwarnings("ignore", category=FutureWarning)

# Load environment variables
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password") + os.getenv("sf_security_token", "")
SF_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"

# PostgreSQL configuration
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

def get_salesforce_token():
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password
    }
    response = requests.post(SF_AUTH_URL, data=payload)
    if response.status_code == 200:
        data = response.json()
        print("✅ Successfully authenticated with Salesforce!")
        return data["access_token"], data["instance_url"].strip("/")
    else:
        print(f"❌ Authentication failed: {response.text}")
        exit(1)

def get_salesforce_bulk():
    access_token, instance_url = get_salesforce_token()
    domain = instance_url.replace("https://", "").strip("/")
    bulk = SalesforceBulk(sessionId=access_token, host=domain)
    return bulk, access_token, instance_url

def describe_salesforce_object(access_token, instance_url, object_name="Lead"):
    describe_url = f"{instance_url}/services/data/v58.0/sobjects/{object_name}/describe"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(describe_url, headers=headers)
    compound_fields = set()
    all_fields = set()
    if response.status_code == 200:
        data = response.json()
        print(f"🔍 Checking compound fields in {object_name} object...")
        for field in data["fields"]:
            all_fields.add(field["name"])
            if "compoundFieldName" in field and field["compoundFieldName"]:
                compound_fields.add(field["name"])
            if field["type"] in ["address", "location"]:
                compound_fields.add(field["name"])
            if "fields" in field and isinstance(field["fields"], list):
                compound_fields.add(field["name"])
        print(f"⚠️ Excluding {len(compound_fields)} compound fields: {compound_fields}")
        print("✅ Metadata retrieval complete.")
    else:
        print(f"❌ Failed to describe {object_name}: {response.text}")
    return all_fields - compound_fields

def fetch_leads_data(bulk, access_token, instance_url):
    valid_fields = describe_salesforce_object(access_token, instance_url, "Lead")
    fields_str = ", ".join(valid_fields)
    query = f"SELECT {fields_str} FROM Lead WHERE LastModifiedDate >= YESTERDAY"
    job = bulk.create_query_job("Lead", contentType="JSON")
    batch = bulk.query(job, query)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    print("✅ Batch processing completed.")
    results = []
    for result in bulk.get_all_results_for_query_batch(batch):
        json_str = result.read().decode("utf-8")
        json_data = json.loads(json_str)
        results.extend(json_data)
    print(f"📌 Retrieved {len(results)} lead records.")
    return results

def connect_to_postgres():
    try:
        conn = psycopg2.connect(**ep_stage_db_params)
        print("✅ Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

def get_postgres_schema(cursor, table_name="sf_leads"):
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    return {row[0]: row[1] for row in cursor.fetchall()}

def convert_df_types(df, schema):
    df = df.copy()
    df.columns = df.columns.str.lower()
    
    for col, pg_type in schema.items():
        if col not in df.columns:
            continue
        df.loc[:, col] = df[col].replace("", None)
        if "date" in pg_type or "timestamp" in pg_type:
            # Convert to string first to handle numeric or mixed input
            df.loc[:, col] = df[col].astype(str)
            # Handle Salesforce timestamps (assume ISO format or milliseconds)
            df.loc[:, col] = pd.to_datetime(df[col], format="%Y-%m-%dT%H:%M:%S.%fZ", errors="coerce", utc=True)
            # Replace NaT with None explicitly
            df.loc[:, col] = df[col].where(~df[col].isna(), None)
        elif pg_type in ["integer", "bigint"]:
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")
            df.loc[:, col] = df[col].where(~df[col].isna(), None)
        elif pg_type == "double precision":
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[:, col] = df[col].where(~df[col].isna(), None)
        elif pg_type == "boolean":
            df.loc[:, col] = df[col].apply(lambda x: True if str(x).lower() == "true" else False if str(x).lower() == "false" else None)
    return df

def save_to_postgres(df, table_name="sf_leads"):
    conn = connect_to_postgres()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        schema = get_postgres_schema(cursor, table_name)
        #print(f"🛠 PostgreSQL schema: {schema}")
        print(f"🛠 Number of columns in PostgreSQL table '{table_name}': {len(schema)}")
        valid_columns = [col for col in df.columns if col.lower() in schema]
        df = convert_df_types(df[valid_columns], schema)
        
        if 'id' in df.columns:
            df = df[df['id'].notnull()]
            print(f"🛠 Filtered to {len(df)} records with non-null 'id'.")
        
        records = [tuple(row) for row in df.to_numpy()]
        #print(f"🛠 Sample record: {records[0]}")  # Debugging
        columns = ", ".join(df.columns)
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != "id"])
        
        # Check how many IDs already exist in the table
        existing_ids_query = f"SELECT COUNT(id) FROM {table_name} WHERE id IN %s"
        cursor.execute(existing_ids_query, (tuple(df["id"].tolist()),))
        existing_count = cursor.fetchone()[0]
        
        # Insert/update data
        insert_query = f"""
            INSERT INTO {table_name} ({columns}) VALUES %s 
            ON CONFLICT (id) DO UPDATE SET {update_clause}
            RETURNING id
        """
        
        cursor.execute("BEGIN")  # Start transaction
        execute_values(cursor, insert_query, records)
        updated_count = cursor.rowcount  # Number of records affected
        conn.commit()
        
        new_records_count = len(df) - existing_count  # New records = Total - Existing
        updated_records_count = existing_count  # Existing ones were updated
        
        print(f"✅ {new_records_count} new records inserted.")
        print(f"🔄 {updated_records_count} existing records updated.")
        
    except Exception as e:
        print(f"❌ Error saving to PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    # Track start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    try:
        bulk, access_token, instance_url = get_salesforce_bulk()
        print("✅ Salesforce Bulk API connection established successfully!")
        leads_json = fetch_leads_data(bulk, access_token, instance_url)
        df = pd.DataFrame(leads_json)
        if df is not None and not df.empty:
            print(f"📋 DataFrame shape: {df.shape}")
            save_to_postgres(df)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        # Track end time and calculate duration
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Script ended at: {end_time}")
        print(f"Total duration: {duration}")

         # Check if the duration exceeds 15 minutes (AWS Lambda limit)
        if duration > timedelta(minutes=15):
            print("Warning: Script runtime exceeds 15 minutes!")
        else:
            print("Script completed within the 15-minute runtime limit.")

if __name__ == "__main__":
    main()