import json
import os

import mysql.connector
import requests
from dotenv import load_dotenv
from mysql.connector import Error

load_dotenv()

# JSF and Dispatch date actions
accounts_data = [
    # Paste the JSF entries here
    {
        "accountRef": "xxxx",
        "action": "jsf",
        "actionDate": "xxxx",
        "jsfId": "xxxx",
        "deviceId": "xxxx",
    }
    # Paste the Dispatch entries here
    {"accountRef": "xxxx", "action": "dispatch", "actionDate": "xxxx"},
]

# API endpoint
url = os.getenv("API_URL")
api_key = os.getenv("API_KEY")

# Database configuration
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "3306")  # Default MySQL port
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Validating environment variables
required_vars = {
    "API_URL": url,
    "API_KEY": api_key,
    "DB_HOST": db_host,
    "DB_NAME": db_name,
    "DB_USER": db_user,
    "DB_PASSWORD": db_password,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(
        f"Missing environment variables: {', '.join(missing_vars)}. Please set them in your .env file."
    )

headers = {
    "api_key": api_key,
    "Content-Type": "application/json",
}


def query_account_status(account_ref):
    """Query MySQL database to confirm account status change after upload."""
    if not account_ref:
        print("No account reference provided.")
        return []

    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        cursor = connection.cursor(dictionary=True)

        placeholders = ", ".join(["%s"] * len(account_ref))
        query = f"""
        SELECT id, accountRef, jsfDate, dispatchDate, status
        FROM accounts
        WHERE accountRef IN ({placeholders})
        ORDER BY accountRef
        """

        cursor.execute(query, account_ref)
        results = cursor.fetchall()

        cursor.close()
        connection.close()

        return results

    except Error as e:
        print(f"Error querying database: {str(e)}")
        return []

    except Exception as e:
        print(f"Unexpected error querying database: {str(e)}")
        return []


def print_account_status(accounts):
    """Print the status of accounts."""
    if not accounts:
        print("No accounts found in database.")
        return

    print("Account Status Verification:")
    print("-" * 80)
    print(
        f"{'ID':<6} {'Account Ref':<15} {'JSF Date':<12} {'Dispatch Date':<14} {'Status':<15}"
    )
    print("-" * 80)

    for account in accounts:
        print(
            f"{account['id']:<6} {account['accountRef']:<15} {str(account['jsfDate'] or 'None'):<12} {str(account['dispatchDate'] or 'None'):<14} {account['status']:<15}"
        )


successful_uploads = []
failed_uploads = []

print(f"Starting upload of {len(accounts_data)} account actions to {url}")
print("-" * 80)

for i, account in enumerate(accounts_data, 1):
    payload = {
        "accountRef": account["accountRef"],
        "action": account["action"],
        "actionDate": account["actionDate"],
    }

    if account["action"] == "jsf":
        payload["jsfId"] = account["jsfId"]
        payload["deviceId"] = account["deviceId"]

    try:
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 201:
            print(
                f"Successfully uploaded {account['accountRef']} ({account['action']}): {response.json()}"
            )
            successful_uploads.append(account)
        else:
            print(
                f"Failed to upload {account['accountRef']} ({account['action']}): {response.status_code} - {response.text}"
            )
            print(f"Response: {response.text}")
            print(f"Payload: {json.dumps(payload)}")
            failed_uploads.append(account)

    except requests.exceptions.RequestException as e:
        print(
            f"Request failed for {account['accountRef']} ({account['action']}): {str(e)}"
        )
        failed_uploads.append(account)

print(f"Upload Summary:")
print(f"Successful uploads: {len(successful_uploads)}")
print(f"Failed uploads: {len(failed_uploads)}")

if failed_uploads:
    print("Failed uploads details:")
    for account in failed_uploads:
        print(f"   - {account['accountRef']} ({account['action']})")

if successful_uploads:
    print(f"\n🔍 Verifying account statuses in database...")

    account_refs = list(set([account["accountRef"] for account in successful_uploads]))

    db_results = query_account_status(account_refs)

    if db_results:
        print_account_status(db_results)

    else:
        print("No accounts found in database for verification.")
else:
    print("No successful uploads to verify in the database.")
print("-" * 80)
print("Script execution completed.")
