from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable, XCom
from airflow.utils.db import provide_session
from airflow.models.xcom import XCom as XComModel
from airflow.utils.session import create_session
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
import clickhouse_connect

# Logging
logger = logging.getLogger(__name__)

# Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Function to get the date range for extraction
def get_date_range(**kwargs):
    """Determine whether this is the first run or a subsequent run,
    and return the appropriate date range."""

    is_first_run = Variable.get("total_first_run", default_var="true").lower() == "true"

    if is_first_run:
        logger.info("This is the first run, fetching historical data")

        days_to_fetch = int(Variable.get("total_historical_days", default_var="30"))
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_to_fetch)

        Variable.set("total_first_run", "false")

    else:
        logger.info("This is a subsequent run, fetching data since last successful run")

        # Get the last successful run date from XCom
        @provide_session
        def get_last_successful_date(session=None):
            last_date_record = (
                session.query(XComModel)
                .filter(
                    XComModel.dag_id == "total_fuel_data_pipeline",
                    XComModel.task_id == "record_successful_date",
                    XComModel.key == "last_successful_date",
                )
                .order_by(XComModel.execution_date.desc())
                .first()
            )

            if last_date_record and last_date_record.value:
                return datetime.fromisoformat(last_date_record.value)
            else:
                # If no record found, default to yesterday
                return datetime.now() - timedelta(days=1)

        start_date = get_last_successful_date()
        end_date = datetime.now()

    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")

    logger.info(f"Date range: {start_date_str} to {end_date_str}")

    return {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "is_first_run": is_first_run,
    }


# Function to record the current date as the last successful run
def record_successful_date(**kwargs):
    """Record the current date as the last successful run date."""
    current_date = datetime.now().isoformat()
    logger.info(f"Recording successful run date: {current_date}")

    return current_date


# Extract data from Total portal
def extract_total_data(**kwargs):
    logger.info("Starting data extraction from Total Portal")

    # Get date range from earlier task
    ti = kwargs["ti"]
    date_range = ti.xcom_pull(task_ids="get_date_range")
    start_date_str = date_range["start_date"]
    end_date_str = date_range["end_date"]
    is_first_run = date_range["is_first_run"]

    logger.info(f"Extracting data from {start_date_str} to {end_date_str}")

    downloads_dir = "/opt/airflow/downloads"
    os.makedirs(downloads_dir, exist_ok=True)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.binary_location = "/usr/bin/chromium"  # Use Chromium

    # Donwload preferences
    prefs = {
        "download.default_directory": downloads_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Chrome webdriver
    driver = webdriver.Chrome(
        executable_path="/usr/bin/chromedriver", options=chrome_options
    )

    try:
        logger.info("Navigating to Total portal")

        driver.get("https://www.mytotalfuelcard.com/Client/app/index.html")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        username = Variable.get("total_username", default_var="YOUR_USERNAME")
        password = Variable.get("total_password", default_var="YOUR_PASSWORD")

        logging.info(f"Logging in as {username}")

        driver.find_element(By.ID, "username").send_keys(username)
        driver.find_element(By.ID, "password").send_keys(password)
        driver.find_element(By.ID, "login-button").click()

        # Wait for dashboard to load and navigate to transactions tab
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//a[contains(@href, 'transactions')]")
            )
        )
        driver.find_element(By.XPATH, "//a[contains(@href, 'transactions')]").click()

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%d/%m/%Y")

        logger.info(f"Setting date range: {yesterday_str}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "begin"))
        )
        begin_date = driver.find_element(By.NAME, "begin")
        begin_date.clear()
        begin_date.send_keys(yesterday_str)

        end_date = driver.find_element(By.NAME, "end")
        end_date.clear()
        end_date.send_keys(yesterday_str)

        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[contains(@class, 'export')]")
            )
        )
        driver.find_element(By.XPATH, "//button[contains(@class, 'export')]").click()

        logger.info("Waiting for CSV download to complete")

        wait_time = 30 if is_first_run else 10
        time.sleep(wait_time)

        # Confirming downloaded file
        files = os.listdir(downloads_dir)
        csv_files = [f for f in files if f.endswith(".csv") and "total" in f.lower()]

        if not csv_files:
            logger.error("No CSV files found in the download directory")
            raise Exception("Download failed: No CSV files found")

        latest_file = max(
            [os.path.join(downloads_dir, f) for f in csv_files], key=os.path.getctime
        )
        logger.info(f"Donwloaded file: {latest_file}")

        return latest_file

    except Exception as e:
        logger.error(f"Error in data extraction: {str(e)}")
        raise
    finally:
        driver.quit()


def check_existing_data(client, data):
    """Check for existing records to avoid duplicates."""
    checked_data = []

    for record in data:
        # Extract key identifiers
        card_number = record[6]  # card_number
        transaction_datetime = record[2]  # transaction_datetime
        receipt_number = record[8]  # receipt_number

        # Check if this transaction already exists
        query = f"""
        SELECT 1 
        FROM fuel_db.fuel_transactions 
        WHERE card_number = '{card_number}' 
        AND transaction_datetime = '{transaction_datetime}' 
        AND receipt_number = '{receipt_number}'
        LIMIT 1
        """

        result = client.execute(query)

        # If no results, this is a new record
        if not result:
            checked_data.append(record)

    return checked_data


def load_to_clickhouse(**kwargs):
    ti = kwargs["ti"]
    csv_path = ti.xcom_pull(task_ids="extract_total_data")
    date_range = ti.xcom_pull(task_ids="get_date_range")
    is_first_run = date_range["is_first_run"]

    logger.info(f"Loading data from {csv_path} to ClickHouse")

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} records from CSV")

        if df.empty:
            logger.info("No data to load")
            return "No data to load"

        df.columns = [
            col.replace(" ", "_").replace(".", "").replace("num", "number").lower()
            for col in df.columns
        ]

        logger.info("Processing date and time columns")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

        if "hour" in df.columns:
            df["hour"] = df["hour"].astype(str).str.replace(r"\.0$", "", regex=True)

            if "date" in df.columns:
                df["transaction_datetime"] = pd.to_datetime(
                    df["date"].dt.strftime("%Y-%m-%d") + " " + df["hour"],
                    errors="coerce",
                )

        if "invoice_date" in df.columns:
            df["invoice_date"] = pd.to_datetime(
                df["invoice_date"], format="%d/%m/%Y", errors="coerce"
            )

        numeric_columns = [
            "current_mileage",
            "past_mileage",
            "unit_price",
            "quantity",
            "amount",
            "balance",
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="default",
            password="clickhouse",
        )

        data = []

        for _, row in df.iterrows():

            if "transaction_datetime" in df.columns and pd.notna(
                row.get("transaction_datetime")
            ):
                transaction_dt = row.get("transaction_datetime")
            elif "date" in df.columns and pd.notna(row.get("date")):
                date_str = row.get("date").strftime("%Y-%m-%d")
                hour_str = row.get("hour", "00:00:00")
                transaction_dt = pd.to_datetime(
                    f"{date_str} {hour_str}", errors="coerce"
                )
                if pd.isna(transaction_dt):
                    transaction_dt = datetime.now()
            else:
                transaction_dt = datetime.now()

            # Get invoice_date
            if "invoice_date" in df.columns and pd.notna(row.get("invoice_date")):
                invoice_dt = row.get("invoice_date").date()
            else:
                invoice_dt = datetime.now().date()

            data_tuple = (
                str(row.get("customer_number", "")),
                str(row.get("customer", "")),
                transaction_dt,
                str(row.get("driver_code", "")),
                str(row.get("registration_number", "")),
                str(row.get("card_type", "")),
                str(row.get("card_number", "")),
                str(row.get("card_name", "")),
                str(row.get("receipt_number", "")),
                float(row.get("past_mileage", 0)),
                float(row.get("current_mileage", 0)),
                str(row.get("operation_type", "")),
                str(row.get("product_code", "")),
                str(row.get("product", "")),
                float(row.get("unit_price", 0)),
                float(row.get("quantity", 0)),
                float(row.get("amount", 0)),
                str(row.get("currency_number", "")),
                str(row.get("currency", "")),
                float(row.get("balance", 0)),
                str(row.get("station_number", "")),
                str(row.get("place", "")),
                invoice_dt,
                str(row.get("invoice_number", "")),
                datetime.now(),  # created_at
            )
            data.append(data_tuple)

        # For non-first runs, check for duplicates
        if not is_first_run:
            logger.info("Checking for duplicate records to avoid re-insertion")
            data = check_existing_data(client, data)
            logger.info(f"Found {len(data)} new records to insert")

        if data:
            column_names = [
                "customer_number",
                "customer",
                "transaction_datetime",
                "driver_code",
                "registration_number",
                "card_type",
                "card_number",
                "card_name",
                "receipt_number",
                "past_mileage",
                "current_mileage",
                "operation_type",
                "product_code",
                "product",
                "unit_price",
                "quantity",
                "amount",
                "currency_number",
                "currency",
                "balance",
                "station_number",
                "place",
                "invoice_date",
                "invoice_number",
                "created_at",
            ]

            client.insert("fuel_db.fuel_transactions", data, column_names=column_names)

            logger.info(f"Successfully loaded {len(data)} records to ClickHouse")
            return f"Loaded {len(data)} records to ClickHouse"
        else:
            logger.info("No new records to insert")
            return "No new records to insert"

    except Exception as e:
        logger.error(f"Error loading to ClickHouse: {str(e)}")
        raise

    finally:
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logger.info(f"Removed file: {csv_path}")


# Setting up a Clickhouse database and table
def setup_clickhouse():
    try:

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,  # Note: using HTTP port instead of native protocol
            username="default",
            password="clickhouse",
        )

        client.command("CREATE DATABASE IF NOT EXISTS fuel_db")

        client.command(
            """
            CREATE TABLE IF NOT EXISTS fuel_db.fuel_transactions (
                customer_number String,
                customer String,
                transaction_datetime DateTime,
                driver_code String,
                registration_number String,
                card_type String,
                card_number String,
                card_name String,
                receipt_number String,
                past_mileage Float64,
                current_mileage Float64,
                operation_type String,
                product_code String,
                product String,
                unit_price Float64,
                quantity Float64,
                amount Float64,
                currency_number String,
                currency String,
                balance Float64,
                station_number String,
                place String,
                invoice_date Date,
                invoice_number String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (transaction_datetime, card_number)
            PARTITION BY toYYYYMM(transaction_datetime)
        """
        )
        logger.info("Clickhouse database and table setup complete")
        return "ClickHouse setup successful"

    except Exception as e:
        logger.error(f"Error setting up ClickHouse: {str(e)}")
        raise


# DAG

with DAG(
    "total_fuel_data_pipeline",
    default_args=default_args,
    description="Extract Total fuel card data and load to ClickHouse",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM
    start_date=datetime(2025, 5, 1),
    catchup=False,
) as dag:

    # Create ClickHouse table
    create_clickhouse_table = PythonOperator(
        task_id="create_clickhouse_table",
        python_callable=setup_clickhouse,
    )

    # Determine date range based on first run status
    get_date_range_task = PythonOperator(
        task_id="get_date_range",
        python_callable=get_date_range,
    )

    # Extract data from Total portal
    extract_task = PythonOperator(
        task_id="extract_total_data",
        python_callable=extract_total_data,
    )

    # Load data to ClickHouse
    load_clickhouse_task = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
    )

    # Record successful date for next run
    record_date_task = PythonOperator(
        task_id="record_successful_date",
        python_callable=record_successful_date,
    )

    # Set task dependencies
    (
        create_clickhouse_table
        >> get_date_range_task
        >> extract_task
        >> load_clickhouse_task
        >> record_date_task
    )
