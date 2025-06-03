import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from io import StringIO
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def set_date_filter(driver, start_date_str, end_date_str):
    """Set the date filter with better error handling and verification."""
    logger.info(f"Setting date filters: {start_date_str} to {end_date_str}")

    try:
        # Find date fields
        begin_date = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "dp_date_debut"))
        )

        end_date = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "dp_date_fin"))
        )

        # Clear existing values
        begin_date.clear()
        end_date.clear()

        # Enter new values
        begin_date.send_keys(start_date_str)
        end_date.send_keys(end_date_str)

        # Take screenshot of the filled form
        driver.save_screenshot("date_filter_filled.png")

        # Press Enter or find a search/filter button
        try:
            # Try to find a Search or Filter button first
            search_buttons = driver.find_elements(
                By.XPATH,
                "//button[contains(text(), 'Search') or contains(text(), 'Filter') or contains(@id, 'search')]",
            )

            if search_buttons:
                logger.info(f"Found Search button: {search_buttons[0].text}")
                search_buttons[0].click()
            else:
                # Fall back to pressing Enter on the end date field
                logger.info("No search button found, pressing Enter")
                end_date.send_keys(Keys.RETURN)

        except Exception as e:
            logger.warning(f"Error clicking search button: {str(e)}")
            # Fall back to pressing Enter
            end_date.send_keys(Keys.RETURN)

        # Verify filtering was applied
        logger.info("Waiting for filter to be applied")
        time.sleep(5)  # Short wait to see initial response

        # Check if there's a loading indicator and wait for it to disappear
        try:
            WebDriverWait(driver, 10).until_not(
                EC.visibility_of_element_located((By.ID, "loading_bar"))
            )
            logger.info("Loading indicator disappeared")
        except:
            logger.info("No loading indicator found or it didn't disappear")

        return True

    except Exception as e:
        logger.error(f"Error setting date filters: {str(e)}")
        driver.save_screenshot("date_filter_error.png")
        return False


def wait_for_results(driver, timeout=180):
    """Wait for results to load after applying filter."""
    logger.info(f"Waiting up to {timeout} seconds for results to load")

    # List of elements that indicate results have loaded
    result_indicators = [
        (
            By.XPATH,
            "//table[contains(@class, 'transaction') or contains(@id, 'transaction')]",
        ),
        (By.XPATH, "//table//tr[2]"),  # At least one data row (after header)
        (By.ID, "export"),  # Export button appears
        (By.ID, "export_text"),  # Export text appears
    ]

    start_time = time.time()
    while time.time() - start_time < timeout:
        for selector_type, selector in result_indicators:
            try:
                elements = driver.find_elements(selector_type, selector)
                if elements and elements[0].is_displayed():
                    logger.info(f"Results loaded, found indicator: {selector}")
                    return True
            except:
                pass

        # Log progress periodically
        elapsed = int(time.time() - start_time)
        if elapsed % 15 == 0:
            logger.info(f"Still waiting for results... ({elapsed} seconds)")
            driver.save_screenshot(f"waiting_for_results_{elapsed}.png")

        time.sleep(1)

    logger.warning("Results loading timed out")
    return False


def wait_for_export_button(driver, timeout=180):
    """Wait specifically for the export button to become visible after filtering."""
    logger.info(f"Waiting up to {timeout} seconds for export button to appear")

    export_selectors = [
        (By.ID, "export"),
        (By.ID, "export_text"),
        (By.XPATH, "//div[contains(@id, 'export')]"),
        (By.XPATH, "//div[@data-l-bound='exports.text']"),
        (By.XPATH, "//div[text()='Export']"),
    ]

    start_time = time.time()
    while time.time() - start_time < timeout:
        for selector_type, selector in export_selectors:
            try:
                element = driver.find_element(selector_type, selector)
                if element.is_displayed():
                    logger.info(
                        f"Export button found and visible: {selector_type} {selector}"
                    )
                    return element
            except:
                pass

        # Log progress periodically
        elapsed = int(time.time() - start_time)
        if elapsed % 15 == 0:
            logger.info(f"Still waiting for export button... ({elapsed} seconds)")
            driver.save_screenshot(f"waiting_for_export_{elapsed}.png")

        time.sleep(1)

    logger.warning("Export button not found within timeout period")
    driver.save_screenshot("export_button_not_found.png")
    return None


def wait_for_download(downloads_dir, timeout=60):
    """Monitor the downloads directory for new files."""
    files_before = set(os.listdir(downloads_dir))
    logger.info(f"Files before download: {files_before}")

    start_time = time.time()
    new_file = None

    while time.time() - start_time < timeout:
        current_files = set(os.listdir(downloads_dir))
        new_files = current_files - files_before

        # Log all files for debugging
        if new_files:
            logger.info(f"New files detected: {new_files}")

        # Look for new CSV files specifically
        csv_files = [f for f in new_files if f.lower().endswith(".csv")]

        if csv_files:
            new_file = os.path.join(downloads_dir, csv_files[0])
            logger.info(f"Found new CSV file: {new_file}")

            # Wait a bit to ensure the file is completely written
            time.sleep(2)

            # Check if file size has stabilized (no longer being written to)
            initial_size = os.path.getsize(new_file)
            time.sleep(1)
            if os.path.getsize(new_file) == initial_size:
                logger.info("File size stabilized, download complete")
                return new_file

        # Log progress periodically
        elapsed = int(time.time() - start_time)
        if elapsed % 5 == 0:
            logger.info(f"Waiting for download... ({elapsed} seconds)")

        time.sleep(1)

    logger.warning(f"No new CSV file detected after {timeout} seconds")
    return None


def extract_table_data(driver, downloads_dir):
    """Extract data directly from the visible transaction table."""
    logger.info("Extracting data directly from the transaction table")

    try:
        # Try using pandas read_html first (easiest method)
        try:
            tables = pd.read_html(driver.page_source)
            if tables:
                logger.info(f"Successfully extracted {len(tables)} tables using pandas")

                # Find the largest table, which is likely the transaction table
                largest_table = max(tables, key=len)

                # Save page source for debugging
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                source_path = os.path.join(
                    downloads_dir, f"page_source_{timestamp}.html"
                )
                with open(source_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)

                # Save the DataFrame to CSV
                csv_path = os.path.join(
                    downloads_dir, f"extracted_table_{timestamp}.csv"
                )
                largest_table.to_csv(csv_path, index=False)

                logger.info(f"Table shape: {largest_table.shape}")
                logger.info(f"Table columns: {largest_table.columns.tolist()}")
                logger.info(f"First few rows: {largest_table.head().to_string()}")

                return csv_path
        except Exception as e:
            logger.warning(f"pandas read_html failed: {str(e)}")

        # Fallback to manual extraction with Selenium
        logger.info("Falling back to manual table extraction")

        # Find all tables
        tables = driver.find_elements(By.TAG_NAME, "table")
        if not tables:
            logger.warning("No tables found on page")
            driver.save_screenshot("no_tables_found.png")
            return None

        logger.info(f"Found {len(tables)} table elements")

        # Find the transaction table (usually the largest one or one with specific headers)
        main_table = None
        for i, table in enumerate(tables):
            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) > 1:  # Table has at least header and one data row
                # Check if it has expected headers
                headers = table.find_elements(By.TAG_NAME, "th")
                header_texts = [h.text.strip() for h in headers]

                # Look for transaction-related headers
                transaction_indicators = ["Card", "Date", "Amount", "Product", "Place"]
                matches = sum(
                    1
                    for indicator in transaction_indicators
                    if any(indicator in text for text in header_texts)
                )

                if matches >= 2:  # At least 2 expected headers match
                    main_table = table
                    logger.info(
                        f"Found transaction table (table {i+1}) with headers: {header_texts}"
                    )
                    break

                # If we can't identify by headers, use the table with most rows
                if not main_table or len(rows) > len(
                    main_table.find_elements(By.TAG_NAME, "tr")
                ):
                    main_table = table
                    logger.info(
                        f"Using table {i+1} with {len(rows)} rows as potential transaction table"
                    )

        if main_table:
            # Extract headers
            headers = []
            header_cells = main_table.find_elements(By.TAG_NAME, "th")
            if header_cells:
                headers = [cell.text.strip() for cell in header_cells]

            # If no headers found, check first row for headers
            if not headers:
                first_row = main_table.find_elements(By.XPATH, ".//tr[1]/td")
                if first_row:
                    headers = [cell.text.strip() for cell in first_row]

            # Extract rows
            rows = []
            data_rows = main_table.find_elements(By.TAG_NAME, "tr")

            # Skip header row if headers were found
            start_idx = 1 if headers else 0

            for row in data_rows[start_idx:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    if any(data for data in row_data if data):  # Skip empty rows
                        rows.append(row_data)

            if rows:
                # Create DataFrame
                df = pd.DataFrame(rows, columns=headers)

                # Save to CSV
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(
                    downloads_dir, f"selenium_extracted_table_{timestamp}.csv"
                )
                df.to_csv(csv_path, index=False)

                logger.info(
                    f"Successfully extracted {len(df)} rows with {len(headers)} columns"
                )
                logger.info(f"Saved to {csv_path}")

                return csv_path
            else:
                logger.warning("No data rows found in the table")
        else:
            logger.warning("Could not identify transaction table")

        # If we reach here, all extraction methods failed
        return None

    except Exception as e:
        logger.error(f"Error extracting table data: {str(e)}")
        driver.save_screenshot("table_extraction_error.png")
        return None


def get_transaction_data(driver, downloads_dir):
    """Try multiple methods to get transaction data."""

    # First try clicking the export button
    export_button = wait_for_export_button(driver)
    if export_button:
        logger.info("Found export button, attempting to download CSV")

        # Take a screenshot of the export button
        driver.save_screenshot("export_button_found.png")

        # Record files before clicking
        files_before = set(os.listdir(downloads_dir))

        # Try multiple click methods
        click_methods = [
            ("direct click", lambda: export_button.click()),
            (
                "JavaScript click",
                lambda: driver.execute_script("arguments[0].click();", export_button),
            ),
            (
                "Action chains",
                lambda: ActionChains(driver)
                .move_to_element(export_button)
                .click()
                .perform(),
            ),
            (
                "Parent element click",
                lambda: driver.execute_script(
                    "arguments[0].click();", export_button.find_element(By.XPATH, "..")
                ),
            ),
        ]

        for method_name, click_method in click_methods:
            try:
                logger.info(f"Trying {method_name}")
                click_method()
                logger.info(f"Successfully clicked export button using {method_name}")

                # Check if download started
                download_path = wait_for_download(downloads_dir)
                if download_path:
                    logger.info(f"Successfully downloaded CSV: {download_path}")
                    return download_path
                else:
                    logger.warning(f"{method_name} did not produce download")
            except Exception as e:
                logger.warning(f"{method_name} failed: {str(e)}")

        # If we reach here, all click methods failed
        logger.warning("All export button click methods failed")
    else:
        logger.warning("Export button not found")

    # Fall back to direct table extraction
    logger.info("Falling back to direct table extraction")
    return extract_table_data(driver, downloads_dir)


def validate_transaction_data(file_path):
    """Validate and standardize the extracted transaction data."""
    try:
        # Try multiple read options
        df = None
        read_attempts = [
            {"params": {"encoding": "utf-8"}, "name": "Default UTF-8"},
            {"params": {"encoding": "latin1"}, "name": "Latin-1 encoding"},
            {
                "params": {"sep": ";", "encoding": "utf-8"},
                "name": "Semicolon separator",
            },
            {
                "params": {"sep": ";", "encoding": "latin1"},
                "name": "Semicolon + Latin-1",
            },
        ]

        for attempt in read_attempts:
            try:
                df = pd.read_csv(file_path, **attempt["params"])
                logger.info(f"Successfully read CSV with {attempt['name']}")
                break
            except Exception as e:
                logger.warning(f"Failed to read CSV with {attempt['name']}: {str(e)}")

        if df is None or len(df) == 0:
            logger.error("Could not read CSV file or file is empty")
            return None

        # Log dataframe info
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")

        # Check for required data
        if len(df) == 0:
            logger.warning("CSV file contains no data rows")
            return None

        # Standardize column names
        column_mapping = {
            "Card": "Card_Number",
            "Card num.": "Card_Number",
            "Date": "Transaction_Date",
            "Product": "Product_Type",
            "Receipt N°": "Receipt_Number",
            "Receipt num.": "Receipt_Number",
            "Place": "Location",
            "Kms": "Kilometers",
            "Qty": "Quantity",
            "Amount": "Amount",
            "Invoice date": "Invoice_Date",
            "Invoice n°": "Invoice_Number",
        }

        # Create a new column mapping with only columns that exist
        actual_mapping = {
            old: new for old, new in column_mapping.items() if old in df.columns
        }

        if actual_mapping:
            df = df.rename(columns=actual_mapping)
            logger.info(f"Renamed columns: {actual_mapping}")

        # Save standardized file
        standardized_path = file_path.replace(".csv", "_standardized.csv")
        df.to_csv(standardized_path, index=False)
        logger.info(f"Saved standardized data to: {standardized_path}")

        return standardized_path

    except Exception as e:
        logger.error(f"Error validating data: {str(e)}")
        return None


def extract_total_data():
    """Extract data from the Total Portal using Selenium with enhanced reliability."""
    # Load environment variables
    load_dotenv()

    username = os.getenv("TOTAL_USERNAME")
    password = os.getenv("TOTAL_PASSWORD")

    if not username or not password:
        raise ValueError(
            "Missing credentials in .env file. Please set TOTAL_USERNAME and TOTAL_PASSWORD."
        )

    # Create download directory
    downloads_dir = os.path.join(os.getcwd(), "downloads")
    os.makedirs(downloads_dir, exist_ok=True)
    downloads_dir_abs = os.path.abspath(downloads_dir)

    # Set date range for extraction - from January 1 of current year to present
    current_year = datetime.now().year
    start_date = datetime(current_year, 1, 1)
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date = datetime.now()
    end_date_str = end_date.strftime("%d/%m/%Y")

    logger.info(f"Will extract data from {start_date_str} to {end_date_str}")
    logger.info(f"Download directory: {downloads_dir_abs}")

    # Configure Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Uncomment for headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-popup-blocking")

    # Set more explicit download preferences for Chrome
    prefs = {
        "download.default_directory": downloads_dir_abs,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "plugins.always_open_pdf_externally": True,
        "browser.download.folderList": 2,
        "browser.helperApps.neverAsk.saveToDisk": "application/csv,text/csv,application/vnd.ms-excel,text/plain",
        "profile.default_content_settings.popups": 0,
        "download.default_content_setting_values.csv": 1,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Initialize Chrome driver
    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(60)  # Set page load timeout

    try:
        # Navigate to the portal
        logger.info("Navigating to Total Portal")
        driver.get("https://www.mytotalfuelcard.com/Client/app/index.html")

        # Take a screenshot to see what's on the page
        logger.info("Taking screenshot of initial page")
        driver.save_screenshot("initial_page.png")

        # Handle cookie banner if present
        logger.info("Looking for cookie accept button")
        try:
            # Try JavaScript approach for cookie banner
            driver.execute_script(
                """
                var cookieButtons = document.querySelectorAll('button');
                for (var i = 0; i < cookieButtons.length; i++) {
                    var btn = cookieButtons[i];
                    if (btn.textContent.toLowerCase().includes('accept') || 
                        btn.textContent.toLowerCase().includes('ok')) {
                        btn.click();
                        return true;
                    }
                }
                return false;
                """
            )
            logger.info("Attempted to click cookie accept button using JavaScript")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Cookie handling failed: {str(e)}")

        # Look for the userid field with explicit wait
        logger.info("Looking for Userid field")
        try:
            userid_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='text']"))
            )
            logger.info("Found Userid field")
        except Exception as e:
            logger.error(f"Could not find Userid field: {str(e)}")
            driver.save_screenshot("error_finding_userid.png")
            raise Exception("Could not locate Userid field")

        # Look for the password field with explicit wait
        logger.info("Looking for Password field")
        try:
            password_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input[type='password']")
                )
            )
            logger.info("Found Password field")
        except Exception as e:
            logger.error(f"Could not find Password field: {str(e)}")
            driver.save_screenshot("error_finding_password.png")
            raise Exception("Could not locate Password field")

        # Enter credentials
        logger.info("Entering credentials")
        userid_field.clear()
        userid_field.send_keys(username)
        password_field.clear()
        password_field.send_keys(password)

        # Find and click login button
        logger.info("Looking for login button")
        try:
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "bt_login"))
            )
            logger.info("Found login button")
            driver.execute_script("arguments[0].click();", login_button)
            logger.info("Clicked login button")
        except Exception as e:
            logger.error(f"Error finding login button: {str(e)}")
            # Use JavaScript as fallback
            try:
                logger.info("Using JavaScript to click login button")
                driver.execute_script("document.getElementById('bt_login').click();")
                logger.info("Clicked login button using JavaScript")
            except Exception as e2:
                logger.error(f"JavaScript click also failed: {str(e2)}")
                driver.save_screenshot("login_button_error.png")
                raise Exception("Could not click login button")

        # Check for recaptcha with explicit waiting
        try:
            recaptcha_frames = WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//iframe[contains(@src, 'recaptcha')]")
                )
            )
            logger.warning("Recaptcha detected! Manual intervention needed")
            driver.save_screenshot("recaptcha_detected.png")

            # Wait for manual captcha solving
            logger.info("Waiting 60 seconds for manual captcha solving...")
            time.sleep(60)
            driver.save_screenshot("after_manual_captcha.png")
        except:
            logger.info("No recaptcha detected, continuing")

        # Wait for dashboard to load with explicit wait
        logger.info("Waiting for dashboard to load")
        try:
            # Wait for either the home page indicator or the logged-in text
            dashboard_loaded = False

            # Check if URL contains #!home
            current_url = driver.current_url
            logger.info(f"Current URL after login attempt: {current_url}")

            if "#!home" in current_url:
                logger.info("Dashboard detected via URL")
                dashboard_loaded = True
            else:
                # Try waiting for logged-in text
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//*[contains(text(), 'You are logged in')]")
                        )
                    )
                    logger.info("Dashboard detected via logged-in text")
                    dashboard_loaded = True
                except:
                    # Check for any dashboard indicator
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.ID, "main_header"))
                        )
                        logger.info("Dashboard detected via main header")
                        dashboard_loaded = True
                    except:
                        pass

            if dashboard_loaded:
                logger.info("Login successful - detected dashboard")
                driver.save_screenshot("dashboard_loaded.png")
            else:
                logger.error("Could not detect dashboard")
                driver.save_screenshot("dashboard_not_detected.png")
                raise Exception("Login appeared to succeed but dashboard not detected")

        except Exception as e:
            logger.error(f"Dashboard loading error: {str(e)}")
            driver.save_screenshot("dashboard_error.png")
            raise Exception(f"Dashboard loading error: {str(e)}")

        # Find and click the Transactions link with better error handling
        logger.info("Looking for Transactions link")
        transactions_selectors = [
            (
                By.XPATH,
                "//a[contains(text(), 'Transactions') or contains(@href, 'transactions')]",
            ),
            (By.XPATH, "//a[contains(@class, 'transactions')]"),
            (By.XPATH, "//div[contains(text(), 'Transactions')]/.."),
            (
                By.XPATH,
                "//*[contains(text(), 'Transactions') and (self::a or self::div or self::span)]",
            ),
        ]

        transactions_link = None
        for selector_type, selector in transactions_selectors:
            try:
                elements = driver.find_elements(selector_type, selector)
                if elements:
                    transactions_link = elements[0]
                    logger.info(f"Found Transactions link using {selector}")
                    break
            except:
                pass

        if transactions_link:
            try:
                logger.info("Clicking Transactions link")
                driver.execute_script("arguments[0].click();", transactions_link)
                logger.info("Clicked Transactions link")
            except Exception as e:
                logger.error(f"Error clicking Transactions link: {str(e)}")
                driver.save_screenshot("transactions_click_error.png")
                raise Exception(f"Could not click Transactions link: {str(e)}")
        else:
            logger.error("Could not find Transactions link")
            driver.save_screenshot("transactions_link_not_found.png")

            # Try to navigate directly to transactions URL
            try:
                logger.info("Attempting direct navigation to transactions page")
                driver.get(
                    "https://www.mytotalfuelcard.com/Client/app/index.html#!transactions"
                )
                time.sleep(5)
                logger.info("Direct navigation to transactions page attempted")
            except Exception as e:
                logger.error(f"Direct navigation failed: {str(e)}")
                raise Exception("Could not access transactions page")

        # Wait for transactions page to load using explicit wait
        logger.info("Waiting for transactions page to load")
        try:
            # Wait for date fields to appear
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "dp_date_debut"))
            )
            logger.info("Transactions page loaded successfully")
        except Exception as e:
            logger.error(f"Error waiting for transactions page: {str(e)}")
            driver.save_screenshot("transactions_page_error.png")
            raise Exception(f"Transactions page didn't load properly: {str(e)}")

        # Set date filters
        if set_date_filter(driver, start_date_str, end_date_str):
            logger.info("Date filters applied successfully")

            # Wait for results to load
            if wait_for_results(driver):
                logger.info("Results loaded successfully")

                # Try to get transaction data (export or scrape)
                data_file = get_transaction_data(driver, downloads_dir)

                if data_file:
                    logger.info(f"Successfully obtained data: {data_file}")

                    # Validate and standardize the data
                    standardized_file = validate_transaction_data(data_file)

                    if standardized_file:
                        logger.info(f"Data validation successful: {standardized_file}")
                        return standardized_file
                    else:
                        logger.warning(
                            "Data validation failed, returning raw data file"
                        )
                        return data_file
                else:
                    logger.error("Failed to obtain transaction data")
                    raise Exception("No transaction data could be obtained")
            else:
                logger.error("Results did not load within timeout period")
                raise Exception("Results did not load after applying filters")
        else:
            logger.error("Failed to apply date filters")
            raise Exception("Could not set date filters")

    except Exception as e:
        logger.error(f"Error in data extraction: {str(e)}")
        raise

    finally:
        logger.info("Closing browser")
        driver.quit()


def extract_with_retry(max_retries=3):
    """Retry the extraction process multiple times."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Extraction attempt {attempt} of {max_retries}")
            return extract_total_data()
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {str(e)}")
            if attempt < max_retries:
                wait_time = 60 * attempt  # Progressive backoff
                logger.info(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                logger.error("All extraction attempts failed")
                raise


if __name__ == "__main__":
    try:
        csv_file = extract_with_retry()
        logger.info(f"Successfully downloaded data to: {csv_file}")
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        raise
