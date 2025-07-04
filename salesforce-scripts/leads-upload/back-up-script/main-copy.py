import time
from simple_salesforce import Salesforce
from tqdm import tqdm
import logging
import sys
import pandas as pd
import numpy as np
import os
import re
from dotenv import load_dotenv
import argparse


load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("salesforce_insert.log"),
        logging.StreamHandler(sys.stdout),
    ],
)




# Connect to salesforce
def connect_to_salesforce(username, password, client_id, client_secret, domain="login"):
    """
    Connect to Salesforce using OAth 2.0 username-password flow
    """
    try:
        sf = Salesforce(
            username=username,
            password=password,
            consumer_key=client_id,
            consumer_secret=client_secret,
            domain=domain,
        )


        logging.info("Successfully connected to Salesforce using OAuth 2.0")
        return sf
    except Exception as e:
        logging.error(f"Failed to connect to Salesforce: {str(e)}")
        return None




def process_full_name(full_name):
    """
    Process a full name into FirstName and LastName
    - If only one name, FirstName = name, LastName = 'N/A'
    - If two names, FirstName = first part, LastName = second part
    - If three or more names, FirstName = first part, LastName = all remaining parts


    Parameters:
    - full_name: The full name to process


    Returns:
    - tuple: (first_name, last_name)
    """
    if pd.isna(full_name) or not full_name:
        return ("N/A", "N/A")


    name_parts = str(full_name).strip().split()


    if len(name_parts) == 0:
        return ("N/A", "N/A")
    elif len(name_parts) == 1:
        return (name_parts[0], "N/A")
    else:
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])
        return (first_name, last_name)




def format_phone_number(phone):
    """
    Format phone number according to specified standardization rules


    Parameters:
    - phone: The phone number to format


    Returns:
    - Standardized phone number or None if invalid
    """
    if pd.isna(phone) or phone == "" or phone is None:
        return None


    phone_str = str(phone).strip()


    if "E" in phone_str or "e" in phone_str:
        try:
            phone_str = str(int(float(phone_str)))
        except ValueError:
            logging.warning(
                f"Could not convert phone number from scientific notation: {phone}"
            )
            return None


    # Remove non-digit characters except for plus sign
    clean_text = re.sub(r"[^\d+]", "", phone_str)


    if clean_text.startswith("2540"):
        # Remove "2540" prefix and add "0"
        result = "0" + clean_text[4:]
    elif any(clean_text.startswith(prefix) for prefix in ["254", "255", "+254", "252", "256"]):
        # Remove "254", "255", "+254", or "252" prefix and add "0"
        result = "0" + clean_text[3 if not clean_text.startswith("+") else 4 :]
    elif clean_text.startswith("7") or clean_text.startswith("1"):
        # Add "0" prefix to numbers starting with "7" or "1"
        result = "0" + clean_text
    elif len(clean_text) == 9 and clean_text.startswith("7"):
        # Add "0" prefix to 9-digit numbers starting with "7"
        result = "0" + clean_text
    else:
        # Keep as is for other cases
        result = clean_text


    # Validate the result (should be 10 digits starting with 0)
    if len(result) == 10 and result.startswith("0"):
        return result
    else:
        logging.warning(
            f"Phone number standardization resulted in invalid format: {phone} → {result}"
        )
        return result




# Read the leads from a Excel File
def read_leads_from_file(file_path):
    """
    Read lead data from an Excel or CSV file and transform according to requirements


    Parameters:
    - file_path: Path to the Excel or CSV file containing lead data


    Returns:
    - List of lead data dictionaries ready for Salesforce insertion
    """
    try:
        if file_path.lower().endswith(".xlsx") or file_path.lower().endswith(".xls"):
            df = pd.read_excel(file_path, dtype={"Phone Number": str})
        else:
            df = pd.read_csv(file_path, dtype={"Phone Number": str})


        df.columns = [col.lower().replace(" ", "_") for col in df.columns]


        expected_columns = {
            "name": "Name",
            "phone_number": "MobilePhone",
            "lead_source": "LeadSource",
            "gender": "Gender__c",
            "company": "Company",
        }


        # Verify expected columns exist or log warnings
        for col in expected_columns:
            if col not in df.columns:
                logging.warning(f"Expected column '{col}' not found in the file")


        leads = []


        for _, row in df.iterrows():
            lead_data = {}


            phone_field = next(
                (field for field in df.columns if "phone" in field.lower()), None
            )


            if not phone_field:
                logging.warning("No phone number column found in the file!")
                continue


            raw_phone = row[phone_field]
            formatted_phone = format_phone_number(raw_phone)


            if not formatted_phone:
                logging.warning(
                    f"Skipping lead due to missing or invalid phone number: {row}"
                )
                continue


            # Add the phone number to the lead data
            lead_data["MobilePhone"] = formatted_phone


            # Process full name if it exists
            name_field = next(
                (field for field in df.columns if field.lower() == "name"), None
            )
            if name_field and name_field in row:
                first_name, last_name = process_full_name(row[name_field])
                lead_data["FirstName"] = first_name
                lead_data["LastName"] = last_name


            else:
                # If no full name field, look for separate first/last name fields
                first_name_field = next(
                    (
                        field
                        for field in df.columns
                        if "first" in field.lower() and "name" in field.lower()
                    ),
                    None,
                )
                last_name_field = next(
                    (
                        field
                        for field in df.columns
                        if "last" in field.lower() and "name" in field.lower()
                    ),
                    None,
                )


                if first_name_field and first_name_field in row:
                    lead_data["FirstName"] = (
                        str(row[first_name_field]).strip()
                        if pd.notna(row[first_name_field])
                        else "N/A"
                    )


                if last_name_field and last_name_field in row:
                    lead_data["LastName"] = (
                        str(row[last_name_field]).strip()
                        if pd.notna(row[last_name_field])
                        else "N/A"
                    )
                elif "FirstName" in lead_data:
                    lead_data["LastName"] = "N/A"


            field_mapping = {
                "first_name": "FirstName",
                "last_name": "LastName",
                "lead_source": "LeadSource",
                "gender": "Gender__c",
                "company": "Company",
            }


            for csv_field, sf_field in field_mapping.items():
                # Skip if column doesn't exist in the CSV
                if csv_field not in df.columns:
                    continue


                value = row[csv_field] if pd.notna(row[csv_field]) else ""


                # Add the transformed field to the lead data
                lead_data[sf_field] = value


            # Add default fields that may not be in the CSV
            if "Company" not in lead_data:
                lead_data["Company"] = "N/A"


            # Ensure required fields are present
            if "FirstName" in lead_data and "MobilePhone" in lead_data:
                # Add default company value if missing
                if "Company" not in lead_data or not lead_data["Company"]:
                    lead_data["Company"] = "N/A"


                # Phone number is already validated
                leads.append(lead_data)
            else:
                logging.warning(f"Skipping lead due to missing required fields: {row}")


        logging.info(f"Successfully read {len(leads)} valid leads from {file_path}")
        return leads


    except Exception as e:
        logging.error(f"Failed to read leads from file: {str(e)}")
        return []




def validate_lead_data(leads):
    """
    Comprehensive validation of all lead data to ensure all conditions are met


    Parameters:
    - leads: List of lead data dictionaries


    Returns:
    - tuple: (is_valid, list of validation errors)
    """


    validation_errors = []


    if not leads:
        validation_errors.append("No valid leads found for processing")
        return False, validation_errors


    # Check each lead for all required conditions
    for i, lead in enumerate(leads):
        lead_identifier = f"Lead #{i+1} ({lead.get('FirstName', 'Unknown')} {lead.get('LastName', 'N/A')})"


        # Phone number validation (most critical)
        if "MobilePhone" not in lead:
            validation_errors.append(f"{lead_identifier} is missing a phone number")
        else:
            phone = lead["MobilePhone"]
            if not (len(phone) == 10 and phone.startswith("0")):
                validation_errors.append(
                    f"{lead_identifier} has invalid phone format: {phone}"
                )


        # First Name validation
        if "FirstName" not in lead or not lead["FirstName"]:
            validation_errors.append(f"{lead_identifier} is missing a first name")


        # Last Name validation (should be 'N/A' if not available)
        if "LastName" not in lead:
            validation_errors.append(f"{lead_identifier} is missing a last name")


    # Check for duplicate phone numbers
    phone_counts = {}


    for lead in leads:
        if "MobilePhone" in lead:
            phone = lead["MobilePhone"]
            if phone in phone_counts:
                phone_counts[phone] += 1
            else:
                phone_counts[phone] = 1


    duplicates = {phone: count for phone, count in phone_counts.items() if count > 1}


    if duplicates:
        for phone, count in duplicates.items():
            validation_errors.append(
                f"Phone number {phone} appears {count} times in the import data"
            )


    return len(validation_errors) == 0, validation_errors




def categorize_error(error_message):
    """
    Categorize error messages for better reporting and user-friendly responses
    """
    if isinstance(error_message, dict):
        error_message = str(error_message)


    if (
        "duplicate value found" in error_message.lower()
        or "duplicates_detected" in error_message.lower()
    ):
        return "Duplicate Record", "This lead already exists in Salesforce"
    elif "required_field_missing" in error_message.lower():
        return "Missing Field", "A required field is missing"
    elif "invalid_field" in error_message.lower():
        return "Invalid Field", "One or more fields contain invalid values"
    elif "malformed request" in error_message.lower():
        return "API Error", "Problem with the request format"
    else:
        return "Other Error", error_message




def insert_leads_to_salesforce(sf, leads, delay=30):
    """
    Insert leads into Salesforce with a specified delay between each insertion


    Parameters:
    - sf: Salesforce connection
    - leads: List of lead data dictionaries
    - delay: Delay in seconds between each insertion (default: 30 seconds)


    Returns:
    - successful_leads: List of successfully inserted leads
    - failed_leads: List of leads that failed to insert
    """


    successful_leads = []
    failed_leads = []


    # Validate data first
    is_valid, validation_errors = validate_lead_data(leads)


    if not is_valid:
        logging.error("VALIDATION FAILED: Cannot proceed with lead insertion")
        logging.error("The following issues must be fixed before proceeding:")
        for error in validation_errors:
            logging.error(f"  - {error}")


        # Validation report file
        try:
            with open("validation_errors.txt", "w") as f:
                f.write("LEAD VALIDATION ERRORS\n")
                f.write("=====================\n\n")
                f.write(f"Total errors found: {len(validation_errors)}\n\n")
                for i, error in enumerate(validation_errors):
                    f.write(f"{i+1}. {error}\n")


            logging.info("Validation errors have been saved to 'validation_errors.txt'")
        except Exception as e:
            logging.error(f"Failed to write validation report: {str(e)}")


        return successful_leads, failed_leads


    logging.info("All leads passed validation checks!")


    total_leads = len(leads)
    logging.info(f"Starting insertion process for {total_leads} leads")


    # Process each lead with the required delay
    for i, lead_data in enumerate(tqdm(leads, desc="Inserting leads")):
        try:
            logging.info(f"Inserting lead {i+1}/{total_leads}: {lead_data}")


            process_time = time.strftime("%Y-%m-%d %H:%M:%S")


            response = sf.Lead.create(lead_data)


            if response.get("success"):
                lead_id = response.get("id")
                logging.info(f"Successfully inserted lead with ID: {lead_id}")
                successful_leads.append(
                    {
                        **lead_data,
                        "Id": lead_id,
                        "Processed_At": process_time,
                        "Status_Message": "Successfully inserted",
                    }
                )
            else:
                error_msg = (
                    response.get("errors", ["Unknown error"])[0]
                    if isinstance(response.get("errors"), list)
                    else str(response.get("errors", "Unkown error"))
                )
                error_category, friendly_message = categorize_error(error_msg)
                logging.error(f"Failed to insert lead: {error_msg}")
                failed_leads.append(
                    {
                        **lead_data,
                        "Processed_At": process_time,
                        "Error_Category": error_category,
                        "Status_Message": error_msg,
                        "Raw_Error": error_msg,
                    }
                )


            # Add delay between insertions (only if not the last lead)
            if i < total_leads - 1:
                logging.info(f"Waiting {delay} seconds before inserting next lead...")
                time.sleep(delay)


        except Exception as e:
            error_message = str(e)
            logging.error(
                f"Error inserting lead {lead_data.get('FirstName', 'Unknown')} {lead_data.get('LastName', 'Unknown')}: {error_message}"
            )
            failed_leads.append({**lead_data, "Status_Message": error_message})


    # Final summary
    success_rate = (len(successful_leads) / total_leads) * 100 if total_leads > 0 else 0


    logging.info(
        f"Insertion process complete: {len(successful_leads)} leads inserted successfully ({success_rate:.2f}%)"
    )
    logging.info(f"Failed insertions: {len(failed_leads)} leads")


    return successful_leads, failed_leads




def export_results(
    successful_leads, failed_leads, output_file="insertion_results.xlsx"
):
    """
    Export the results to an Excel file
    """
    try:
        # Check if there is data to export
        if not successful_leads and not failed_leads:
            logging.info("No results to export (no leads were processed).")
            return True


        # Return successful and failed leads
        if successful_leads:
            successful_df = pd.DataFrame(successful_leads)
        else:
            successful_df = pd.DataFrame(columns=["Status_Message", "Processed_At"])


        if failed_leads:
            failed_df = pd.DataFrame(failed_leads)
        else:
            failed_df = pd.DataFrame(columns=["Status_Message", "Processed_At"])


        # Export results
        with pd.ExcelWriter(output_file) as writer:
            # Combined results if both dataframes have data
            if not successful_df.empty or not failed_df.empty:
                if successful_df.empty:
                    results_df = failed_df
                elif failed_df.empty:
                    results_df = successful_df
                else:
                    # Make sure both dataframes have the same columns
                    all_columns = list(
                        set(successful_df.columns) | set(failed_df.columns)
                    )
                    for col in all_columns:
                        if col not in successful_df.columns:
                            successful_df[col] = None
                        if col not in failed_df.columns:
                            failed_df[col] = None


                    results_df = pd.concat([successful_df, failed_df])


                results_df.to_excel(writer, sheet_name="All Results", index=False)


            # Individual sheets
            if not successful_df.empty:
                successful_df.to_excel(
                    writer, sheet_name="Successful Leads", index=False
                )
            if not failed_df.empty:
                failed_df.to_excel(writer, sheet_name="Failed Leads", index=False)


        logging.info(f"Results exported to {output_file}")
        return True
    except Exception as e:
        logging.error(f"Failed to export results: {str(e)}")
        return False




def get_most_recent_file(directory="files/", extensions=(".xlsx", ".xls", ".csv")):
    """
    Get the most recently modified file with the specified extensions in the given directory


    Parameters:
    - directory: Directory to search for files
    - extensions: Tuple of file extensions to look for


    Returns:
    - Path to the most recent file or None if no files found
    """
    try:
        files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.endswith(extensions)
        ]


        if not files:
            return None


        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)


        return files[0]


    except Exception as e:
        logging.error(f"Error finding files in directory {directory}: {str(e)}")
        return None




def main():
    """
    Main function to execute the lead insertion process
    """
    # Salesforce OAuth 2.0 credentials
    username = os.environ.get("username")
    password = os.environ.get("password")
    client_id = os.environ.get("consumer_key")
    client_secret = os.environ.get("consumer_secret")


    # Connect to Salesforce using OAuth 2.0
    sf = connect_to_salesforce(username, password, client_id, client_secret)


    if not sf:
        return


    # File containing lead data
    file_path = get_most_recent_file()


    if not file_path:
        logging.error("No Excel or CSV files found in the 'files' directory.")
        return


    logging.info(f"Using most recent file: {file_path}")


    # Read leads from file
    leads = read_leads_from_file(file_path)


    if not leads:
        logging.error("No valid leads found in the file. Process aborted.")
        return


    logging.info(f"Processing {len(leads)} leads for insertion")


    # Request for lead source
    lead_source = input("Enter Lead Source (e.g., Facebook, Meta, WhatsApp, etc.): ")


    logging.info(f"Using '{lead_source}' as Lead Source for all leads")


    for lead in leads:
        lead["LeadSource"] = lead_source


    # Ask for delay time
    try:
        delay = int(
            input("Enter delay between lead insertions in seconds (default 30): ") or 30
        )
    except ValueError:
        delay = 30
        logging.info("Invalid delay value, using default of 30 seconds")


    # Insert leads into Salesforce
    successful_leads, failed_leads = insert_leads_to_salesforce(sf, leads, delay=delay)


    export_results(successful_leads, failed_leads)




if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Salesforce lead insertion process")


    main()


    elapsed_time = time.time() - start_time
    logging.info(f"Process completed in {elapsed_time:.2f} seconds")
