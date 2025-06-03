import time
from simple_salesforce import Salesforce
from tqdm import tqdm
import logging
import sys
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("salesforce_update.log"),
        logging.StreamHandler(sys.stdout),
    ],
)


def connect_to_salesforce(username, password, client_id, client_secret, domain="login"):
    """
    Connect to Salesforce using OAuth 2.0 username-password flow
    """
    try:
        # For OAuth 2.0 username-password flow
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


def read_lead_ids_from_csv(csv_file):
    """
    Read lead IDs from a CSV file

    Parameters:
    - csv_file: Path to the CSV file containing lead IDs

    Returns:
    - List of lead IDs
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)

        # Check if the CSV has the expected column
        if "LeadId" in df.columns:
            lead_column = "LeadId"
        elif "lead_id" in df.columns:
            lead_column = "lead_id"
        else:
            # Try to use the first column if column name is not standard
            lead_column = df.columns[0]
            logging.warning(
                f"No column named 'LeadId' found, using first column: '{lead_column}'"
            )

        # Extract lead IDs and convert to list
        lead_ids = df[lead_column].astype(str).tolist()

        # Remove any leading/trailing whitespace
        lead_ids = [lead_id.strip() for lead_id in lead_ids]

        # Remove any empty strings or NaN values
        lead_ids = [
            lead_id for lead_id in lead_ids if lead_id and lead_id.lower() != "nan"
        ]

        logging.info(f"Successfully read {len(lead_ids)} lead IDs from {csv_file}")
        return lead_ids
    except Exception as e:
        logging.error(f"Failed to read lead IDs from CSV: {str(e)}")
        return []


def update_leads_from_list(sf, lead_ids, agent_id, product_id, batch_size=100):
    """
    Update leads in Salesforce with the specified agent and product using a list of lead IDs

    Parameters:
    - sf: Salesforce connection
    - lead_ids: List of Salesforce Lead IDs to update
    - agent_id: ID of the agent to assign to all leads
    - product_id: ID of the product to assign to all leads
    - batch_size: Number of records to update in each batch

    Returns:
    - updated_leads: List of successfully updated lead IDs
    - failed_leads: List of lead IDs that failed to update
    """
    updated_leads = []
    failed_leads = []

    # Calculate total number of batches
    total_leads = len(lead_ids)
    total_batches = (total_leads + batch_size - 1) // batch_size

    logging.info(
        f"Starting update process for {total_leads} leads (in {total_batches} batches)"
    )

    # Process in batches to avoid hitting API limits
    for i in range(0, total_leads, batch_size):
        batch_ids = lead_ids[i : min(i + batch_size, total_leads)]

        progress_msg = f"Processing batch {i//batch_size + 1}/{total_batches} ({len(batch_ids)} leads)"
        logging.info(progress_msg)

        # Process each lead in the current batch
        for lead_id in tqdm(batch_ids, desc=f"Batch {i//batch_size + 1}"):
            try:
                # First, check if lead already has the correct values
                current_lead = sf.Lead.get(lead_id)

                # Check if update is needed
                if (
                    current_lead.get("Agent__c") == agent_id
                    and current_lead.get("Product__c") == product_id
                ):
                    logging.info(
                        f"Lead {lead_id} already has correct values, skipping update"
                    )
                    updated_leads.append(
                        lead_id
                    )  # Count as updated since it has the right values
                    continue

                # Prepare update data
                data = {"Agent__c": agent_id, "Product__c": product_id}

                # Update the lead in Salesforce
                sf.Lead.update(lead_id, data)
                updated_leads.append(lead_id)

                # Add a small delay to avoid hitting API rate limits
                time.sleep(0.05)

            except Exception as e:
                logging.error(f"Failed to update lead {lead_id}: {str(e)}")
                failed_leads.append(lead_id)

        # Summary for the current batch
        batch_summary = f"Batch {i//batch_size + 1} complete: {len(batch_ids) - len(failed_leads[-len(batch_ids):])} updated, {len(failed_leads[-len(batch_ids):])} failed"
        logging.info(batch_summary)

    # Final summary
    success_rate = (len(updated_leads) / total_leads) * 100 if total_leads > 0 else 0
    logging.info(
        f"Update process complete: {len(updated_leads)} leads updated successfully ({success_rate:.2f}%)"
    )
    logging.info(f"Failed updates: {len(failed_leads)} leads")

    return updated_leads, failed_leads


def export_results(updated_leads, failed_leads, output_file="update_results.xlsx"):
    """
    Export the results to an Excel file
    """
    try:
        # Create DataFrames for updated and failed leads
        updated_df = pd.DataFrame({"LeadId": updated_leads, "Status": "Updated"})
        failed_df = pd.DataFrame({"LeadId": failed_leads, "Status": "Failed"})

        # Combine the results
        results_df = pd.concat([updated_df, failed_df])

        # Export to Excel
        with pd.ExcelWriter(output_file) as writer:
            results_df.to_excel(writer, sheet_name="Update Results", index=False)
            updated_df.to_excel(writer, sheet_name="Updated Leads", index=False)
            failed_df.to_excel(writer, sheet_name="Failed Leads", index=False)

        logging.info(f"Results exported to {output_file}")
        return True
    except Exception as e:
        logging.error(f"Failed to export results: {str(e)}")
        return False


def main():
    # Salesforce OAuth 2.0 credentials
    username = os.environ.get("username")
    password = os.environ.get("password")
    client_id = os.environ.get("consumer_key")
    client_secret = os.environ.get("consumer_secret")

    # Connect to Salesforce using OAuth 2.0
    sf = connect_to_salesforce(username, password, client_id, client_secret)

    if not sf:
        return

    # CSV file containing lead IDs
    csv_file = "Leads Summary.csv"

    # Read lead IDs from CSV
    lead_ids = read_lead_ids_from_csv(csv_file)

    if not lead_ids:
        logging.error("No lead IDs found in the CSV file. Process aborted.")
        return

    logging.info(f"Processing {len(lead_ids)} lead IDs for update")

    # Agent and Product IDs
    agent_id = "a05Pz000005zId3IAE"
    product_id = "01t8d000000RMjOAAW"

    # Update leads in Salesforce
    updated_leads, failed_leads = update_leads_from_list(
        sf, lead_ids, agent_id, product_id, batch_size=100
    )

    # Export results to Excel
    export_results(updated_leads, failed_leads)


if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Salesforce lead update process")

    main()

    elapsed_time = time.time() - start_time
    logging.info(f"Process completed in {elapsed_time:.2f} seconds")
