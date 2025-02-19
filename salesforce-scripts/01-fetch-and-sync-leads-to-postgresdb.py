from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import traceback

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password")

# print("Client ID:", sf_client_id)  # Debugging: Check if value is loaded

# Salesforce Authentication URL
sf_instance_url = os.getenv("sf_instance_url")

# Global variable to store access token and expiry time
access_token = None
token_expiry = None

# PostgreSQL configuration
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    sf_auth_url="https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password, 
    }
    response = requests.post(sf_auth_url, data=payload)
    #return response.json()["access_token"]
    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        instance_url = data.get("instance_url")  # Get dynamically
        print("Salesforce Access Token:", access_token)
        return access_token, instance_url  # Return both
    else:
        print("Error:", response.status_code, response.text)
        return None
    
def fetch_salesforce_leads(access_token, instance_url):
    """Fetch Leads from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    query = """
        SELECT Id,
        IsDeleted,
        MasterRecordId,
        LastName,
        FirstName,
        Salutation,
        Name,
        Title,
        Company,
        Street,
        City,
        State,
        PostalCode,
        Country,
        Latitude,
        Longitude,
        GeocodeAccuracy,
        Address,
        Phone,
        MobilePhone,
        Website,
        LeadSource,
        Status,
        Industry,
        Rating,
        NumberOfEmployees,
        OwnerId,
        IsConverted,
        ConvertedDate,
        ConvertedAccountId,
        ConvertedContactId,
        ConvertedOpportunityId,
        IsUnreadByOwner,
        CreatedDate,
        CreatedById,
        LastModifiedDate,
        LastModifiedById,
        SystemModstamp,
        LastActivityDate,
        LastViewedDate,
        LastReferencedDate,
        Jigsaw,
        JigsawContactId,
        EmailBouncedReason,
        EmailBouncedDate,
        Acreage__c,
        Country_Code__c,
        Date_of_Birth__c,
        Gender__c,
        Lead_AMT_Customer_Id__c,
        Installation_Date__c,
        Lead_Category__c,
        Lead_Channel__c,
        Location__c,
        Payment_Method__c,
        Preferred_Language__c,
        Product_del__c,
        Purchase_Date__c,
        Water_Source_Distance__c,
        Water_Source__c,
        leadcap__Facebook_Lead_ID__c,
        Customer_Type__c,
        Lead_Model_Category__c,
        ID_Number__c,
        Call_Back_Date__c,
        Follow_Up_Date__c,
        Product__c,
        KYC_Status__c,
        Agent_Phone_Number__c,
        Agent__c,
        Payment_Terms__c,
        Referral_Name__c,
        Referral_ID__c,
        Income_Threshold__c,
        Last_Updated_By__c,
        Daily_Water_Usage__c,
        MobileNumberWithCountryCode__c,
        Lead_Source_Other_Comment__c,
        Total_Dynamic_Head__c,
        Referral_Phone_Number__c,
        Custom_Opportunity_Name__c,
        SMSMessage__c,
        Contact_External_Id_Source__c,
        ContactRegionId__c,
        OpportunityPayPlanId__c,
        AMT_Customer_Name__c,
        Old_AMT_Customer_ID__c,
        Agent_Employee_Number__c,
        Other_Phone__c,
        Lead_Date_Created__c,
        Number_of_Units_Lead__c,
        KRA_Pin__c,
        Customer_to_Claim_VAT__c,
        Customer_Product_of_Interest__c,
        My_lead_Filter__c,
        Referral_Source_Application__c,
        Agent_Referral_SMSBody__c,
        Through_Partner_Lead__c,
        Unique_Phone_Number__c,
        Through_Partner_Customer__c,
        Referral_Lead_ID__c,
        CDS1Tracker__c,
        CDS_Status__c,
        Survey_Stat__c,
        SADM_Account__c,
        SADM_CDS_ID__c,
        SADM_Customer__c,
        SADM_KYC_Date__c,
        SADM_CDS1_Date__c,
        SADM_CDS2_Date__c,
        SADM_Customer_Creation_Date__c,
        SADM_Deposit_Date__c,
        SADM_FIRST_MONTH_INSTALLMENT__c,
        SADM_JSF_Date__c,
        SADM_Status__c,
        Employee_ID__c,
        Employee_Name__c,
        Employee_Phone__c
        FROM Lead
        WHERE LastModifiedDate = THIS_MONTH
    """
    all_leads = []
    next_url = f"{instance_url}/services/data/v58.0/query?q={query}"
    
    while next_url:
        response = requests.get(next_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_leads.extend(data.get("records", []))
            next_url = f"{instance_url}{data.get('nextRecordsUrl')}" if "nextRecordsUrl" in data else None
        else:
            print("Failed to fetch Leads:", response.text)
            break
    return all_leads

    
def save_to_postgresql(leads):
    """Save Leads to PostgreSQL."""
    db_connection = psycopg2.connect(**ep_stage_db_params)
    cursor = db_connection.cursor()
    
    # Upsert query (adjust fields based on your schema)
    query = """
    INSERT INTO sf_leads (
        Id,
        IsDeleted,
        MasterRecordId,
        LastName,
        FirstName,
        Salutation,
        Name,
        Title,
        Company,
        Street,
        City,
        State,
        PostalCode,
        Country,
        Latitude,
        Longitude,
        GeocodeAccuracy,
        Address,
        Phone,
        MobilePhone,
        Website,
        LeadSource,
        Status,
        Industry,
        Rating,
        NumberOfEmployees,
        OwnerId,
        IsConverted,
        ConvertedDate,
        ConvertedAccountId,
        ConvertedContactId,
        ConvertedOpportunityId,
        IsUnreadByOwner,
        CreatedDate,
        CreatedById,
        LastModifiedDate,
        LastModifiedById,
        SystemModstamp,
        LastActivityDate,
        LastViewedDate,
        LastReferencedDate,
        Jigsaw,
        JigsawContactId,
        EmailBouncedReason,
        EmailBouncedDate,
        Acreage__c,
        Country_Code__c,
        Date_of_Birth__c,
        Gender__c,
        Lead_AMT_Customer_Id__c,
        Installation_Date__c,
        Lead_Category__c,
        Lead_Channel__c,
        Location__c,
        Payment_Method__c,
        Preferred_Language__c,
        Product_del__c,
        Purchase_Date__c,
        Water_Source_Distance__c,
        Water_Source__c,
        leadcap__Facebook_Lead_ID__c,
        Customer_Type__c,
        Lead_Model_Category__c,
        ID_Number__c,
        Call_Back_Date__c,
        Follow_Up_Date__c,
        Product__c,
        KYC_Status__c,
        Agent_Phone_Number__c,
        Agent__c,
        Payment_Terms__c,
        Referral_Name__c,
        Referral_ID__c,
        Income_Threshold__c,
        Last_Updated_By__c,
        Daily_Water_Usage__c,
        MobileNumberWithCountryCode__c,
        Lead_Source_Other_Comment__c,
        Total_Dynamic_Head__c,
        Referral_Phone_Number__c,
        Custom_Opportunity_Name__c,
        SMSMessage__c,
        Contact_External_Id_Source__c,
        ContactRegionId__c,
        OpportunityPayPlanId__c,
        AMT_Customer_Name__c,
        Old_AMT_Customer_ID__c,
        Agent_Employee_Number__c,
        Other_Phone__c,
        Lead_Date_Created__c,
        Number_of_Units_Lead__c,
        KRA_Pin__c,
        Customer_to_Claim_VAT__c,
        Customer_Product_of_Interest__c,
        My_lead_Filter__c,
        Referral_Source_Application__c,
        Agent_Referral_SMSBody__c,
        Through_Partner_Lead__c,
        Unique_Phone_Number__c,
        Through_Partner_Customer__c,
        Referral_Lead_ID__c,
        CDS1Tracker__c,
        CDS_Status__c,
        Survey_Stat__c,
        SADM_Account__c,
        SADM_CDS_ID__c,
        SADM_Customer__c,
        SADM_KYC_Date__c,
        SADM_CDS1_Date__c,
        SADM_CDS2_Date__c,
        SADM_Customer_Creation_Date__c,
        SADM_Deposit_Date__c,
        SADM_FIRST_MONTH_INSTALLMENT__c,
        SADM_JSF_Date__c,
        SADM_Status__c,
        Employee_ID__c,
        Employee_Name__c,
        Employee_Phone__c
    )
    VALUES (
        %(Id)s,
        %(IsDeleted)s,
        %(MasterRecordId)s,
        %(LastName)s,
        %(FirstName)s,
        %(Salutation)s,
        %(Name)s,
        %(Title)s,
        %(Company)s,
        %(Street)s,
        %(City)s,
        %(State)s,
        %(PostalCode)s,
        %(Country)s,
        %(Latitude)s,
        %(Longitude)s,
        %(GeocodeAccuracy)s,
        %(Address)s,
        %(Phone)s,
        %(MobilePhone)s,
        %(Website)s,
        %(LeadSource)s,
        %(Status)s,
        %(Industry)s,
        %(Rating)s,
        %(NumberOfEmployees)s,
        %(OwnerId)s,
        %(IsConverted)s,
        %(ConvertedDate)s,
        %(ConvertedAccountId)s,
        %(ConvertedContactId)s,
        %(ConvertedOpportunityId)s,
        %(IsUnreadByOwner)s,
        %(CreatedDate)s,
        %(CreatedById)s,
        %(LastModifiedDate)s,
        %(LastModifiedById)s,
        %(SystemModstamp)s,
        %(LastActivityDate)s,
        %(LastViewedDate)s,
        %(LastReferencedDate)s,
        %(Jigsaw)s,
        %(JigsawContactId)s,
        %(EmailBouncedReason)s,
        %(EmailBouncedDate)s,
        %(Acreage__c)s,
        %(Country_Code__c)s,
        %(Date_of_Birth__c)s,
        %(Gender__c)s,
        %(Lead_AMT_Customer_Id__c)s,
        %(Installation_Date__c)s,
        %(Lead_Category__c)s,
        %(Lead_Channel__c)s,
        %(Location__c)s,
        %(Payment_Method__c)s,
        %(Preferred_Language__c)s,
        %(Product_del__c)s,
        %(Purchase_Date__c)s,
        %(Water_Source_Distance__c)s,
        %(Water_Source__c)s,
        %(leadcap__Facebook_Lead_ID__c)s,
        %(Customer_Type__c)s,
        %(Lead_Model_Category__c)s,
        %(ID_Number__c)s,
        %(Call_Back_Date__c)s,
        %(Follow_Up_Date__c)s,
        %(Product__c)s,
        %(KYC_Status__c)s,
        %(Agent_Phone_Number__c)s,
        %(Agent__c)s,
        %(Payment_Terms__c)s,
        %(Referral_Name__c)s,
        %(Referral_ID__c)s,
        %(Income_Threshold__c)s,
        %(Last_Updated_By__c)s,
        %(Daily_Water_Usage__c)s,
        %(MobileNumberWithCountryCode__c)s,
        %(Lead_Source_Other_Comment__c)s,
        %(Total_Dynamic_Head__c)s,
        %(Referral_Phone_Number__c)s,
        %(Custom_Opportunity_Name__c)s,
        %(SMSMessage__c)s,
        %(Contact_External_Id_Source__c)s,
        %(ContactRegionId__c)s,
        %(OpportunityPayPlanId__c)s,
        %(AMT_Customer_Name__c)s,
        %(Old_AMT_Customer_ID__c)s,
        %(Agent_Employee_Number__c)s,
        %(Other_Phone__c)s,
        %(Lead_Date_Created__c)s,
        %(Number_of_Units_Lead__c)s,
        %(KRA_Pin__c)s,
        %(Customer_to_Claim_VAT__c)s,
        %(Customer_Product_of_Interest__c)s,
        %(My_lead_Filter__c)s,
        %(Referral_Source_Application__c)s,
        %(Agent_Referral_SMSBody__c)s,
        %(Through_Partner_Lead__c)s,
        %(Unique_Phone_Number__c)s,
        %(Through_Partner_Customer__c)s,
        %(Referral_Lead_ID__c)s,
        %(CDS1Tracker__c)s,
        %(CDS_Status__c)s,
        %(Survey_Stat__c)s,
        %(SADM_Account__c)s,
        %(SADM_CDS_ID__c)s,
        %(SADM_Customer__c)s,
        %(SADM_KYC_Date__c)s,
        %(SADM_CDS1_Date__c)s,
        %(SADM_CDS2_Date__c)s,
        %(SADM_Customer_Creation_Date__c)s,
        %(SADM_Deposit_Date__c)s,
        %(SADM_FIRST_MONTH_INSTALLMENT__c)s,
        %(SADM_JSF_Date__c)s,
        %(SADM_Status__c)s,
        %(Employee_ID__c)s,
        %(Employee_Name__c)s,
        %(Employee_Phone__c)s
    )
    ON CONFLICT (Id) 
    DO UPDATE SET 
    IsDeleted = EXCLUDED.IsDeleted,
    MasterRecordId = EXCLUDED.MasterRecordId,
    LastName = EXCLUDED.LastName,
    FirstName = EXCLUDED.FirstName,
    Salutation = EXCLUDED.Salutation,
    Name = EXCLUDED.Name,
    Title = EXCLUDED.Title,
    Company = EXCLUDED.Company,
    Street = EXCLUDED.Street,
    City = EXCLUDED.City,
    State = EXCLUDED.State,
    PostalCode = EXCLUDED.PostalCode,
    Country = EXCLUDED.Country,
    Latitude = EXCLUDED.Latitude,
    Longitude = EXCLUDED.Longitude,
    GeocodeAccuracy = EXCLUDED.GeocodeAccuracy,
    Address = EXCLUDED.Address,
    Phone = EXCLUDED.Phone,
    MobilePhone = EXCLUDED.MobilePhone,
    Website = EXCLUDED.Website,
    LeadSource = EXCLUDED.LeadSource,
    Status = EXCLUDED.Status,
    Industry = EXCLUDED.Industry,
    Rating = EXCLUDED.Rating,
    NumberOfEmployees = EXCLUDED.NumberOfEmployees,
    OwnerId = EXCLUDED.OwnerId,
    IsConverted = EXCLUDED.IsConverted,
    ConvertedDate = EXCLUDED.ConvertedDate,
    ConvertedAccountId = EXCLUDED.ConvertedAccountId,
    ConvertedContactId = EXCLUDED.ConvertedContactId,
    ConvertedOpportunityId = EXCLUDED.ConvertedOpportunityId,
    IsUnreadByOwner = EXCLUDED.IsUnreadByOwner,
    CreatedDate = EXCLUDED.CreatedDate,
    CreatedById = EXCLUDED.CreatedById,
    LastModifiedDate = EXCLUDED.LastModifiedDate,
    LastModifiedById = EXCLUDED.LastModifiedById,
    SystemModstamp = EXCLUDED.SystemModstamp,
    LastActivityDate = EXCLUDED.LastActivityDate,
    LastViewedDate = EXCLUDED.LastViewedDate,
    LastReferencedDate = EXCLUDED.LastReferencedDate,
    Jigsaw = EXCLUDED.Jigsaw,
    JigsawContactId = EXCLUDED.JigsawContactId,
    EmailBouncedReason = EXCLUDED.EmailBouncedReason,
    EmailBouncedDate = EXCLUDED.EmailBouncedDate,
    Acreage__c = EXCLUDED.Acreage__c,
    Country_Code__c = EXCLUDED.Country_Code__c,
    Date_of_Birth__c = EXCLUDED.Date_of_Birth__c,
    Gender__c = EXCLUDED.Gender__c,
    Lead_AMT_Customer_Id__c = EXCLUDED.Lead_AMT_Customer_Id__c,
    Installation_Date__c = EXCLUDED.Installation_Date__c,
    Lead_Category__c = EXCLUDED.Lead_Category__c,
    Lead_Channel__c = EXCLUDED.Lead_Channel__c,
    Location__c = EXCLUDED.Location__c,
    Payment_Method__c = EXCLUDED.Payment_Method__c,
    Preferred_Language__c = EXCLUDED.Preferred_Language__c,
    Product_del__c = EXCLUDED.Product_del__c,
    Purchase_Date__c = EXCLUDED.Purchase_Date__c,
    Water_Source_Distance__c = EXCLUDED.Water_Source_Distance__c,
    Water_Source__c = EXCLUDED.Water_Source__c,
    leadcap__Facebook_Lead_ID__c = EXCLUDED.leadcap__Facebook_Lead_ID__c,
    Customer_Type__c = EXCLUDED.Customer_Type__c,
    Lead_Model_Category__c = EXCLUDED.Lead_Model_Category__c,
    ID_Number__c = EXCLUDED.ID_Number__c,
    Call_Back_Date__c = EXCLUDED.Call_Back_Date__c,
    Follow_Up_Date__c = EXCLUDED.Follow_Up_Date__c,
    Product__c = EXCLUDED.Product__c,
    KYC_Status__c = EXCLUDED.KYC_Status__c,
    Agent_Phone_Number__c = EXCLUDED.Agent_Phone_Number__c,
    Agent__c = EXCLUDED.Agent__c,
    Payment_Terms__c = EXCLUDED.Payment_Terms__c,
    Referral_Name__c = EXCLUDED.Referral_Name__c,
    Referral_ID__c = EXCLUDED.Referral_ID__c,
    Income_Threshold__c = EXCLUDED.Income_Threshold__c,
    Last_Updated_By__c = EXCLUDED.Last_Updated_By__c,
    Daily_Water_Usage__c = EXCLUDED.Daily_Water_Usage__c,
    MobileNumberWithCountryCode__c = EXCLUDED.MobileNumberWithCountryCode__c,
    Lead_Source_Other_Comment__c = EXCLUDED.Lead_Source_Other_Comment__c,
    Total_Dynamic_Head__c = EXCLUDED.Total_Dynamic_Head__c,
    Referral_Phone_Number__c = EXCLUDED.Referral_Phone_Number__c,
    Custom_Opportunity_Name__c = EXCLUDED.Custom_Opportunity_Name__c,
    SMSMessage__c = EXCLUDED.SMSMessage__c,
    Contact_External_Id_Source__c = EXCLUDED.Contact_External_Id_Source__c,
    ContactRegionId__c = EXCLUDED.ContactRegionId__c,
    OpportunityPayPlanId__c = EXCLUDED.OpportunityPayPlanId__c,
    AMT_Customer_Name__c = EXCLUDED.AMT_Customer_Name__c,
    Old_AMT_Customer_ID__c = EXCLUDED.Old_AMT_Customer_ID__c,
    Agent_Employee_Number__c = EXCLUDED.Agent_Employee_Number__c,
    Other_Phone__c = EXCLUDED.Other_Phone__c,
    Lead_Date_Created__c = EXCLUDED.Lead_Date_Created__c,
    Number_of_Units_Lead__c = EXCLUDED.Number_of_Units_Lead__c,
    KRA_Pin__c = EXCLUDED.KRA_Pin__c,
    Customer_to_Claim_VAT__c = EXCLUDED.Customer_to_Claim_VAT__c,
    Customer_Product_of_Interest__c = EXCLUDED.Customer_Product_of_Interest__c,
    My_lead_Filter__c = EXCLUDED.My_lead_Filter__c,
    Referral_Source_Application__c = EXCLUDED.Referral_Source_Application__c,
    Agent_Referral_SMSBody__c = EXCLUDED.Agent_Referral_SMSBody__c,
    Through_Partner_Lead__c = EXCLUDED.Through_Partner_Lead__c,
    Unique_Phone_Number__c = EXCLUDED.Unique_Phone_Number__c,
    Through_Partner_Customer__c = EXCLUDED.Through_Partner_Customer__c,
    Referral_Lead_ID__c = EXCLUDED.Referral_Lead_ID__c,
    CDS1Tracker__c = EXCLUDED.CDS1Tracker__c,
    CDS_Status__c = EXCLUDED.CDS_Status__c,
    Survey_Stat__c = EXCLUDED.Survey_Stat__c,
    SADM_Account__c = EXCLUDED.SADM_Account__c,
    SADM_CDS_ID__c = EXCLUDED.SADM_CDS_ID__c,
    SADM_Customer__c = EXCLUDED.SADM_Customer__c,
    SADM_KYC_Date__c = EXCLUDED.SADM_KYC_Date__c,
    SADM_CDS1_Date__c = EXCLUDED.SADM_CDS1_Date__c,
    SADM_CDS2_Date__c = EXCLUDED.SADM_CDS2_Date__c,
    SADM_Customer_Creation_Date__c = EXCLUDED.SADM_Customer_Creation_Date__c,
    SADM_Deposit_Date__c = EXCLUDED.SADM_Deposit_Date__c,
    SADM_FIRST_MONTH_INSTALLMENT__c = EXCLUDED.SADM_FIRST_MONTH_INSTALLMENT__c,
    SADM_JSF_Date__c = EXCLUDED.SADM_JSF_Date__c,
    SADM_Status__c = EXCLUDED.SADM_Status__c,
    Employee_ID__c = EXCLUDED.Employee_ID__c,
    Employee_Name__c = EXCLUDED.Employee_Name__c,
    Employee_Phone__c = EXCLUDED.Employee_Phone__c
    """
    try:
        # Prepare data
        data = []
        for lead in leads:
            # Check each field for length violations
            lead_data = {
                # Map all fields here
                'Id': lead.get('Id'),
                'IsDeleted': lead.get('IsDeleted'),
                'MasterRecordId': lead.get('MasterRecordId'),
                'LastName': lead.get('LastName'),
                'FirstName': lead.get('FirstName'),
                'Salutation': lead.get('Salutation'),
                'Name': lead.get('Name'),
                'Title': lead.get('Title'),
                'Company': lead.get('Company'),
                'Street': lead.get('Street'),
                'City': lead.get('City'),
                'State': lead.get('State'),
                'PostalCode': lead.get('PostalCode'),
                'Country': lead.get('Country'),
                'Latitude': lead.get('Latitude'),
                'Longitude': lead.get('Longitude'),
                'GeocodeAccuracy': lead.get('GeocodeAccuracy'),
                'Address': lead.get('Address'),
                'Phone': lead.get('Phone'),
                'MobilePhone': lead.get('MobilePhone'),
                'Website': lead.get('Website'),
                'LeadSource': lead.get('LeadSource'),
                'Status': lead.get('Status'),
                'Industry': lead.get('Industry'),
                'Rating': lead.get('Rating'),
                'NumberOfEmployees': lead.get('NumberOfEmployees'),
                'OwnerId': lead.get('OwnerId'),
                'IsConverted': lead.get('IsConverted'),
                'ConvertedDate': lead.get('ConvertedDate'),
                'ConvertedAccountId': lead.get('ConvertedAccountId'),
                'ConvertedContactId': lead.get('ConvertedContactId'),
                'ConvertedOpportunityId': lead.get('ConvertedOpportunityId'),
                'IsUnreadByOwner': lead.get('IsUnreadByOwner'),
                'CreatedDate': lead.get('CreatedDate'),
                'CreatedById': lead.get('CreatedById'),
                'LastModifiedDate': lead.get('LastModifiedDate'),
                'LastModifiedById': lead.get('LastModifiedById'),
                'SystemModstamp': lead.get('SystemModstamp'),
                'LastActivityDate': lead.get('LastActivityDate'),
                'LastViewedDate': lead.get('LastViewedDate'),
                'LastReferencedDate': lead.get('LastReferencedDate'),
                'Jigsaw': lead.get('Jigsaw'),
                'JigsawContactId': lead.get('JigsawContactId'),
                'EmailBouncedReason': lead.get('EmailBouncedReason'),
                'EmailBouncedDate': lead.get('EmailBouncedDate'),
                'Acreage__c': lead.get('Acreage__c'),
                'Country_Code__c': lead.get('Country_Code__c'),
                'Date_of_Birth__c': lead.get('Date_of_Birth__c'),
                'Gender__c': lead.get('Gender__c'),
                'Lead_AMT_Customer_Id__c': lead.get('Lead_AMT_Customer_Id__c'),
                'Installation_Date__c': lead.get('Installation_Date__c'),
                'Lead_Category__c': lead.get('Lead_Category__c'),
                'Lead_Channel__c': lead.get('Lead_Channel__c'),
                'Location__c': lead.get('Location__c'),
                'Payment_Method__c': lead.get('Payment_Method__c'),
                'Preferred_Language__c': lead.get('Preferred_Language__c'),
                'Product_del__c': lead.get('Product_del__c'),
                'Purchase_Date__c': lead.get('Purchase_Date__c'),
                'Water_Source_Distance__c': lead.get('Water_Source_Distance__c'),
                'Water_Source__c': lead.get('Water_Source__c'),
                'leadcap__Facebook_Lead_ID__c': lead.get('leadcap__Facebook_Lead_ID__c'),
                'Customer_Type__c': lead.get('Customer_Type__c'),
                'Lead_Model_Category__c': lead.get('Lead_Model_Category__c'),
                'ID_Number__c': lead.get('ID_Number__c'),
                'Call_Back_Date__c': lead.get('Call_Back_Date__c'),
                'Follow_Up_Date__c': lead.get('Follow_Up_Date__c'),
                'Product__c': lead.get('Product__c'),
                'KYC_Status__c': lead.get('KYC_Status__c'),
                'Agent_Phone_Number__c': lead.get('Agent_Phone_Number__c'),
                'Agent__c': lead.get('Agent__c'),
                'Payment_Terms__c': lead.get('Payment_Terms__c'),
                'Referral_Name__c': lead.get('Referral_Name__c'),
                'Referral_ID__c': lead.get('Referral_ID__c'),
                'Income_Threshold__c': lead.get('Income_Threshold__c'),
                'Last_Updated_By__c': lead.get('Last_Updated_By__c'),
                'Daily_Water_Usage__c': lead.get('Daily_Water_Usage__c'),
                'MobileNumberWithCountryCode__c': lead.get('MobileNumberWithCountryCode__c'),
                'Lead_Source_Other_Comment__c': lead.get('Lead_Source_Other_Comment__c'),
                'Total_Dynamic_Head__c': lead.get('Total_Dynamic_Head__c'),
                'Referral_Phone_Number__c': lead.get('Referral_Phone_Number__c'),
                'Custom_Opportunity_Name__c': lead.get('Custom_Opportunity_Name__c'),
                'SMSMessage__c': lead.get('SMSMessage__c'),
                'Contact_External_Id_Source__c': lead.get('Contact_External_Id_Source__c'),
                'ContactRegionId__c': lead.get('ContactRegionId__c'),
                'OpportunityPayPlanId__c': lead.get('OpportunityPayPlanId__c'),
                'AMT_Customer_Name__c': lead.get('AMT_Customer_Name__c'),
                'Old_AMT_Customer_ID__c': lead.get('Old_AMT_Customer_ID__c'),
                'Agent_Employee_Number__c': lead.get('Agent_Employee_Number__c'),
                'Other_Phone__c': lead.get('Other_Phone__c'),
                'Lead_Date_Created__c': lead.get('Lead_Date_Created__c'),
                'Number_of_Units_Lead__c': lead.get('Number_of_Units_Lead__c'),
                'KRA_Pin__c': lead.get('KRA_Pin__c'),
                'Customer_to_Claim_VAT__c': lead.get('Customer_to_Claim_VAT__c'),
                'Customer_Product_of_Interest__c': lead.get('Customer_Product_of_Interest__c'),
                'My_lead_Filter__c': lead.get('My_lead_Filter__c'),
                'Referral_Source_Application__c': lead.get('Referral_Source_Application__c'),
                'Agent_Referral_SMSBody__c': lead.get('Agent_Referral_SMSBody__c'),
                'Through_Partner_Lead__c': lead.get('Through_Partner_Lead__c'),
                'Unique_Phone_Number__c': lead.get('Unique_Phone_Number__c'),
                'Through_Partner_Customer__c': lead.get('Through_Partner_Customer__c'),
                'Referral_Lead_ID__c': lead.get('Referral_Lead_ID__c'),
                'CDS1Tracker__c': lead.get('CDS1Tracker__c'),
                'CDS_Status__c': lead.get('CDS_Status__c'),
                'Survey_Stat__c': lead.get('Survey_Stat__c'),
                'SADM_Account__c': lead.get('SADM_Account__c'),
                'SADM_CDS_ID__c': lead.get('SADM_CDS_ID__c'),
                'SADM_Customer__c': lead.get('SADM_Customer__c'),
                'SADM_KYC_Date__c': lead.get('SADM_KYC_Date__c'),
                'SADM_CDS1_Date__c': lead.get('SADM_CDS1_Date__c'),
                'SADM_CDS2_Date__c': lead.get('SADM_CDS2_Date__c'),
                'SADM_Customer_Creation_Date__c': lead.get('SADM_Customer_Creation_Date__c'),
                'SADM_Deposit_Date__c': lead.get('SADM_Deposit_Date__c'),
                'SADM_FIRST_MONTH_INSTALLMENT__c': lead.get('SADM_FIRST_MONTH_INSTALLMENT__c'),
                'SADM_JSF_Date__c': lead.get('SADM_JSF_Date__c'),
                'SADM_Status__c': lead.get('SADM_Status__c'),
                'Employee_ID__c': lead.get('Employee_ID__c'),
                'Employee_Name__c': lead.get('Employee_Name__c'),
                'Employee_Phone__c': lead.get('Employee_Phone__c')
            }
            # Validate and truncate fields if necessary
            for field_name, value in lead_data.items():
                if isinstance(value, str):
                    # Check if the value exceeds the PostgreSQL column length
                    if len(value) > 18:  # Adjust the length limit as needed
                        #print(f"Warning: Field '{field_name}' exceeds length limit. Value: '{value}'")
                        lead_data[field_name] = value[:18]  # Truncate to 18 characters
            data.append(lead_data)
        
        # Attempt to insert leads in batches
        execute_batch(cursor, query, data)
        db_connection.commit()
        print(f"Successfully upserted {len(leads)} leads")
    except Exception as e:
        db_connection.rollback()
        # Log the full exception for debugging
        print(f"Error saving leads: {str(e)}")
        print("Traceback:", traceback.format_exc())  # Log full traceback
        
    finally:
        cursor.close()
        db_connection.close()

def main():
    """Main function to orchestrate the process."""
    # Track start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    
    # Get Salesforce token and fetch leads (as before)
    access_token, instance_url = get_salesforce_token()

    if not access_token:
        print("Authentication failed. Exiting.")
        exit(1)
        
    print("\nFetching leads from Salesforce...")
    leads = fetch_salesforce_leads(access_token, instance_url)
    
    if not leads:
        print("No leads found or error occurred during fetch.")
        exit(2)
        
    print(f"Found {len(leads)} leads to process")
    
    # Save leads to PostgreSQL
    print("\nSaving leads to PostgreSQL...")
    save_to_postgresql(leads)
    print("Process completed successfully!")
    
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