# JSF Dispatch Updates

This script handles the uploading of JSF and Dispatch action updates to an API endpoint and verifies the changes in a MySQL database.

## Overview

The script processes account data containing JSF and Dispatch actions, uploads them to a specified API endpoint, and then verifies the status changes in a MySQL database to ensure the updates were successful.

## Features

- **Batch Processing**: Handles multiple account actions in a single run
- **Environment Variable Validation**: Ensures all required configuration is present
- **Error Handling**: Comprehensive error handling for API requests and database queries
- **Status Verification**: Queries the database to confirm account status changes
- **Detailed Logging**: Provides detailed output for successful and failed uploads

## Prerequisites

- Python 3.x
- Required Python packages:
  - `mysql-connector-python`
  - `requests`
  - `python-dotenv`
- MySQL database access
- API endpoint with valid API key

## Installation

1. Clone or download the script
2. Install required dependencies:
   ```bash
   pip install mysql-connector-python requests python-dotenv
   ```
3. Create a `.env` file with your configuration (see Configuration section)

## Configuration

Create a `.env` file in the same directory as the script with the following variables:

```env
API_URL=your_api_endpoint_url
API_KEY=your_api_key
DB_HOST=your_database_host
DB_PORT=3306
DB_NAME=your_database_name
DB_USER=your_database_username
DB_PASSWORD=your_database_password
```

## Usage

1. **Prepare your data**: Edit the `accounts_data` list in `main.py` to include your account actions:

   For JSF actions:
   ```python
   {
       "accountRef": "ACCOUNT123",
       "action": "jsf",
       "actionDate": "2025-07-23",
       "jsfId": "JSF456",
       "deviceId": "DEVICE789"
   }
   ```

   For Dispatch actions:
   ```python
   {
       "accountRef": "ACCOUNT123",
       "action": "dispatch",
       "actionDate": "2025-07-23"
   }
   ```

2. **Run the script**:
   ```bash
   python main.py
   ```

## Script Workflow

1. **Environment Validation**: Checks that all required environment variables are set
2. **Data Processing**: Iterates through each account action in the `accounts_data` list
3. **API Upload**: Sends POST requests to the API endpoint with account data
4. **Response Handling**: Logs successful uploads and captures failed attempts
5. **Database Verification**: Queries the MySQL database to verify account status changes
6. **Summary Report**: Provides a summary of successful and failed uploads

## API Payload Structure

### JSF Actions
```json
{
    "accountRef": "string",
    "action": "jsf",
    "actionDate": "YYYY-MM-DD",
    "jsfId": "string",
    "deviceId": "string"
}
```

### Dispatch Actions
```json
{
    "accountRef": "string",
    "action": "dispatch",
    "actionDate": "YYYY-MM-DD"
}
```


## Output

The script provides detailed console output including:

- Progress updates for each account upload
- Success/failure status for each API request
- Summary of successful and failed uploads
- Database verification results showing account statuses
- Error details for troubleshooting

## Error Handling

- **Missing Environment Variables**: Script exits with detailed error message
- **API Request Failures**: Logs detailed error information and continues processing
- **Database Connection Issues**: Handles connection errors gracefully
- **Invalid Data**: Validates account references and handles missing data

## Example Output

```
Starting upload of 2 account actions to https://api.example.com/accounts
--------------------------------------------------------------------------------
Successfully uploaded ACCOUNT123 (jsf): {'message': 'Account updated successfully'}
Successfully uploaded ACCOUNT456 (dispatch): {'message': 'Account updated successfully'}
Upload Summary:
Successful uploads: 2
Failed uploads: 0

🔍 Verifying account statuses in database...
Account Status Verification:
--------------------------------------------------------------------------------
ID     Account Ref     JSF Date     Dispatch Date  Status         
--------------------------------------------------------------------------------
1      ACCOUNT123      2025-07-23   None           jsf_completed  
2      ACCOUNT456      None         2025-07-23     dispatched     
--------------------------------------------------------------------------------
Script execution completed.
```

## Troubleshooting

- **Missing Dependencies**: Install required packages using pip
- **Environment Variables**: Ensure all required variables are set in `.env` file
- **Database Connection**: Verify database credentials and network connectivity
- **API Issues**: Check API endpoint URL and API key validity
- **Data Format**: Ensure account data follows the expected structure