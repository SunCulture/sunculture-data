# Payment Upload Script

This script sends payment records to an API endpoint using environment variables for configuration. It supports batch processing, secure API communication, and simple logging for the success or failure of each payment.

---

## Overview

- Sends payment data to an API using HTTP POST requests    
- Supports batch processing through a `dataList` array  
- Logs success or error for each payment request  
- Adds a delay between requests to prevent server overload  

---

## Features

- **Batch processing** of multiple payments  
- **Secure environment variable usage** (no hardcoded credentials)  
- **Axios** used for API requests  
- **500ms delay** between requests  
- **Error handling** and clear logging  

---

## ✅ Prerequisites

- Node.js installed  
- Valid API URL and API Key    

---

## ✅ Installation and Setup

Install dependencies:

```bash
npm install axios dotenv
```

Create a `.env` file in the project folder and add:

```env
API_URL=your_api_endpoint
API_KEY=your_api_key
```

Add your payment data inside the `dataList` array in the script:

```javascript
const dataList = [
  {
    'accountRef': '',
    'paymentRef': '',
    'payerName': '',
    'paymentAmount': '',
    'timeStampMade': '',
    'payerNumber': '',
    'paymentTypeId': null, // Numeric ID
    'sourceAmountCurrency': '',
    'selectedCurrency': { id: null }, // Numeric ID
    'selectedBank': { id: '' } // Bank ID as numeric string
  }
];
```

You can add more objects to send multiple payments in one run.

## How to Run

Execute in the terminal:

```bash
node your_script_name.js
```

Example:

```bash
node sendPayments.js
```

## Script Workflow

1. Loads API credentials from `.env` file  
2. Iterates through each payment in `dataList`  
3. Sends POST request to the API with each payment  
4. Logs success or error for each request  
5. Waits 500ms before sending the next request