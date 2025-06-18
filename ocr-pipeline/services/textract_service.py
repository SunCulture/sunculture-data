import boto3
import json
import re
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Initialize Textract client and verify connection
try:
    textract_client = boto3.client(
        'textract',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    logger.info("Successfully connected to AWS Textract")
except Exception as e:
    logger.error(f"Failed to connect to AWS Textract: {e}")
    raise

# Enhanced regex patterns for date extraction
DATE_REGEX_MMDDYYYY = re.compile(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2}|\d{4})')  # MM/DD/YYYY or MM-DD-YYYY
DATE_REGEX_YYYYMMDD = re.compile(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})')       # YYYY-MM-DD or YYYY/MM/DD
DATE_REGEX_DDMMYYYY = re.compile(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2}|\d{4})')  # DD-MM-YYYY or DD/MM/YYYY
DATE_REGEX_DDMMYY = re.compile(r'(\d{1,2})[.](\d{1,2})[.](\d{2})')           # DD.MM.YY
DATE_REGEX_DDMMMYYYY = re.compile(r'(\d{1,2})[ -](\w{3,9})[ -](\d{4})', re.IGNORECASE)  # e.g., 30-May-2025

# List of prohibited items (alcoholic beverages)
PROHIBITED_ITEMS = [
    'beer', 'wine', 'whiskey', 'whisky', 'vodva', 'gin', 'rum', 'tequila',
    'brandy', 'cognac', 'champagne', 'sake', 'cider', 'ale', 'lager', 'stout',
    'port', 'sherry', 'vermouth', 'absinthe', 'liquor', 'spirit', 'alcohol'
]

# Expanded keywords for expense items
EXPENSE_KEYWORDS = [
    'lunch', 'transport', 'travel', 'meal', 'fuel', 'accommodation', 'facilitation',
    'delivery', 'chicken', 'vanilla', 'charges', 'mishkaki', 'food', 'service'
]

# Regex for amount, currency, and vendor name validation
AMOUNT_REGEX = re.compile(r'^[\d,.]+(\.?\d{0,2})?$')
CURRENCY_REGEX = re.compile(r'^(KES|UGX|XOF|USD|EUR|GBP)\s*[\d,.]+$', re.IGNORECASE)
VENDOR_NAME_REGEX = re.compile(r'^[A-Za-z0-9\s&\'-]+$', re.IGNORECASE)  # Updated to allow apostrophes

def safe_float(value: str) -> Optional[float]:
    """Safely convert a string to float, returning None if conversion fails."""
    try:
        return float(value.replace(',', ''))
    except (ValueError, TypeError):
        logger.debug(f"Failed to convert '{value}' to float")
        return None

def is_valid_date(date_str: str) -> bool:
    """Validate date string by checking if it matches any known format."""
    if not date_str:
        return False
    cleaned = re.sub(r'[^\d./-]', '', date_str.strip())
    return bool(
        DATE_REGEX_MMDDYYYY.match(cleaned) or 
        DATE_REGEX_YYYYMMDD.match(cleaned) or 
        DATE_REGEX_DDMMYYYY.match(cleaned) or 
        DATE_REGEX_DDMMYY.match(cleaned) or
        DATE_REGEX_DDMMMYYYY.search(date_str)
    )

def is_valid_amount(amount_str: str) -> bool:
    """Check if string represents a valid monetary amount."""
    if not amount_str:
        return False
    cleaned = re.sub(r'[^\d.,]', '', amount_str.strip())
    return bool(AMOUNT_REGEX.match(cleaned))

def clean_date(date_str: str) -> Optional[str]:
    """Clean and validate date strings, preserving the original format and padding 2-digit years."""
    if not date_str:
        return None

    current_year = datetime.now().year
    current_date = datetime.now().strftime("%d-%m-%Y")
    separator = next((sep for sep in ['.', '/', '-'] if sep in date_str), '/')

    # Month mapping for textual months
    month_map = {
        'jan': '01', 'january': '01',
        'feb': '02', 'february': '02',
        'mar': '03', 'march': '03',
        'apr': '04', 'april': '04',
        'may': '05',
        'jun': '06', 'june': '06',
        'jul': '07', 'july': '07',
        'aug': '08', 'august': '08',
        'sep': '09', 'september': '09',
        'oct': '10', 'october': '10',
        'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
    }

    # Try DD-MMM-YYYY (e.g., 30-May-2025)
    ddmmmyyyy_match = DATE_REGEX_DDMMMYYYY.search(date_str)
    if ddmmmyyyy_match:
        day, month_str, year = ddmmmyyyy_match.groups()
        logger.debug(f"DDMMMYYYY match: day={day}, month_str={month_str}, year={year}")
        month_str = month_str.lower()
        month = month_map.get(month_str)
        if month and len(year) == 4:
            try:
                parsed_date = datetime.strptime(f"{day.zfill(2)}-{month}-{year}", "%d-%m-%Y")
                logger.debug(f"Validated date: {parsed_date.strftime('%d-%m-%Y')}")
                return f"{day.zfill(2)}{separator}{month}{separator}{year}"
            except ValueError:
                logger.warning(f"Invalid date components: {day}-{month_str}-{year}")
                return f"{day.zfill(2)}{separator}{month}{separator}{current_year}"
        logger.warning(f"Invalid month or year in {date_str}")
        return f"{day.zfill(2)}{separator}{month}{separator}{current_year}"

    # Try DD.MM.YY first
    ddmmyy_match = DATE_REGEX_DDMMYY.search(date_str)
    if ddmmyy_match:
        day, month, year = ddmmyy_match.groups()
        year_int = int(year)
        if year_int <= 50:
            year = str(year_int + 2000)
        else:
            year = str(year_int + 1900)
        if abs(int(year) - current_year) > 10:
            year = str(current_year)
        return f"{day.zfill(2)}{separator}{month.zfill(2)}{separator}{year}"

    # Try MM/DD/YYYY or MM-DD-YYYY
    mmddyyyy_match = DATE_REGEX_MMDDYYYY.search(date_str)
    if mmddyyyy_match:
        month, day, year = mmddyyyy_match.groups()
        if len(year) == 2:
            year_int = int(year)
            if year_int <= 50:
                year = str(year_int + 2000)
            else:
                year = str(year_int + 1900)
            if abs(int(year) - current_year) > 10:
                year = str(current_year)
        return f"{month.zfill(2)}{separator}{day.zfill(2)}{separator}{year}"

    # Try YYYY-MM-DD or YYYY/MM/DD
    yyyymmdd_match = DATE_REGEX_YYYYMMDD.search(date_str)
    if yyyymmdd_match:
        year, month, day = yyyymmdd_match.groups()
        if len(year) == 2:
            year_int = int(year)
            if year_int <= 50:
                year = str(year_int + 2000)
            else:
                year = str(year_int + 1900)
            if abs(int(year) - current_year) > 10:
                year = str(current_year)
        return f"{year}{separator}{month.zfill(2)}{separator}{day.zfill(2)}"

    # Try DD-MM-YYYY or DD/MM/YYYY
    ddmmyyyy_match = DATE_REGEX_DDMMYYYY.search(date_str)
    if ddmmyyyy_match:
        day, month, year = ddmmyyyy_match.groups()
        if len(year) == 2:
            year_int = int(year)
            if year_int <= 50:
                year = str(year_int + 2000)
            else:
                year = str(year_int + 1900)
            if abs(int(year) - current_year) > 10:
                year = str(current_year)
        return f"{day.zfill(2)}{separator}{month.zfill(2)}{separator}{year}"

    logger.warning(f"Could not parse date: {date_str}")
    return current_date  # Default to current date if all parsing fails

def clean_amount(amount_str: str) -> Optional[str]:
    """Clean and validate amount strings."""
    if not amount_str:
        return None
    cleaned = re.sub(r'[^\d.,]', '', amount_str.strip())
    if is_valid_amount(cleaned):
        return cleaned
    numeric_match = re.search(r'[\d,.]+', amount_str)
    if numeric_match:
        cleaned = numeric_match.group(0)
        if is_valid_amount(cleaned):
            return cleaned
    logger.warning(f"Could not clean amount: {amount_str}")
    return None

def clean_vendor_name(vendor_str: str) -> Optional[str]:
    """Clean and validate vendor name strings."""
    if not vendor_str:
        return None
    cleaned = vendor_str.strip()
    if len(cleaned) < 2 or len(cleaned) > 100:  # Reasonable length for business names
        logger.warning(f"Invalid vendor name length: {cleaned}")
        return None
    if VENDOR_NAME_REGEX.match(cleaned):
        return cleaned
    logger.warning(f"Invalid vendor name format: {cleaned}")
    return None

def detect_currency(form_data: Dict, raw_text: Optional[str] = None) -> str:
    """Detect the currency used in the receipt data."""
    currencies = {
        'KES': ['KES', 'Ksh', 'Kenya Shilling'],
        'UGX': ['UGX', 'Ush', 'Ugandan Shilling'],
        'XOF': ['XOF', 'CFA', 'West African CFA Franc'],
        'USD': ['USD', '$', 'US Dollar'],
        'EUR': ['EUR', '€', 'Euro'],
        'GBP': ['GBP', '£', 'British Pound']
    }
    
    text_to_search = raw_text or ''
    if 'Items' in form_data:
        text_to_search += ' ' + ' '.join(f"{item.get('Amount', '')} {item.get('Description', '')}" for item in form_data['Items'])
    if 'Total Amount Requested' in form_data:
        text_to_search += ' ' + form_data['Total Amount Requested']
    if 'Vendor Name' in form_data:
        text_to_search += ' ' + form_data['Vendor Name']
    
    text_to_search = text_to_search.lower().strip()
    
    for currency_code, indicators in currencies.items():
        for indicator in indicators:
            if re.search(rf'\b{re.escape(indicator.lower())}\b', text_to_search):
                logger.info(f"Detected currency: {currency_code} (indicator: {indicator})")
                return currency_code
    
    logger.warning("No currency detected in the data")
    return "Not Detected"

def validate_extracted_data(form_data: Dict) -> Dict:
    """
    Validate and add confidence scores to extracted data.
    Returns data with validation results and confidence scores.
    """
    validation_result = {
        'data': form_data,
        'validation': {
            'has_critical_fields': False,
            'has_complete_items': False,
            'confidence_score': 0.0,
            'issues': []
        }
    }
    
    # Check for critical fields with flexible matching
    critical_field_patterns = {
        'date': ['date', 'when'],
        'amount': ['total', 'amount', 'cost', 'sum'],
        'vendor': ['vendor', 'merchant', 'provider', 'hotel', 'airline'],
        'signature': ['signature', 'sign']
    }
    
    found_critical = {}
    for category, patterns in critical_field_patterns.items():
        for key in form_data.keys():
            if any(pattern in key.lower() for pattern in patterns):
                found_critical[category] = key
                break
    
    if len(found_critical) >= 2:
        validation_result['validation']['has_critical_fields'] = True
    else:
        validation_result['validation']['issues'].append(f'Missing critical fields. Found: {list(found_critical.keys())}')
    
    # Validate items
    items = form_data.get('Items', [])
    if items:
        valid_items = []
        for item in items:
            has_description = any(
                v and len(str(v)) > 5 and
                (any(keyword in str(v).lower() for keyword in EXPENSE_KEYWORDS) or len(str(v).split()) > 1)
                for k, v in item.items() if 'description' in k.lower() or 'expense' in k.lower()
            )
            has_amount = any(
                v and is_valid_amount(str(v)) and (safe_float(str(v)) or 0) > 0
                for k, v in item.items() if 'amount' in k.lower()
            )
            if has_description and has_amount:
                valid_items.append(item)
            else:
                logger.debug(f"Item failed validation: {item} (has_description={has_description}, has_amount={has_amount})")
        
        if valid_items:
            validation_result['validation']['has_complete_items'] = True
            validation_result['validation']['completion_rate'] = len(valid_items) / len(items)
            validation_result['validation']['valid_items_count'] = len(valid_items)
            form_data['Items'] = valid_items
        else:
            validation_result['validation']['issues'].append('No valid expense items found (missing description or amount)')
    else:
        validation_result['validation']['issues'].append('No expense items extracted')
    
    # Calculate confidence score
    score = 0.0
    if validation_result['validation']['has_critical_fields']:
        critical_score = len(found_critical) / len(critical_field_patterns)
        score += 0.4 * critical_score
    if validation_result['validation']['has_complete_items']:
        items_score = validation_result['validation'].get('completion_rate', 0)
        score += 0.6 * items_score
    validation_result['validation']['confidence_score'] = round(score, 2)
    
    return validation_result

def check_for_prohibited_items(form_data: Dict) -> bool:
    """
    Check if the extracted data contains prohibited items (alcoholic beverages).
    Returns True if prohibited items are found, False otherwise.
    """
    logger.debug(f"Checking for prohibited items in: {form_data}")
    items = form_data.get('Items', [])
    for item in items:
        for key, value in item.items():
            if isinstance(value, str):
                value_lower = value.lower()
                logger.debug(f"Checking item value '{value_lower}' for prohibited items")
                if any(prohibited in value_lower for prohibited in PROHIBITED_ITEMS):
                    logger.info(f"Prohibited item found in item {key}: {value}")
                    return True
    
    for key, value in form_data.items():
        if key == 'Items':
            continue
        if isinstance(key, str):
            key_lower = key.lower()
            logger.debug(f"Checking key '{key_lower}' for prohibited items")
            if any(prohibited in key_lower for prohibited in PROHIBITED_ITEMS):
                logger.info(f"Prohibited item found in key '{key}': {key}")
                return True
        if isinstance(value, str):
            value_lower = value.lower()
            logger.debug(f"Checking value for key '{key}' with value '{value_lower}' for prohibited items")
            if any(prohibited in value_lower for prohibited in PROHIBITED_ITEMS):
                logger.info(f"Prohibited item found in {key}: {value}")
                return True
    
    logger.debug("No prohibited items found")
    return False

def extract_text_from_file(file_path: str) -> Dict:
    """
    Extract text using AnalyzeExpense API for better receipt processing.
    Includes validation, prohibited items detection, and fallback logic.
    """
    try:
        with open(file_path, 'rb') as file:
            file_bytes = file.read()
        
        # Use AnalyzeExpense for receipt-specific extraction
        response = textract_client.analyze_expense(Document={'Bytes': file_bytes})
        
        # Parse the AnalyzeExpense response
        form_data = {}
        total_confidence = 0.0
        confidence_count = 0
        
        for expense_doc in response.get('ExpenseDocuments', []):
            # Extract summary fields
            for field in expense_doc.get('SummaryFields', []):
                field_type = field.get('Type', {}).get('Text')
                value = field.get('ValueDetection', {}).get('Text')
                confidence = field.get('ValueDetection', {}).get('Confidence', 0.0)
                
                if field_type == 'INVOICE_RECEIPT_DATE':
                    cleaned_date = clean_date(value)
                    if cleaned_date:
                        form_data['Date'] = cleaned_date
                elif field_type == 'TOTAL':
                    cleaned_amount = clean_amount(value)
                    if cleaned_amount:
                        form_data['Total Amount Requested'] = cleaned_amount
                elif field_type in ['VENDOR_NAME', 'RECEIVER_NAME', 'MERCHANT_NAME']:
                    cleaned_vendor = clean_vendor_name(value)
                    if cleaned_vendor:
                        form_data['Vendor Name'] = cleaned_vendor
                
                if confidence:
                    total_confidence += confidence
                    confidence_count += 1
            
            # Extract line items (expense items)
            items = []
            for line_item_group in expense_doc.get('LineItemGroups', []):
                for line_item in line_item_group.get('LineItems', []):
                    item = {}
                    for field in line_item.get('LineItemExpenseFields', []):
                        field_type = field.get('Type', {}).get('Text')
                        value = field.get('ValueDetection', {}).get('Text')
                        confidence = field.get('ValueDetection', {}).get('Confidence', 0.0)
                        if field_type == 'ITEM':
                            item['Description'] = value
                        elif field_type == 'PRICE':
                            cleaned_amount = clean_amount(value)
                            if cleaned_amount:
                                item['Amount'] = cleaned_amount
                        if confidence:
                            total_confidence += confidence
                            confidence_count += 1
                    if 'Description' in item and 'Amount' in item:
                        items.append(item)
            
            if items:
                form_data['Items'] = items
        
        # Calculate average confidence score
        if confidence_count > 0:
            form_data['confidence_score'] = total_confidence / confidence_count
        
        # Fallback extraction if critical fields are missing
        critical_fields = ['Date', 'Total Amount Requested', 'Vendor Name']
        missing_critical = [field for field in critical_fields 
                           if field not in form_data or not form_data[field]]
        
        raw_text = None
        if missing_critical:
            logger.info(f"Attempting fallback extraction for missing fields: {missing_critical}")
            raw_response = textract_client.detect_document_text(Document={'Bytes': file_bytes})
            raw_text = '\n'.join([item['Text'] for item in raw_response.get('Blocks', []) 
                                 if item['BlockType'] == 'LINE'])
            
            fallback_patterns = {
                'Date': [
                    r'Date[:\s]*(\d{1,2}[ -]\w{3,9}[ -]\d{4})',  # e.g., 30-May-2025
                    r'Date[:\s]*(\d{1,2}[./-]\d{1,2}[./-]\d{2})',  # DD.MM.YY
                    r'Date[:\s]*(\d{4}[./-]\d{1,2}[./-]\d{1,2})',  # YYYY-MM-DD
                    r'(\d{1,2}[./-]\d{1,2}[./-]\d{2})',            # DD.MM.YY
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+\s*,?\s*\d{4})' # e.g., 23rd November 2024
                ],
                'Total Amount Requested': [
                    r'Total Amount Requested[:\s]*([\d,.]+)',
                    r'Total[:\s]*([\d,.]+)'
                ],
                'Vendor Name': [
                    r'Vendor[:\s]*([A-Za-z0-9\s&\'-]+)',
                    r'Merchant[:\s]*([A-Za-z0-9\s&\'-]+)',
                    r'Provider[:\s]*([A-Za-z0-9\s&\'-]+)',
                    r'^([A-Za-z0-9\s&\'-]+)\n'  # Vendor name often at top of receipt
                ]
            }
            
            for field in missing_critical:
                patterns = fallback_patterns.get(field, [])
                for pattern in patterns:
                    match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
                    if match:
                        extracted_value = match.group(1).strip()
                        logger.debug(f"Fallback match for {field} with pattern {pattern}: {extracted_value}")
                        if field == 'Date':
                            cleaned_value = clean_date(extracted_value)
                            if cleaned_value:
                                form_data[field] = cleaned_value
                                break
                        elif 'amount' in field.lower():
                            cleaned_value = clean_amount(extracted_value)
                            if cleaned_value:
                                form_data[field] = cleaned_value
                                break
                        else:
                            cleaned_value = clean_vendor_name(extracted_value)
                            if cleaned_value:
                                form_data[field] = cleaned_value
                                break
            
            if not any(field.lower() in ' '.join(form_data.keys()).lower() for field in critical_fields):
                form_data['raw_text'] = raw_text.strip()
        
        # Detect currency
        form_data['Currency'] = detect_currency(form_data, raw_text)
        
        # Validate the extracted data
        validated_result = validate_extracted_data(form_data)
        
        # Check for prohibited items
        validated_result['has_prohibited_items'] = check_for_prohibited_items(form_data)
        
        logger.info(f"Successfully extracted data from {file_path} with confidence score: {validated_result['validation']['confidence_score']}")
        if validated_result['validation']['issues']:
            logger.warning(f"Validation issues: {validated_result['validation']['issues']}")
        
        return validated_result
    
    except Exception as e:
        logger.error(f"Error extracting data from {file_path}: {e}")
        error_result = {
            'data': {'error': str(e)},
            'validation': {
                'has_critical_fields': False,
                'has_complete_items': False,
                'confidence_score': 0.0,
                'issues': [f'Extraction failed: {str(e)}']
            },
            'has_prohibited_items': False
        }
        return error_result

def extract_data_from_image(file_path: str) -> Dict:
    """
    Extract data from an image file using AWS Textract.
    Returns a dictionary containing the extracted and validated data.
    """
    logger.info(f"Extracting data from image: {file_path}")
    result = extract_text_from_file(file_path)
    return result