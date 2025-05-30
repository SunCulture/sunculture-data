import boto3
import json
import re
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
import logging
from typing import Dict, List, Optional, Tuple, Any

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

# Enhanced regex patterns
DATE_REGEX = re.compile(r'^\d{1,2}[-/]\d{1,2}[-/](\d{2}|\d{4})$')
AMOUNT_REGEX = re.compile(r'^[\d,.]+(\.?\d{0,2})?$')
CURRENCY_REGEX = re.compile(r'^(KES|USD|EUR|GBP)\s*[\d,.]+$', re.IGNORECASE)

def is_valid_date(date_str: str) -> bool:
    """Enhanced date validation supporting multiple formats"""
    if not date_str:
        return False
    # Clean the date string
    cleaned = re.sub(r'[^\d/-]', '', date_str.strip())
    return bool(DATE_REGEX.match(cleaned))

def is_valid_amount(amount_str: str) -> bool:
    """Check if string represents a valid monetary amount"""
    if not amount_str:
        return False
    cleaned = re.sub(r'[^\d.,]', '', amount_str.strip())
    return bool(AMOUNT_REGEX.match(cleaned))

def clean_date(date_str: str) -> Optional[str]:
    """Clean and validate date strings"""
    if not date_str:
        return None
    
    # Extract date pattern from string
    date_match = re.search(r'\d{1,2}[-/]\d{1,2}[-/](\d{2}|\d{4})', date_str)
    if date_match and is_valid_date(date_match.group(0)):
        return date_match.group(0)
    return None

def clean_amount(amount_str: str) -> Optional[str]:
    """Clean and validate amount strings"""
    if not amount_str:
        return None
    
    # Remove currency symbols and extra spaces
    cleaned = re.sub(r'[^\d.,]', '', amount_str.strip())
    if is_valid_amount(cleaned):
        return cleaned
    return None

def detect_table_structure(table_rows: List[Dict]) -> Tuple[List[str], int]:
    """
    Dynamically detect table headers and data start index
    Returns: (headers, start_row_index)
    """
    if not table_rows:
        return [], 0
    
    # Look for potential header patterns
    header_indicators = ['expense', 'type', 'amount', 'cost', 'currency', 'description', 'category']
    start_idx = 0
    headers = []
    
    # Find the first row that looks like data (not headers/labels)
    for idx, row in enumerate(table_rows):
        row_values = [str(v).lower().strip() for v in row.values() if v]
        
        # Skip empty rows
        if not row_values:
            continue
            
        # Skip rows with day names or common labels
        skip_indicators = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 
                          'id number', 'cell phone', 'week ending', 'total']
        if any(indicator in ' '.join(row_values) for indicator in skip_indicators):
            start_idx = idx + 1
            continue
            
        # Check if this row contains actual data (amounts or expense types)
        has_amount = any(is_valid_amount(str(v)) for v in row.values())
        has_expense_data = any(len(str(v).strip()) > 2 and not str(v).isdigit() 
                              for v in row.values() if v)
        
        if has_amount or has_expense_data:
            # This looks like a data row, previous structure analysis complete
            break
        
        start_idx = idx + 1
    
    # Generate dynamic headers based on column count and content analysis
    if table_rows and start_idx < len(table_rows):
        sample_row = table_rows[start_idx] if start_idx < len(table_rows) else table_rows[0]
        col_count = max(sample_row.keys()) + 1 if sample_row else 0
        
        # Analyze first few data rows to understand structure
        expense_cols = []
        amount_cols = []
        
        for row_idx in range(start_idx, min(start_idx + 3, len(table_rows))):
            if row_idx >= len(table_rows):
                break
            row = table_rows[row_idx]
            for col_idx, value in row.items():
                if is_valid_amount(str(value)):
                    if col_idx not in amount_cols:
                        amount_cols.append(col_idx)
                elif value and len(str(value).strip()) > 2:
                    if col_idx not in expense_cols:
                        expense_cols.append(col_idx)
        
        # Generate headers based on analysis
        headers = []
        for i in range(col_count):
            if i in amount_cols:
                headers.append(f'Amount_{len([h for h in headers if h.startswith("Amount")])}'.replace('_0', ''))
            elif i in expense_cols:
                headers.append(f'Expense_Type_{len([h for h in headers if h.startswith("Expense")])}'.replace('_0', ''))
            else:
                headers.append(f'Column_{i}')
    
    return headers, start_idx

def extract_table_data_enhanced(table_rows: List[Dict]) -> List[Dict]:
    """Enhanced table data extraction with dynamic structure detection"""
    if not table_rows:
        return []
    
    headers, start_idx = detect_table_structure(table_rows)
    if not headers or start_idx >= len(table_rows):
        return []
    
    items = []
    for row_idx in range(start_idx, len(table_rows)):
        row = table_rows[row_idx]
        if not any(row.values()):  # Skip empty rows
            continue
        
        item = {}
        for col_idx, header in enumerate(headers):
            value = row.get(col_idx, '')
            if value:
                # Clean amounts
                if 'amount' in header.lower():
                    cleaned_amount = clean_amount(str(value))
                    if cleaned_amount:
                        item[header] = cleaned_amount
                else:
                    item[header] = str(value).strip()
        
        # Only include rows with meaningful data
        if any(item.values()):
            items.append(item)
    
    return items

def validate_extracted_data(form_data: Dict) -> Dict:
    """
    Validate and add confidence scores to extracted data
    Returns data with validation results and confidence scores
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
    
    # Check for critical fields
    critical_fields = ['Date', 'Total Amount Requested', 'Bill Total', 'Name of Employee']
    found_critical = sum(1 for field in critical_fields if any(field.lower() in k.lower() for k in form_data.keys()))
    
    if found_critical > 0:
        validation_result['validation']['has_critical_fields'] = True
    else:
        validation_result['validation']['issues'].append('Missing critical fields')
    
    # Validate items
    items = form_data.get('Items', [])
    if items:
        complete_items = 0
        for item in items:
            has_expense = any('expense' in k.lower() or 'type' in k.lower() for k, v in item.items() if v)
            has_amount = any('amount' in k.lower() for k, v in item.items() if v and is_valid_amount(str(v)))
            if has_expense and has_amount:
                complete_items += 1
        
        if complete_items > 0:
            validation_result['validation']['has_complete_items'] = True
            validation_result['validation']['completion_rate'] = complete_items / len(items)
        else:
            validation_result['validation']['issues'].append('No complete expense items found')
    else:
        validation_result['validation']['issues'].append('No expense items extracted')
    
    # Calculate confidence score
    score = 0.0
    if validation_result['validation']['has_critical_fields']:
        score += 0.4
    if validation_result['validation']['has_complete_items']:
        score += 0.6 * validation_result['validation'].get('completion_rate', 0)
    
    validation_result['validation']['confidence_score'] = round(score, 2)
    
    return validation_result

def extract_text_from_file(file_path: str) -> str:
    """
    Enhanced text extraction with improved error handling and validation
    """
    try:
        with open(file_path, 'rb') as file:
            file_bytes = file.read()
            
        # Use AnalyzeDocument with FORMS and TABLES
        response = textract_client.analyze_document(
            Document={'Bytes': file_bytes},
            FeatureTypes=['FORMS', 'TABLES']
        )
        
        # Initialize result dictionary
        form_data = {}
        table_data = []
        current_table = None
        
        # Create block lookup for efficient processing
        blocks_by_id = {block['Id']: block for block in response.get('Blocks', [])}
        
        # Process blocks with enhanced logic
        for block in response.get('Blocks', []):
            if block['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in block.get('EntityTypes', []):
                    # Extract key
                    key = extract_text_from_block(block, blocks_by_id)
                    
                    # Find corresponding value
                    for rel in block.get('Relationships', []):
                        if rel['Type'] == 'VALUE':
                            for value_id in rel['Ids']:
                                value_block = blocks_by_id.get(value_id)
                                if value_block:
                                    value = extract_text_from_block(value_block, blocks_by_id)
                                    if key and value:
                                        # Enhanced field processing
                                        if 'date' in key.lower():
                                            cleaned_date = clean_date(value)
                                            if cleaned_date:
                                                form_data[key] = cleaned_date
                                            else:
                                                logger.warning(f"Invalid date for key '{key}': {value}")
                                        elif any(term in key.lower() for term in ['amount', 'total', 'cost']):
                                            cleaned_amount = clean_amount(value)
                                            if cleaned_amount:
                                                form_data[key] = cleaned_amount
                                            else:
                                                form_data[key] = value  # Keep original if cleaning fails
                                        else:
                                            form_data[key] = value

            elif block['BlockType'] == 'TABLE':
                current_table = {'rows': []}
                table_data.append(current_table)
            
            elif block['BlockType'] == 'CELL' and current_table is not None:
                row_index = block.get('RowIndex', 1) - 1
                col_index = block.get('ColumnIndex', 1) - 1
                text = extract_text_from_block(block, blocks_by_id)

                # Ensure row exists
                while len(current_table['rows']) <= row_index:
                    current_table['rows'].append({})
                current_table['rows'][row_index][col_index] = text

        # Enhanced table processing
        if table_data:
            all_items = []
            for table in table_data:
                if table['rows']:
                    items = extract_table_data_enhanced(table['rows'])
                    all_items.extend(items)
            
            if all_items:
                form_data['Items'] = all_items

        # Enhanced fallback with better field extraction
        critical_fields = ['Date', 'Total Amount Requested', 'Bill Total', 'Name of Employee']
        missing_critical = [field for field in critical_fields 
                           if not any(field.lower() in k.lower() for k in form_data.keys())]
        
        if missing_critical:
            logger.info(f"Attempting fallback extraction for missing fields: {missing_critical}")
            
            # Get raw text
            raw_response = textract_client.detect_document_text(Document={'Bytes': file_bytes})
            raw_text = '\n'.join([item['Text'] for item in raw_response.get('Blocks', []) 
                                 if item['BlockType'] == 'LINE'])
            
            # Enhanced regex extraction
            fallback_patterns = {
                'Date': [
                    r'Date[:\s]*(\d{1,2}[-/]\d{1,2}[-/](?:\d{2}|\d{4}))',
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+\s*,?\s*\d{4})'
                ],
                'Total Amount Requested': [
                    r'Total Amount Requested[:\s]*([\d,.]+)',
                    r'Total[:\s]*([\d,.]+)'
                ],
                'Name of Employee': [
                    r'Name of Employee[:\s]*([A-Za-z\s]+)',
                    r'Employee[:\s]*([A-Za-z\s]+)'
                ]
            }
            
            for field in missing_critical:
                patterns = fallback_patterns.get(field, [])
                for pattern in patterns:
                    match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
                    if match:
                        extracted_value = match.group(1).strip()
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
                            form_data[field] = extracted_value
                            break
            
            # Include raw text only if still missing critical data
            if not any(field.lower() in ' '.join(form_data.keys()).lower() for field in critical_fields):
                form_data['raw_text'] = raw_text.strip()

        # Validate and return results
        validated_result = validate_extracted_data(form_data)
        
        logger.info(f"Successfully extracted data from {file_path} with confidence score: {validated_result['validation']['confidence_score']}")
        if validated_result['validation']['issues']:
            logger.warning(f"Validation issues: {validated_result['validation']['issues']}")
        
        return json.dumps(validated_result, indent=2)

    except Exception as e:
        logger.error(f"Error extracting data from {file_path}: {e}")
        # Return partial results instead of raising
        error_result = {
            'data': {'error': str(e)},
            'validation': {
                'has_critical_fields': False,
                'has_complete_items': False,
                'confidence_score': 0.0,
                'issues': [f'Extraction failed: {str(e)}']
            }
        }
        return json.dumps(error_result, indent=2)

def extract_text_from_block(block: Dict, blocks_by_id: Dict) -> str:
    """Helper function to extract text from a block and its children"""
    text = ''
    for rel in block.get('Relationships', []):
        if rel['Type'] == 'CHILD':
            for child_id in rel['Ids']:
                child_block = blocks_by_id.get(child_id)
                if child_block and child_block['BlockType'] == 'WORD':
                    text += child_block['Text'] + ' '
    return text.strip()