import boto3
import json
import re
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
import logging

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

# Regex for date validation (DD-MM-YY or DD-MM-YYYY)
DATE_REGEX = re.compile(r'^\d{2}-\d{2}-(\d{2}|\d{4})$')

def is_valid_date(date_str):
    return bool(DATE_REGEX.match(date_str))

def clean_date(date_str):
    if is_valid_date(date_str):
        return date_str
    return None  # Return None for invalid dates

def extract_text_from_file(file_path):
    try:
        with open(file_path, 'rb') as file:
            # Use AnalyzeDocument with FORMS and TABLES
            response = textract_client.analyze_document(
                Document={'Bytes': file.read()},
                FeatureTypes=['FORMS', 'TABLES']
            )
        
        # Initialize result dictionary
        form_data = {}
        table_data = []
        current_table = None

        # Process blocks
        for block in response.get('Blocks', []):
            if block['BlockType'] == 'KEY_VALUE_SET':
                if block['EntityTypes'][0] == 'KEY':
                    key = ''
                    for rel in block.get('Relationships', []):
                        if rel['Type'] == 'CHILD':
                            for child_id in rel['Ids']:
                                child_block = next((b for b in response['Blocks'] if b['Id'] == child_id), None)
                                if child_block and child_block['BlockType'] == 'WORD':
                                    key += child_block['Text'] + ' '
                    key = key.strip()
                elif block['EntityTypes'][0] == 'VALUE':
                    value = ''
                    for rel in block.get('Relationships', []):
                        if rel['Type'] == 'CHILD':
                            for child_id in rel['Ids']:
                                child_block = next((b for b in response['Blocks'] if b['Id'] == child_id), None)
                                if child_block and child_block['BlockType'] == 'WORD':
                                    value += child_block['Text'] + ' '
                    value = value.strip()
                    if key and value:
                        # Clean date fields
                        if 'date' in key.lower():
                            cleaned_value = clean_date(value)
                            if cleaned_value:
                                form_data[key] = cleaned_value
                            else:
                                logger.warning(f"Invalid date for key '{key}': {value}")
                        else:
                            form_data[key] = value

            elif block['BlockType'] == 'TABLE':
                current_table = {'rows': []}
                table_data.append(current_table)
            
            elif block['BlockType'] == 'CELL':
                row_index = block['RowIndex'] - 1
                col_index = block['ColumnIndex'] - 1
                text = ''
                for rel in block.get('Relationships', []):
                    if rel['Type'] == 'CHILD':
                        for child_id in rel['Ids']:
                            child_block = next((b for b in response['Blocks'] if b['Id'] == child_id), None)
                            if child_block and child_block['BlockType'] == 'WORD':
                                text += child_block['Text'] + ' '
                text = text.strip()

                # Ensure row exists
                while len(current_table['rows']) <= row_index:
                    current_table['rows'].append({})
                current_table['rows'][row_index][col_index] = text

        # Process table data
        if table_data:
            items = []
            for table in table_data:
                if not table['rows']:
                    continue
                # Skip metadata rows (e.g., headers with days of week, labels like 'ID Number')
                start_idx = 0
                while start_idx < len(table['rows']):
                    row_values = list(table['rows'][start_idx].values())
                    if (any('KES' in str(v) for v in row_values) or
                        all(not v for v in row_values) or
                        any(day in str(v).lower() for v in row_values for day in ['sun', 'mon', 'tues', 'weds', 'thurs', 'fri', 'sat']) or
                        any(label in str(v).lower() for v in row_values for label in ['id number', 'cell phone number', 'week ending'])):
                        start_idx += 1
                    else:
                        break
                if start_idx >= len(table['rows']):
                    continue

                # Use custom headers for a two-column layout
                headers = ['Expense Type 1', 'Currency 1', 'Expense Type 2', 'Amount']
                for row in table['rows'][start_idx:]:
                    if any(row.values()):
                        item = dict(zip(headers, [row.get(i, '') for i in range(len(headers))]))
                        # Fix amount placement: if Currency 1 is numeric and Amount is empty, move it
                        if item['Currency 1'].replace('.', '').replace(',', '').replace('-', '').isdigit() and not item['Amount']:
                            item['Amount'] = item['Currency 1']
                            item['Currency 1'] = ''  # Could set to 'KES' if currency is known
                        # Only include rows with a meaningful amount or expense type
                        if (item['Amount'] and item['Amount'].replace('.', '').replace(',', '').replace('-', '').isdigit()) or \
                           any(item[f'Expense Type {i}'] for i in [1, 2]):
                            items.append(item)

            if items:
                form_data['Items'] = items

        # Remove redundant fields (e.g., days of the week, expense types already in Items)
        days = ['Sun', 'Mon', 'Tues', 'Weds', 'Thurs', 'Fri', 'Sat']
        for day in days:
            if day in form_data and any(day.lower() in str(item).lower() for item in form_data.get('Items', [])):
                del form_data[day]
        # Remove expense types that match Items
        expense_types = set()
        for item in form_data.get('Items', []):
            for i in [1, 2]:
                expense_type = item.get(f'Expense Type {i}')
                if expense_type:
                    expense_types.add(expense_type)
        for key in list(form_data.keys()):
            if key in expense_types and key != 'Items':
                del form_data[key]

        # Fallback to raw text if critical fields are missing
        critical_fields = ['Date', 'Total Amount Requested', 'Bill Total']
        if not any(field in form_data for field in critical_fields):
            with open(file_path, 'rb') as file:
                raw_response = textract_client.detect_document_text(Document={'Bytes': file.read()})
            raw_text = ''
            for item in raw_response.get('Blocks', []):
                if item['BlockType'] == 'LINE':
                    raw_text += item['Text'] + '\n'
            # Extract missing fields using simple regex
            for field in critical_fields:
                if field == 'Date':
                    date_match = re.search(r'\b\d{2}-\d{2}-(\d{2}|\d{4})\b', raw_text)
                    if date_match:
                        form_data['Date'] = date_match.group(0)
                elif field == 'Total Amount Requested' or field == 'Bill Total':
                    total_match = re.search(r'(?:Total Amount Requested|Bill Total)[:\s]*([\d,.]+)', raw_text, re.IGNORECASE)
                    if total_match:
                        form_data[field] = total_match.group(1)
            if not any(field in form_data for field in critical_fields):
                form_data['raw_text'] = raw_text.strip()

        logger.info(f"Successfully extracted form and table data from {file_path}")
        return json.dumps(form_data)  # Return JSON string

    except Exception as e:
        logger.error(f"Error extracting form and table data with Textract from {file_path}: {e}")
        raise