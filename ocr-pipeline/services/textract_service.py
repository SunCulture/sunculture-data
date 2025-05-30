import boto3
import json
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
                # Add text to the appropriate column
                current_table['rows'][row_index][col_index] = text

        # Convert table data to a structured format (e.g., assuming first row is headers)
        if table_data:
            items = []
            for table in table_data:
                headers = table['rows'][0] if table['rows'] else {}
                for row in table['rows'][1:]:  # Skip header row
                    item = {}
                    for col_idx, value in row.items():
                        header = list(headers.keys())[col_idx] if col_idx < len(headers) else f'Column{col_idx+1}'
                        item[header] = value
                    if item:
                        items.append(item)
            if items:
                form_data['Items'] = items

        # Fallback to raw text if no form or table data
        if not form_data:
            raw_response = textract_client.detect_document_text(Document={'Bytes': file.read()})
            raw_text = ''
            for item in raw_response.get('Blocks', []):
                if item['BlockType'] == 'LINE':
                    raw_text += item['Text'] + '\n'
            form_data['raw_text'] = raw_text.strip()

        logger.info(f"Successfully extracted form and table data from {file_path}")
        return json.dumps(form_data)  # Return JSON string

    except Exception as e:
        logger.error(f"Error extracting form and table data with Textract from {file_path}: {e}")
        raise