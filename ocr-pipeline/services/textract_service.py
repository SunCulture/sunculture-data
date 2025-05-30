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
            response = textract_client.analyze_document(
                Document={'Bytes': file.read()},
                FeatureTypes=['FORMS']
            )
        
        # Parse key-value pairs
        form_data = {}
        for block in response.get('Blocks', []):
            if block['BlockType'] == 'KEY_VALUE_SET':
                if block['EntityTypes'][0] == 'KEY':
                    key = ''
                    value = ''
                    # Get key text
                    for rel in block.get('Relationships', []):
                        if rel['Type'] == 'CHILD':
                            for child_id in rel['Ids']:
                                child_block = next(b for b in response['Blocks'] if b['Id'] == child_id)
                                if child_block['BlockType'] == 'WORD':
                                    key += child_block['Text'] + ' '
                    key = key.strip()
                    # Get value text
                    for rel in block.get('Relationships', []):
                        if rel['Type'] == 'VALUE':
                            for value_id in rel['Ids']:
                                value_block = next(b for b in response['Blocks'] if b['Id'] == value_id)
                                for val_rel in value_block.get('Relationships', []):
                                    if val_rel['Type'] == 'CHILD':
                                        for child_id in val_rel['Ids']:
                                            child_block = next(b for b in response['Blocks'] if b['Id'] == child_id)
                                            if child_block['BlockType'] == 'WORD':
                                                value += child_block['Text'] + ' '
                    value = value.strip()
                    if key and value:
                        form_data[key] = value
        
        logger.info(f"Successfully extracted form data from {file_path}")
        return json.dumps(form_data)  # Return JSON string
    except Exception as e:
        logger.error(f"Error extracting form data with Textract from {file_path}: {e}")
        raise