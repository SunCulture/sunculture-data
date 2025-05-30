# services/textract_service.py
import boto3
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
    # Test Textract connection (no direct API to test, but initialization is enough)
    logger.info("Successfully connected to AWS Textract")
except Exception as e:
    logger.error(f"Failed to connect to AWS Textract: {e}")
    raise

def extract_text_from_file(file_path):
    try:
        with open(file_path, 'rb') as file:
            response = textract_client.detect_document_text(
                Document={'Bytes': file.read()}
            )
        
        extracted_text = ''
        for item in response.get('Blocks', []):
            if item['BlockType'] == 'LINE':
                extracted_text += item['Text'] + '\n'
        
        logger.info(f"Successfully extracted text from {file_path}")
        return extracted_text
    except Exception as e:
        logger.error(f"Error extracting text with Textract from {file_path}: {e}")
        raise