import boto3
import json
import requests
import threading
import time
import logging
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, SQS_QUEUE_URL

logger = logging.getLogger(__name__)

# Initialize SQS client
try:
    sqs_client = boto3.client(
        'sqs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    logger.info("Successfully connected to AWS SQS")
except Exception as e:
    logger.error(f"Failed to connect to AWS SQS: {e}")
    raise

def poll_sqs():
    """Background thread to poll SQS queue and process messages."""
    if not SQS_QUEUE_URL:
        logger.error("SQS_QUEUE_URL not set. Cannot poll SQS.")
        return

    logger.info(f"Starting SQS polling for queue: {SQS_QUEUE_URL}")
    while True:
        try:
            # Receive messages from SQS (up to 10 at a time)
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20  # Long polling to reduce empty responses
            )
            
            messages = response.get('Messages', [])
            if not messages:
                logger.debug("No messages in queue, continuing to poll...")
                continue

            for message in messages:
                try:
                    # Parse S3 event from message (direct S3-to-SQS event, no SNS wrapper)
                    s3_event = json.loads(message['Body'])
                    # Verify the message is an S3 event
                    if 'Records' not in s3_event or not s3_event['Records']:
                        logger.warning(f"Invalid SQS message format, skipping: {message['Body']}")
                        # Delete the message to avoid reprocessing invalid messages
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        continue

                    bucket = s3_event['Records'][0]['s3']['bucket']['name']
                    file_key = s3_event['Records'][0]['s3']['object']['key']
                    logger.info(f"Processing SQS message for file: s3://{bucket}/{file_key}")

                    # Call the Flask /process-file endpoint internally
                    payload = {'file_key': file_key}
                    response = requests.post(
                        'http://localhost:5001/ocr/process-file',
                        json=payload,
                        headers={'Content-Type': 'application/json'}
                    )

                    if response.status_code == 200:
                        logger.info(f"Successfully processed file: s3://{bucket}/{file_key}")
                        # Delete message from queue after successful processing
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logger.info("Message deleted from queue")
                    else:
                        logger.error(f"Failed to process file s3://{bucket}/{file_key}: {response.text}")
                        # Do not delete message; let visibility timeout handle retries

                except Exception as e:
                    logger.error(f"Error processing SQS message: {str(e)}")
                    # Optionally, move to DLQ or retry based on your strategy

        except Exception as e:
            logger.error(f"Error polling SQS: {str(e)}")
            time.sleep(5)  # Wait before retrying on queue errors

def start_sqs_polling():
    """Start SQS polling in a background thread."""
    threading.Thread(target=poll_sqs, daemon=True).start()