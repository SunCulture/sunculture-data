import os

# AWS configurations
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

S3_CASH_RELEASE_BUCKET = os.getenv('S3_CASH_RELEASE_BUCKET')  # Bucket for input files
S3_OCR_PIPELINE_BUCKET = os.getenv('S3_OCR_PIPELINE_BUCKET')  # Bucket for JSON outputs

SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL')  # Dead Letter Queue URL

# Supported file extensions
SUPPORTED_EXTENSIONS = {'.png', '.pdf', '.jpeg', '.jpg'}