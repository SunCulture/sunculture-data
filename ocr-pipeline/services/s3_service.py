import boto3
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, SUPPORTED_EXTENSIONS
import logging

logger = logging.getLogger(__name__)

# Initialize S3 client and verify connection
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    # Test S3 connection by listing buckets
    s3_client.list_buckets()
    logger.info("Successfully connected to AWS S3")
except Exception as e:
    logger.error(f"Failed to connect to AWS S3: {e}")
    raise

def download_file_from_s3(bucket, file_key, temp_file):
    try:
        s3_client.download_file(bucket, file_key, temp_file)
        logger.info(f"Successfully downloaded {file_key} from bucket {bucket} to {temp_file}")
    except Exception as e:
        logger.error(f"Error downloading file {file_key} from S3 bucket {bucket}: {e}")
        raise

def list_files_in_folder(bucket, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [
            obj['Key'] for obj in response.get('Contents', [])
            if any(obj['Key'].lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS)
        ]
        
        # Handle pagination
        while response.get('IsTruncated', False):
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            files.extend([
                obj['Key'] for obj in response.get('Contents', [])
                if any(obj['Key'].lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS)
            ])
        
        logger.info(f"Successfully listed {len(files)} supported files in {bucket}/{prefix}")
        return files
    except Exception as e:
        logger.error(f"Error listing files in {bucket}/{prefix}: {e}")
        raise

def upload_json_to_s3(bucket, file_key, json_data):
    """
    Upload JSON data to S3 with a modified file key (e.g., append .json).
    Returns the S3 path of the uploaded file.
    """
    try:
        import json
        json_file_key = f"processed/{file_key}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=json_file_key,
            Body=json.dumps(json_data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Successfully uploaded JSON to s3://{bucket}/{json_file_key}")
        return json_file_key
    except Exception as e:
        logger.error(f"Error uploading JSON to s3://{bucket}/{json_file_key}: {e}")
        raise

def check_file_exists(bucket, file_key):
    """
    Check if a file exists in S3 by attempting to retrieve its metadata.
    Returns True if the file exists, False if it does not.
    """
    try:
        # Check for the actual source file
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except Exception as e:
        if '404' in str(e) or 'NoSuchKey' in str(e):
            return False
        logger.error(f"Error checking file existence for {bucket}/{file_key}: {e}")
        raise