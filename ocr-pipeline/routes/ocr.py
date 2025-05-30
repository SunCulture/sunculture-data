from flask import Blueprint, request, jsonify
from services.s3_service import download_file_from_s3, list_files_in_folder
from services.textract_service import extract_text_from_file
from services.db_service import save_to_db
from werkzeug.utils import secure_filename
from config.settings import S3_BUCKET, SUPPORTED_EXTENSIONS
import logging
import os
import json
import psycopg2
from config.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

ocr_bp = Blueprint('ocr', __name__)
logger = logging.getLogger(__name__)

def check_duplicate(file_key):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM extracted_text WHERE file_name = %s", (file_key,))
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        logger.error(f"Error checking duplicate for {file_key}: {e}")
        return False

@ocr_bp.route('/process-file', methods=['POST'])
def process_file():
    try:
        data = request.get_json()
        if not data or 'file_key' not in data:
            logger.error("Missing file_key in request payload")
            return jsonify({'error': 'Missing file_key'}), 400

        file_key = data['file_key'].strip()  # Trim whitespace
        logger.info(f"Received request to process file: {file_key}")
        
        # Check for duplicate
        if check_duplicate(file_key):
            logger.warning(f"Duplicate file detected: {file_key}")
            return jsonify({'error': 'File already processed', 'file_key': file_key}), 409

        # Check if file has extension, if not, skip validation (assume it's processable)
        basename = os.path.basename(file_key)
        has_extension = any(basename.lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS)
        has_any_extension = '.' in basename
        
        logger.info(f"File extension check - has supported extension: {has_extension}, has any extension: {has_any_extension}")
        
        # Only validate extension if the file has an extension
        if has_any_extension and not has_extension:
            return jsonify({'error': f'Unsupported file type. Supported extensions: {SUPPORTED_EXTENSIONS}'}), 400

        # Create temp directory if it doesn't exist
        os.makedirs('/tmp', exist_ok=True)
        
        # Download file from S3
        filename = os.path.basename(file_key)
        temp_file = f"/tmp/{secure_filename(filename)}"
        download_file_from_s3(S3_BUCKET, file_key, temp_file)

        # Process with Textract
        extracted_json = extract_text_from_file(temp_file)  # JSON string
        extracted_data = json.loads(extracted_json)  # Parse for response

        # Save to database
        record_id = save_to_db(file_key, extracted_json)  # Save JSON string

        # Clean up
        if os.path.exists(temp_file):
            os.remove(temp_file)

        logger.info(f"Successfully processed file: {file_key}, record ID: {record_id}")
        return jsonify({
            'status': 'success',
            'file_key': file_key,
            'extracted_data': extracted_data,  # Return parsed JSON
            'record_id': record_id
        }), 200

    except Exception as e:
        logger.error(f"Error processing file {file_key if 'file_key' in locals() else 'unknown'}: {e}")
        return jsonify({'error': str(e)}), 500

@ocr_bp.route('/process-all-files', methods=['POST'])
def process_all_files():
    try:
        logger.info("Received request to process all files in dev/employee-documents/")
        # List all files in dev/employee-documents/
        prefix = 'dev/employee-documents/'
        files = list_files_in_folder(S3_BUCKET, prefix)

        if not files:
            logger.info("No supported files found in dev/employee-documents/")
            return jsonify({'status': 'success', 'message': 'No supported files found in dev/employee-documents/'}), 200

        results = []
        for file_key in files:
            try:
                logger.info(f"Processing file: {file_key}")
                
                # Create temp directory if it doesn't exist
                os.makedirs('/tmp', exist_ok=True)
                
                # Download file
                filename = os.path.basename(file_key)
                temp_file = f"/tmp/{secure_filename(filename)}"
                download_file_from_s3(S3_BUCKET, file_key, temp_file)

                # Process with Textract
                extracted_json = extract_text_from_file(temp_file)  # JSON string
                extracted_data = json.loads(extracted_json)  # Parse for response

                # Save to database
                record_id = save_to_db(file_key, extracted_json)  # Save JSON string

                # Add to results
                results.append({
                    'file_key': file_key,
                    'record_id': record_id,
                    'status': 'success'
                })

                # Clean up
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                logger.info(f"Successfully processed file: {file_key}, record ID: {record_id}")
            except Exception as e:
                logger.error(f"Error processing {file_key}: {e}")
                results.append({
                    'file_key': file_key,
                    'record_id': None,
                    'status': 'failed',
                    'error': str(e)
                })

        logger.info(f"Completed processing {len(files)} files: {sum(1 for r in results if r['status'] == 'success')} successful, {sum(1 for r in results if r['status'] == 'failed')} failed")
        return jsonify({
            'status': 'success',
            'processed_files': results,
            'total_files': len(files),
            'successful': sum(1 for r in results if r['status'] == 'success'),
            'failed': sum(1 for r in results if r['status'] == 'failed')
        }), 200

    except Exception as e:
        logger.error(f"Error processing all files: {e}")
        return jsonify({'error': str(e)}), 500

# Health check endpoint
@ocr_bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200