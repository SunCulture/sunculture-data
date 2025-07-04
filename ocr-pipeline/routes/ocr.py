import logging
import os
import json
from datetime import datetime
from flask import Blueprint, request, jsonify
from services.s3_service import download_file_from_s3, check_file_exists, upload_json_to_s3, list_files_in_folder
from services.textract_service import extract_data_from_image
from config.settings import S3_CASH_RELEASE_BUCKET, S3_OCR_PIPELINE_BUCKET

logger = logging.getLogger(__name__)

ocr_bp = Blueprint('ocr', __name__)

def is_duplicate_file_name(file_key):
    """Check if a file with the same name (excluding path) has been processed."""
    base_name = os.path.basename(file_key)
    processed_prefix = "processed/"
    files = list_files_in_folder(S3_OCR_PIPELINE_BUCKET, processed_prefix)
    for processed_file in files:
        if os.path.basename(processed_file).replace('.json', '') == base_name:
            return True
    return False

@ocr_bp.route('/process-file', methods=['POST'])
def process_file():
    try:
        data = request.get_json()
        if not data or 'file_key' not in data:
            logger.error("No file_key provided in request")
            return jsonify({"error": "No file_key provided"}), 400

        file_key = data['file_key']
        logger.info(f"Received request to process file: s3://{S3_CASH_RELEASE_BUCKET}/{file_key}")

        # Check for duplicate file name
        duplicate_file_name = is_duplicate_file_name(file_key)
        logger.info(f"Duplicate file name check for {file_key}: {duplicate_file_name}")

        # Validate file extension
        if not any(file_key.lower().endswith(ext) for ext in {'.png', '.pdf', '.jpeg', '.jpg'}):
            logger.error(f"Unsupported file format: {file_key}")
            return jsonify({"error": "Unsupported file format"}), 400

        # Check if file exists in S3
        if not check_file_exists(S3_CASH_RELEASE_BUCKET, file_key):
            logger.error(f"File not found in s3://{S3_CASH_RELEASE_BUCKET}/{file_key}")
            return jsonify({"error": "File not found in S3"}), 404

        # Download file
        temp_file = f"/tmp/{os.path.basename(file_key)}"
        logger.debug(f"Attempting to download s3://{S3_CASH_RELEASE_BUCKET}/{file_key} to {temp_file}")
        
        try:
            download_file_from_s3(S3_CASH_RELEASE_BUCKET, file_key, temp_file)
        except Exception as e:
            logger.error(f"Failed to download file from S3: {e}")
            return jsonify({"error": "Failed to download file from S3"}), 500

        # Extract data
        try:
            extracted_data = extract_data_from_image(temp_file)
        except Exception as e:
            logger.error(f"Failed to extract data from image: {e}")
            return jsonify({"error": "Failed to extract data from image"}), 500
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)

        # Prepare JSON output
        data_section = extracted_data.get("data", {})
        
        # Extract receipt_date from data section and move to root
        receipt_date = data_section.get("Date")
        if receipt_date:
            # Remove Date from data section to avoid duplication
            data_section = {k: v for k, v in data_section.items() if k != "Date"}
        
        output_data = {
            "file_key": file_key,
            "duplicate_file_name": duplicate_file_name,
            "receipt_date": receipt_date,
            "scanned_date": datetime.utcnow().isoformat() + "Z",
            "data": data_section,
            "validation": extracted_data.get("validation", {}),
            "has_prohibited_items": extracted_data.get("has_prohibited_items", False)
        }

        # Upload JSON
        try:
            json_key = upload_json_to_s3(S3_OCR_PIPELINE_BUCKET, file_key, output_data)
        except Exception as e:
            logger.error(f"Failed to upload JSON to S3: {e}")
            return jsonify({"error": "Failed to upload processed data to S3"}), 500

        logger.info(f"Successfully processed file: s3://{S3_CASH_RELEASE_BUCKET}/{file_key}, JSON stored at: s3://{S3_OCR_PIPELINE_BUCKET}/{json_key}")
        return jsonify(output_data), 200

    except Exception as e:
        logger.error(f"Unexpected error processing file: {e}")
        return jsonify({"error": "Internal server error"}), 500