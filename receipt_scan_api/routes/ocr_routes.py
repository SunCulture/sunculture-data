from flask import Blueprint, jsonify, request
from services.db_service import get_db_engine
from services.ocr_service import extract_text, parse_receipt
import logging
import uuid
from sqlalchemy import text
import json

logger = logging.getLogger(__name__)
ocr_routes = Blueprint('ocr_routes', __name__)

@ocr_routes.route("/process/<int:receipt_id>", methods=["POST"])
def process_receipt(receipt_id):
    request_id = str(uuid.uuid4())
    logger.info(f"Processing receipt ID: {receipt_id}", extra={'request_id': request_id})
    
    engine = get_db_engine()
    if not engine:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""UPDATE ocr_queue SET status = 'processing', started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE receipt_id = :receipt_id AND status = 'pending'"""),
                {"receipt_id": receipt_id}
            )
            
            result = conn.execute(
                text("SELECT image, file_type FROM receipts WHERE id = :receipt_id"),
                {"receipt_id": receipt_id}
            )
            record = result.fetchone()
            if not record:
                logger.error(f"Receipt ID {receipt_id} not found", extra={'request_id': request_id})
                conn.execute(
                    text("""UPDATE ocr_queue SET status = 'failed', error_message = :error, updated_at = CURRENT_TIMESTAMP
                            WHERE receipt_id = :receipt_id"""),
                    {"receipt_id": receipt_id, "error": "Receipt not found"}
                )
                return jsonify({"error": "Receipt not found"}), 404
            
            image_data, file_type = record
            raw_text = extract_text(image_data, file_type)
            if not raw_text:
                logger.error(f"OCR failed for receipt ID {receipt_id}", extra={'request_id': request_id})
                conn.execute(
                    text("""UPDATE ocr_queue SET status = 'failed', error_message = :error, updated_at = CURRENT_TIMESTAMP
                            WHERE receipt_id = :receipt_id"""),
                    {"receipt_id": receipt_id, "error": "OCR processing failed"}
                )
                return jsonify({"error": "OCR processing failed"}), 500
            
            ocr_result = parse_receipt(raw_text)
            
            conn.execute(
                text("""INSERT INTO ocr_results (receipt_id, raw_text, merchant_name, total_amount, receipt_date, confidence_score, extracted_data, created_at)
                        VALUES (:receipt_id, :raw_text, :merchant_name, :total_amount, :receipt_date, :confidence_score, :extracted_data, CURRENT_TIMESTAMP)"""),
                {
                    "receipt_id": receipt_id,
                    "raw_text": raw_text,
                    "merchant_name": ocr_result["merchant_name"],
                    "total_amount": ocr_result["total_amount"],
                    "receipt_date": ocr_result["receipt_date"],
                    "confidence_score": ocr_result["confidence_score"],
                    "extracted_data": json.dumps(ocr_result["extracted_data"])
                }
            )
            
            conn.execute(
                text("""UPDATE ocr_queue SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE receipt_id = :receipt_id"""),
                {"receipt_id": receipt_id}
            )
            
            logger.info(f"Completed OCR for receipt ID: {receipt_id}", extra={'request_id': request_id})
            return jsonify({"status": "completed", "receipt_id": receipt_id}), 200

    except Exception as e:
        logger.error(f"Error processing receipt ID {receipt_id}: {str(e)}", extra={'request_id': request_id})
        with engine.begin() as conn:
            conn.execute(
                text("""UPDATE ocr_queue SET status = 'failed', error_message = :error, updated_at = CURRENT_TIMESTAMP
                        WHERE receipt_id = :receipt_id"""),
                {"receipt_id": receipt_id, "error": str(e)}
            )
        return jsonify({"error": str(e)}), 500

@ocr_routes.route("/results/<int:receipt_id>", methods=["GET"])
def get_results(receipt_id):
    request_id = str(uuid.uuid4())
    logger.info(f"Fetching results for receipt ID: {receipt_id}", extra={'request_id': request_id})
    
    engine = get_db_engine()
    if not engine:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT oq.status, oq.error_message, ore.raw_text, ore.merchant_name, ore.total_amount, ore.receipt_date, ore.confidence_score, ore.extracted_data
                    FROM ocr_queue oq
                    LEFT JOIN ocr_results ore ON oq.receipt_id = ore.receipt_id
                    WHERE oq.receipt_id = :receipt_id
                """),
                {"receipt_id": receipt_id}
            )
            record = result.fetchone()
            if not record:
                logger.error(f"No results for receipt ID {receipt_id}", extra={'request_id': request_id})
                return jsonify({"error": "No results found"}), 404
            
            extracted_data = record[7]
            if isinstance(extracted_data, str):
                extracted_data = json.loads(extracted_data)
            elif not isinstance(extracted_data, dict):
                extracted_data = {}

            response = {
                "receipt_id": receipt_id,
                "status": record[0],
                "error_message": record[1],
                "results": {
                    "raw_text": record[2],
                    "merchant_name": record[3],
                    "total_amount": float(record[4]) if record[4] is not None else None,
                    "receipt_date": record[5],
                    "confidence_score": float(record[6]) if record[6] is not None else None,
                    "extracted_data": extracted_data
                }
            }
            logger.info(f"Retrieved results for receipt ID: {receipt_id}", extra={'request_id': request_id})
            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching results for receipt ID {receipt_id}: {str(e)}", extra={'request_id': request_id})
        return jsonify({"error": str(e)}), 500