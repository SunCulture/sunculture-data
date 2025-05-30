import streamlit as st
import psycopg2
from sqlalchemy import create_engine, text
import os
import logging
import traceback
import sys
import uuid
from urllib.parse import quote_plus
import hashlib
import requests

# Configure logging
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'no-request-id'
        return super().format(record)

handler = logging.FileHandler("/app/logs/app.log")
handler.setFormatter(CustomFormatter(
    "%(asctime)s - %(name)s - %(request_id)s - %(levelname)s - %(message)s"
))
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(CustomFormatter(
    "%(asctime)s - %(name)s - %(request_id)s - %(levelname)s - %(message)s"
))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.handlers = []
logger.addHandler(handler)
logger.addHandler(stream_handler)

os.makedirs("/app/logs", exist_ok=True)
logger.info("Initializing Streamlit OCR application")

# Constants
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
ALLOWED_EXTENSIONS = ["png", "jpg", "jpeg", "pdf"]
API_URL = "http://receipt-scan-api:5000/api"

def get_db_engine():
    request_id = str(uuid.uuid4())
    logger.debug("Initializing database engine", extra={'request_id': request_id})
    try:
        db_user = os.getenv("ep_stage_db_user")
        db_pass = quote_plus(os.getenv("ep_stage_db_password") or "")
        db_host = os.getenv("ep_stage_db_host")
        db_port = os.getenv("ep_stage_db_port")
        db_name = os.getenv("ep_stage_db")
        
        connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established", extra={'request_id': request_id})
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}\n{traceback.format_exc()}", extra={'request_id': request_id})
        return None

def validate_file(uploaded_file):
    request_id = str(uuid.uuid4())
    file_name = uploaded_file.name
    logger.debug(f"Validating file: {file_name}", extra={'request_id': request_id})
    
    file_extension = file_name.lower().split(".")[-1]
    if file_extension not in ALLOWED_EXTENSIONS:
        error_msg = f"Unsupported file type: {file_extension}. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    
    image_bytes = uploaded_file.read()
    uploaded_file.seek(0)
    
    if not image_bytes or len(image_bytes) == 0:
        error_msg = f"File {file_name} is empty"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    
    if len(image_bytes) > MAX_FILE_SIZE:
        error_msg = f"File {file_name} too large ({len(image_bytes)} bytes). Max: {MAX_FILE_SIZE} bytes"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    
    if file_extension in ["jpg", "jpeg"] and not image_bytes.startswith(b'\xff\xd8\xff'):
        error_msg = f"File {file_name} not a valid JPEG"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    elif file_extension == "png" and not image_bytes.startswith(b'\x89PNG\r\n\x1a\n'):
        error_msg = f"File {file_name} not a valid PNG"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    elif file_extension == "pdf" and not image_bytes.startswith(b'%PDF'):
        error_msg = f"File {file_name} not a valid PDF"
        logger.error(error_msg, extra={'request_id': request_id})
        return False, error_msg
    
    logger.info(f"File {file_name} passed validation ({len(image_bytes)} bytes)", extra={'request_id': request_id})
    return True, image_bytes

def check_duplicate_filename(engine, file_name):
    request_id = str(uuid.uuid4())
    logger.debug(f"Checking duplicate filename: {file_name}", extra={'request_id': request_id})
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, created_at FROM receipts WHERE file_name = :file_name LIMIT 1"),
                {"file_name": file_name}
            )
            record = result.fetchone()
            if record:
                logger.warning(f"Duplicate filename found: {file_name} (ID: {record[0]})", extra={'request_id': request_id})
                return True, record
            return False, None
    except Exception as e:
        logger.error(f"Error checking duplicate filename: {str(e)}\n{traceback.format_exc()}", extra={'request_id': request_id})
        return False, None

def check_duplicate_content(engine, image_bytes):
    request_id = str(uuid.uuid4())
    file_hash = hashlib.md5(image_bytes).hexdigest()
    logger.debug(f"Checking duplicate content hash: {file_hash}", extra={'request_id': request_id})
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, file_name, created_at FROM receipts WHERE file_hash = :file_hash LIMIT 1"),
                {"file_hash": file_hash}
            )
            record = result.fetchone()
            if record:
                logger.warning(f"Duplicate content found: {file_hash} (ID: {record[0]})", extra={'request_id': request_id})
                return True, record
            return False, None
    except Exception as e:
        logger.error(f"Error checking duplicate content: {str(e)}\n{traceback.format_exc()}", extra={'request_id': request_id})
        return False, None

def store_image(engine, image_bytes, file_name, file_type):
    request_id = str(uuid.uuid4())
    logger.info(f"Storing {file_type} {file_name} ({len(image_bytes)} bytes)", extra={'request_id': request_id})
    
    if not image_bytes:
        error_msg = f"No image data for {file_name}"
        logger.error(error_msg, extra={'request_id': request_id})
        st.error(error_msg)
        return None
    
    try:
        file_hash = hashlib.md5(image_bytes).hexdigest()
        priority = "normal"
        
        with engine.begin() as conn:
            result = conn.execute(
                text("""INSERT INTO receipts (image, file_name, file_hash, file_type, file_size, created_at) 
                        VALUES (:blob_data, :file_name, :file_hash, :file_type, :file_size, CURRENT_TIMESTAMP) 
                        RETURNING id"""),
                {
                    "blob_data": image_bytes,
                    "file_name": file_name,
                    "file_hash": file_hash,
                    "file_type": file_type,
                    "file_size": len(image_bytes)
                }
            )
            receipt_id = result.fetchone()[0]
            logger.info(f"Stored {file_type} {file_name} with ID: {receipt_id}", extra={'request_id': request_id})
            
            result = conn.execute(
                text("""INSERT INTO ocr_queue (receipt_id, status, priority, created_at, updated_at) 
                        VALUES (:receipt_id, 'pending', :priority, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id"""),
                {"receipt_id": receipt_id, "priority": priority}
            )
            queue_id = result.fetchone()[0]
            logger.info(f"Queued OCR job with ID: {queue_id}", extra={'request_id': request_id})
            
            # Trigger API
            response = requests.post(f"{API_URL}/process/{receipt_id}")
            if response.status_code != 200:
                logger.error(f"Failed to trigger OCR: {response.text}", extra={'request_id': request_id})
                st.error(f"Failed to trigger OCR: {response.text}")
                return None
            
            return receipt_id
            
    except Exception as e:
        logger.error(f"Error storing {file_name}: {str(e)}\n{traceback.format_exc()}", extra={'request_id': request_id})
        st.error(f"Error storing {file_name}: {str(e)}")
        return None

def get_receipt_status(receipt_id):
    request_id = str(uuid.uuid4())
    logger.debug(f"Fetching status for receipt ID: {receipt_id}", extra={'request_id': request_id})
    
    try:
        response = requests.get(f"{API_URL}/results/{receipt_id}")
        if response.status_code == 200:
            return response.json()
        logger.error(f"Failed to fetch status: {response.text}", extra={'request_id': request_id})
        return None
    except Exception as e:
        logger.error(f"Error fetching status: {str(e)}\n{traceback.format_exc()}", extra={'request_id': request_id})
        return None

# Streamlit UI
try:
    st.title("Receipt OCR POC")
    st.write(f"Max file size: {MAX_FILE_SIZE // (1024*1024)}MB | Formats: {', '.join(ALLOWED_EXTENSIONS).upper()}")

    tab1, tab2, tab3 = st.tabs(["Upload Receipt", "Processing Status", "View Results"])

    with tab1:
        uploaded_file = st.file_uploader("Upload a receipt", type=ALLOWED_EXTENSIONS)
        if uploaded_file:
            request_id = str(uuid.uuid4())
            file_name = uploaded_file.name
            file_extension = file_name.lower().split(".")[-1]
            logger.info(f"Processing file: {file_name}", extra={'request_id': request_id})
            
            is_valid, validation_result = validate_file(uploaded_file)
            if not is_valid:
                st.error(validation_result)
            else:
                engine = get_db_engine()
                if not engine:
                    st.error("Database connection failed")
                else:
                    image_bytes = validation_result
                    is_duplicate_name, name_record = check_duplicate_filename(engine, file_name)
                    if is_duplicate_name:
                        st.error(f"File '{file_name}' already exists (uploaded on {name_record[1]}).")
                        st.info("Please rename your file.")
                    else:
                        is_duplicate_content, content_record = check_duplicate_content(engine, image_bytes)
                        if is_duplicate_content:
                            st.warning(f"Identical content exists: {content_record[1]} (ID: {content_record[0]})")
                            if st.button("Upload anyway", key="upload_duplicate"):
                                file_type = "PDF" if file_extension == "pdf" else "image"
                                receipt_id = store_image(engine, image_bytes, file_name, file_type)
                                if receipt_id:
                                    st.success(f"Uploaded '{file_name}' with ID: {receipt_id}")
                                    st.info("OCR processing queued. Check 'Processing Status'.")
                        else:
                            file_type = "PDF" if file_extension == "pdf" else "image"
                            receipt_id = store_image(engine, image_bytes, file_name, file_type)
                            if receipt_id:
                                st.success(f"Uploaded '{file_name}' with ID: {receipt_id}")
                                st.info("OCR processing queued. Check 'Processing Status'.")

    with tab2:
        st.header("Processing Status")
        engine = get_db_engine()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(
                        text("""
                        SELECT r.id, r.file_name, r.file_type, r.created_at, oq.status, oq.priority, oq.updated_at, oq.error_message
                        FROM receipts r
                        LEFT JOIN ocr_queue oq ON r.id = oq.receipt_id
                        ORDER BY r.created_at DESC
                        LIMIT 20
                        """)
                    )
                    records = result.fetchall()
                    if records:
                        for record in records:
                            status_emoji = {
                                'pending': '⏳', 'processing': '🔄', 'completed': '✅', 'failed': '❌', None: '📄'
                            }.get(record[4], '❓')
                            with st.expander(f"{status_emoji} {record[1]} (ID: {record[0]}) - {record[4] or 'Not queued'}"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.write(f"**File Type:** {record[2]}")
                                    st.write(f"**Uploaded:** {record[3]}")
                                    st.write(f"**Status:** {record[4] or 'Not queued'}")
                                with col2:
                                    if record[5]:
                                        st.write(f"**Priority:** {record[5]}")
                                    if record[6]:
                                        st.write(f"**Updated:** {record[6]}")
                                    if record[7]:
                                        st.error(f"**Error:** {record[7]}")
                    else:
                        st.info("No receipts found.")
            except Exception as e:
                logger.error(f"Error fetching status: {str(e)}\n{traceback.format_exc()}")
                st.error(f"Error fetching status: {str(e)}")

    with tab3:
        st.header("OCR Results")
        receipt_id_input = st.number_input("Enter Receipt ID:", min_value=1, step=1)
        if st.button("Load Results") and receipt_id_input:
            status_data = get_receipt_status(receipt_id_input)
            if status_data:
                st.subheader(f"Receipt ID: {receipt_id_input}")
                st.write(f"**Status:** {status_data['status']}")
                if status_data.get('error_message'):
                    st.error(f"**Error:** {status_data['error_message']}")
                if status_data['status'] == 'completed' and status_data['results']:
                    results = status_data['results']
                    st.write(f"**Merchant:** {results.get('merchant_name', 'N/A')}")
                    st.write(f"**Amount:** {results.get('total_amount', 'N/A')}")
                    st.write(f"**Date:** {results.get('receipt_date', 'N/A')}")
                    st.write(f"**Confidence:** {results.get('confidence_score', 'N/A')}%")
                    if results.get('raw_text'):
                        st.text_area("Raw Text", results['raw_text'], height=200)
                elif status_data['status'] == 'failed':
                    st.error("OCR processing failed")
                else:
                    st.info(f"Processing {status_data['status']}")
            else:
                st.error(f"No results for ID {receipt_id_input}")

except Exception as e:
    logger.error(f"Application crashed: {str(e)}\n{traceback.format_exc()}")
    st.error(f"Application error: {str(e)}")
    raise
finally:
    logger.info("Streamlit application shutting down")