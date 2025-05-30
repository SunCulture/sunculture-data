from flask import Flask
from config.config import Config
from routes.ocr_routes import ocr_routes
import logging
import sys
import os
import threading
import time
import requests
from services.db_service import get_db_engine
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/receipt_scan_api.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Ensure log directory exists
os.makedirs("/app/logs", exist_ok=True)

def process_queue():
    """Background thread to process pending OCR jobs."""
    logger.info("Starting queue processor")
    engine = get_db_engine()
    if not engine:
        logger.error("Failed to initialize database engine for queue processor")
        return
    
    while True:
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT receipt_id 
                        FROM ocr_queue 
                        WHERE status = 'pending' 
                        ORDER BY created_at ASC 
                        LIMIT 1
                    """)
                )
                job = result.fetchone()
                if job:
                    receipt_id = job[0]
                    logger.info(f"Processing queued job for receipt ID: {receipt_id}")
                    response = requests.post(f"http://localhost:5000/api/process/{receipt_id}")
                    if response.status_code != 200:
                        logger.error(f"Failed to process receipt {receipt_id}: {response.text}")
        except Exception as e:
            logger.error(f"Queue processing error: {str(e)}")
        time.sleep(10)  # Poll every 10 seconds

app = Flask(__name__)
app.config.from_object(Config)
app.register_blueprint(ocr_routes, url_prefix='/api')

if __name__ == "__main__":
    queue_thread = threading.Thread(target=process_queue, daemon=True)
    queue_thread.start()
    logger.info("ReceiptScanAPI initialized")
    app.run(host="0.0.0.0", port=5000)