import logging
import threading
from flask import Flask
from services.sqs_service import SQSService
from routes.ocr import ocr_bp

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Register blueprint with /ocr prefix to match the expected endpoint
app.register_blueprint(ocr_bp, url_prefix='/ocr')

if __name__ == '__main__':
    try:

        # Start SQS polling in a separate thread
        sqs_service = SQSService()
        sqs_thread = threading.Thread(target=sqs_service.start_sqs_polling, daemon=True)
        sqs_thread.start()
        logger.info("SQS polling started in a separate thread")

        # Start Flask app
        app.run(host='0.0.0.0', port=5001, debug=False)
    except Exception as e:
        logger.error(f"Error starting application: {e}")
        raise