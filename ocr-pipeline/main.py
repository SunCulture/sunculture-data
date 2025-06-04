# main.py
import logging
import importlib
import time
import requests
from flask import Flask
from scripts.db_init import init_db
from services.sqs_service import start_sqs_polling
from werkzeug.serving import run_simple

# Configure logging before any imports
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.debug("Logging configured in main.py")

app = Flask(__name__)

# Add a test route directly on the app
@app.route('/test', methods=['GET'])
def test_route():
    logger.info("Serving /test route")
    return "Test route is working!", 200

# Force reload of routes.ocr module
logger.info("Reloading routes.ocr module")
import routes.ocr
importlib.reload(routes.ocr)
from routes.ocr import ocr_bp
logger.info("Imported ocr_bp from routes.ocr")

# Register blueprints with debugging
logger.info("Registering ocr_bp blueprint")
app.register_blueprint(ocr_bp)
logger.info("Successfully registered ocr_bp blueprint")

# Log all registered routes with their full URLs
with app.test_request_context():
    logger.info("Registered routes with full URLs:")
    for rule in app.url_map.iter_rules():
        logger.info(f"Rule: {rule}, Endpoint: {rule.endpoint}, Methods: {rule.methods}")

if __name__ == '__main__':
    logger.info("Starting database initialization")
    init_db()

    # Start Flask app in a separate thread
    from threading import Thread
    def run_flask():
        logger.info("Starting Flask app")
        run_simple('0.0.0.0', 5001, app, use_reloader=False, use_debugger=True)

    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Wait until Flask app is ready by checking the /ocr/health endpoint
    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get('http://localhost:5001/ocr/health')
            if response.status_code == 200:
                logger.info("Flask app is ready, starting SQS polling")
                break
        except requests.ConnectionError:
            logger.warning("Flask app not ready yet, retrying...")
        time.sleep(2)
        retry_count += 1

    if retry_count == max_retries:
        logger.error("Flask app failed to start, exiting")
        exit(1)

    # Start SQS polling
    start_sqs_polling()
    logger.info("SQS polling started successfully.")

    # Keep the main thread alive
    flask_thread.join()