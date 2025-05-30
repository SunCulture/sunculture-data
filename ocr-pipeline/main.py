# main.py
from flask import Flask
from routes.ocr import ocr_bp
from scripts.db_init import init_db
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Register blueprints
app.register_blueprint(ocr_bp)

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5001, debug=True)