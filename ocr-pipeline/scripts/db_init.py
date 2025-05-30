import psycopg2
from config.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import logging

logger = logging.getLogger(__name__)

def init_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS extracted_text (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            extracted_text JSONB,  -- Changed to JSONB for form data
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        cursor.close()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise