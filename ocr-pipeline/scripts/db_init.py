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
        
        # Create the table with new columns
        create_table_query = """
        CREATE TABLE IF NOT EXISTS extracted_text (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            extracted_text JSONB,  -- Stores the full JSON response
            has_prohibited_items BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        
        # Add an index on has_prohibited_items for faster querying
        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_has_prohibited_items 
        ON extracted_text (has_prohibited_items);
        """
        cursor.execute(create_index_query)
        
        conn.commit()
        
        cursor.close()
        conn.close()
        logger.info("Database initialized successfully with has_prohibited_items and updated_at columns")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise