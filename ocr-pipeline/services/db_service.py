import psycopg2
from psycopg2 import sql
from config.settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import logging

logger = logging.getLogger(__name__)

def save_to_db(file_name, extracted_text):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info(f"Successfully connected to RDS PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        cursor = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO extracted_text (file_name, extracted_text)
            VALUES (%s, %s)
            RETURNING id
        """)
        cursor.execute(insert_query, (file_name, extracted_text))  # extracted_text is a JSON string
        record_id = cursor.fetchone()[0]
        conn.commit()
        
        logger.info(f"Successfully saved form data for {file_name} to database with record ID {record_id}")
        
        cursor.close()
        conn.close()
        return record_id
    except Exception as e:
        logger.error(f"Error saving to database for {file_name}: {e}")
        raise