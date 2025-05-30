from sqlalchemy import create_engine, text
import logging
from config.config import Config

logger = logging.getLogger(__name__)

def get_db_engine():
    """Initialize SQLAlchemy engine for PostgreSQL."""
    try:
        engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        return None