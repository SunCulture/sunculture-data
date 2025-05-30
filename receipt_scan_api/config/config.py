from urllib.parse import quote_plus
import os

class Config:
    DEBUG = True
    DB_USER = os.getenv("ep_stage_db_user")
    DB_PASS = quote_plus(os.getenv("ep_stage_db_password") or "")
    DB_HOST = os.getenv("ep_stage_db_host")
    DB_PORT = os.getenv("ep_stage_db_port")
    DB_NAME = os.getenv("ep_stage_db")
    SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False