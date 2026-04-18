import os
import psycopg2
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger("db")

def get_connection():
    """
    Opens and returns a psycopg2 connection.
    Callers are responsible for calling .commit() and .close().

    Centralising this means if your DB credentials ever
    change, you fix it in one place — not in every file.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    log.debug("Database connection opened")
    return conn