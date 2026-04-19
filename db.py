import os
import psycopg2
from dotenv import load_dotenv
from logger import get_logger

log = get_logger("db")

# load_dotenv() reads .env if it exists.
# Inside the Airflow worker container there is no .env file —
# credentials are injected as environment variables by Docker Compose.
# load_dotenv() is safe to call even when .env doesn't exist:
# it just does nothing, and os.getenv() still reads from the
# environment variables Docker already set.
# This means the same db.py works in BOTH contexts:
#   - Local dev:  reads from .env file
#   - Airflow:    reads from Docker environment variables
load_dotenv()

def get_connection():
    # Read from environment — works whether set by .env or Docker Compose.
    # The Docker Compose file sets CRYPTO_DB_HOST etc. (with CRYPTO_ prefix)
    # Your .env file sets DB_HOST etc. (without prefix)
    # We check the CRYPTO_ prefixed version first, fall back to unprefixed.
    host     = os.getenv("CRYPTO_DB_HOST")     or os.getenv("DB_HOST")
    port     = os.getenv("CRYPTO_DB_PORT")     or os.getenv("DB_PORT")
    dbname   = os.getenv("CRYPTO_DB_NAME")     or os.getenv("DB_NAME")
    user     = os.getenv("CRYPTO_DB_USER")     or os.getenv("DB_USER")
    password = os.getenv("CRYPTO_DB_PASSWORD") or os.getenv("DB_PASSWORD")

    log.debug(f"Connecting to {host}:{port}/{dbname} as {user}")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    log.debug("Database connection opened")
    return conn