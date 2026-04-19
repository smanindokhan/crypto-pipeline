# Start from the official Airflow 2.10.x image
# Pinning to 2.10 gives us stability without locking to a patch version
FROM apache/airflow:2.10.5

# Switch to root briefly to install system dependencies if needed
# For our project we only need Python packages so we stay as airflow user
USER airflow

# Copy our requirements file into the image
COPY requirements.txt /requirements.txt

# Install our pipeline's Python dependencies into the Airflow image.
# --no-cache-dir keeps the image smaller.
# --constraint pins Airflow's own deps to avoid conflicts.
RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    python-dotenv \
    dbt-postgres