FROM apache/airflow:2.10.5

USER airflow

# Install packages in two separate steps.
# psycopg2-binary, requests, python-dotenv are small and safe.
# dbt-postgres is installed last with --no-deps to avoid
# overwriting Airflow's pinned SQLAlchemy and other shared deps.
# The worker uses dbt via BashOperator (shell command), not
# Python import — so dbt doesn't need to share Airflow's
# Python environment cleanly, it just needs to be on PATH.
RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    python-dotenv

RUN pip install --no-cache-dir \
    dbt-postgres \
    --no-deps

# dbt-postgres --no-deps won't install dbt's own dependencies.
# Install them separately but only what dbt actually needs
# to run as a CLI tool, without touching Airflow's packages.
RUN pip install --no-cache-dir \
    "dbt-core>=1.8,<2.0" \
    "dbt-adapters>=1.0,<2.0" \
    "dbt-postgres>=1.8,<2.0" \
    --upgrade