import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── PATHS ────────────────────────────────────────────────
PIPELINE_PATH = "/opt/pipeline"
DBT_PROJECT_PATH = "/opt/pipeline/crypto_transforms"
DBT_PROFILES_PATH = "/home/airflow/.dbt"

if PIPELINE_PATH not in sys.path:
    sys.path.insert(0, PIPELINE_PATH)

# ── FAILURE CALLBACK ─────────────────────────────────────
# This function runs whenever any task in the DAG fails.
# context contains: dag_id, task_id, execution_date, exception, log_url
# In production you'd send this to Slack or PagerDuty.
# For now we write a structured log entry that's easy to grep.
def on_task_failure(context):
    import logging
    log = logging.getLogger(__name__)

    dag_id       = context.get("dag").dag_id
    task_id      = context.get("task_instance").task_id
    exec_date    = context.get("data_interval_start")
    exception    = context.get("exception")
    log_url      = context.get("task_instance").log_url

    log.error(
        f"TASK FAILED | dag={dag_id} | task={task_id} | "
        f"date={exec_date} | error={exception} | "
        f"logs={log_url}"
    )

# ── DEFAULT ARGS ────────────────────────────────────────
default_args = {
    "owner": "anindo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Wire the callback — runs on every task failure in this DAG
    "on_failure_callback": on_task_failure,
}

# ── DAG ────────────────────────────────────────────────
with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Crypto pipeline: ingest → dbt → tests",
   schedule="30 3 * * *",  # 9:30 AM UTC+6,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["crypto", "pipeline"],
) as dag:

    # ── STEP 1: CREATE RAW TABLES ───────────────────────
    def ensure_raw_tables(**context):
        from db import get_connection
        import logging

        log = logging.getLogger(__name__)
        conn = get_connection()
        cur = conn.cursor()

        log.info("Ensuring raw tables exist")

        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.coin_prices (
                id SERIAL PRIMARY KEY,
                coin_id VARCHAR(100) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(100) NOT NULL,
                current_price NUMERIC(20,8),
                market_cap BIGINT,
                total_volume BIGINT,
                price_change_pct_24h NUMERIC(10,4),
                fetched_at TIMESTAMP NOT NULL,
                UNIQUE (coin_id, fetched_at)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.global_market (
                id SERIAL PRIMARY KEY,
                total_market_cap_usd NUMERIC(24,2),
                total_volume_usd NUMERIC(24,2),
                btc_dominance_pct NUMERIC(8,4),
                eth_dominance_pct NUMERIC(8,4),
                active_coins INTEGER,
                fetched_at TIMESTAMP NOT NULL,
                UNIQUE (fetched_at)
            );
        """)

        conn.commit()
        cur.close()
        conn.close()

        log.info("Raw tables ready")

    task_ensure_tables = PythonOperator(
        task_id="ensure_raw_tables",
        python_callable=ensure_raw_tables,
    )

    # ── STEP 2: INGEST COIN PRICES ──────────────────────
    def run_coin_prices(**context):
        from ingestion.coin_prices import run
        import logging

        log = logging.getLogger(__name__)
        execution_date = context["data_interval_start"]

        log.info(f"coin_prices: {execution_date}")
        run(execution_date=execution_date)

    task_coin_prices = PythonOperator(
        task_id="ingest_coin_prices",
        python_callable=run_coin_prices,
    )

    # ── STEP 3: INGEST GLOBAL MARKET ────────────────────
    def run_global_market(**context):
        from ingestion.global_market import run
        import logging

        log = logging.getLogger(__name__)
        execution_date = context["data_interval_start"]

        log.info(f"global_market: {execution_date}")
        run(execution_date=execution_date)

    task_global_market = PythonOperator(
        task_id="ingest_global_market",
        python_callable=run_global_market,
    )

    # ── STEP 4: DBT DEPS (SEPARATED) ────────────────────
    task_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        cd {DBT_PROJECT_PATH} &&
        dbt deps --profiles-dir {DBT_PROFILES_PATH}
        """,
    )

    # ── STEP 5: DBT RUN ────────────────────────────────
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_PATH} &&
        dbt run --profiles-dir {DBT_PROFILES_PATH} --no-write-json
        """,
    )

    # ── STEP 6: DBT TEST ───────────────────────────────
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_PATH} &&
        dbt test --profiles-dir {DBT_PROFILES_PATH} --no-write-json
        """,
    )

    # ── DEPENDENCIES ───────────────────────────────────
    task_ensure_tables >> [task_coin_prices, task_global_market]

    [task_coin_prices, task_global_market] >> task_dbt_deps

    task_dbt_deps >> task_dbt_run >> task_dbt_test