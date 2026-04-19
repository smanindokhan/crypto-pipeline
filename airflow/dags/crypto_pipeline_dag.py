import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── PATHS ─────────────────────────────────────────────────
# All paths are absolute inside the worker container.
# /opt/pipeline is where docker-compose mounts the project root.
# /home/airflow/.dbt is where profiles.yml is mounted.
PIPELINE_PATH     = "/opt/pipeline"
DBT_PROJECT_PATH  = "/opt/pipeline/crypto_transforms"
DBT_PROFILES_PATH = "/home/airflow/.dbt"
# DBT_TARGET read from environment — set to "docker" in docker-compose.yml.
# Falls back to "dev" if somehow not set (safe default).
DBT_TARGET        = os.getenv("DBT_TARGET", "dev")

if PIPELINE_PATH not in sys.path:
    sys.path.insert(0, PIPELINE_PATH)


# ── FAILURE CALLBACK ──────────────────────────────────────
def on_task_failure(context):
    import logging
    log = logging.getLogger(__name__)
    dag_id    = context.get("dag").dag_id
    task_id   = context.get("task_instance").task_id
    exec_date = context.get("data_interval_start")
    exception = context.get("exception")
    log_url   = context.get("task_instance").log_url
    log.error(
        f"PIPELINE FAILURE | dag={dag_id} | task={task_id} | "
        f"date={exec_date} | error={exception} | logs={log_url}"
    )


# ── DEFAULT ARGS ──────────────────────────────────────────
# retries=3: ingestion and dbt tasks each retry up to 3 times.
# retry_delay=timedelta(minutes=5): base wait between retries.
# retry_exponential_backoff=True: retry 1=5min, retry 2=10min,
#   retry 3=20min. Prevents hammering a rate-limited API.
default_args = {
    "owner": "anindo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_task_failure,
}


# ── DAG ───────────────────────────────────────────────────
# schedule="30 3 * * *" = cron for 03:30 UTC every day.
# Cron format: minute hour day-of-month month day-of-week
#   30 = minute 30
#    3 = hour 3 (UTC)
#    * = every day of month
#    * = every month
#    * = every day of week
#
# catchup=False: only run going forward, never backfill
#   missed days automatically.
# max_active_runs=1: never run two instances of this DAG
#   simultaneously. Prevents race conditions on the database.
with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Daily crypto pipeline: ingest → dbt run → dbt test. Runs 03:30 UTC.",
    schedule="30 3 * * *",
    start_date=datetime(2025, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "pipeline"],
) as dag:

    # ── TASK 1: ensure_raw_tables ──────────────────────────
    # Creates raw schema + both tables if they don't exist.
    # Uses IF NOT EXISTS — safe to run every single day.
    # This is your idempotent setup gate before any ingestion.
    def ensure_raw_tables(**context):
        import logging
        log = logging.getLogger(__name__)
        from db import get_connection

        conn = get_connection()
        cur  = conn.cursor()

        log.info("Ensuring raw schema exists")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        log.info("Ensuring raw.coin_prices exists")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.coin_prices (
                id                   SERIAL PRIMARY KEY,
                coin_id              VARCHAR(100)   NOT NULL,
                symbol               VARCHAR(20)    NOT NULL,
                name                 VARCHAR(100)   NOT NULL,
                current_price        NUMERIC(20, 8),
                market_cap           BIGINT,
                total_volume         BIGINT,
                price_change_pct_24h NUMERIC(10, 4),
                fetched_at           TIMESTAMP      NOT NULL,
                UNIQUE (coin_id, fetched_at)
            );
        """)

        log.info("Ensuring raw.global_market exists")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.global_market (
                id                   SERIAL PRIMARY KEY,
                total_market_cap_usd NUMERIC(24, 2),
                total_volume_usd     NUMERIC(24, 2),
                btc_dominance_pct    NUMERIC(8,  4),
                eth_dominance_pct    NUMERIC(8,  4),
                active_coins         INTEGER,
                fetched_at           TIMESTAMP      NOT NULL,
                UNIQUE (fetched_at)
            );
        """)

        conn.commit()
        cur.close()
        conn.close()
        log.info("Raw tables verified successfully")

    task_ensure_tables = PythonOperator(
        task_id="ensure_raw_tables",
        python_callable=ensure_raw_tables,
    )


    # ── TASK 2: ingest_coin_prices ────────────────────────
    # Connects via CRYPTO_DB_HOST (host.docker.internal)
    # → crypto_pipeline database → raw.coin_prices table.
    # Uses data_interval_start as the execution timestamp
    # so each daily run is stamped with its correct date.
    def run_coin_prices(**context):
        import logging
        log = logging.getLogger(__name__)
        from ingestion.coin_prices import run

        execution_date = context["data_interval_start"]
        log.info(f"Ingesting coin_prices for execution_date={execution_date}")
        run(execution_date=execution_date)
        log.info("coin_prices ingestion complete")

    task_coin_prices = PythonOperator(
        task_id="ingest_coin_prices",
        python_callable=run_coin_prices,
        # Override retries for ingestion tasks — API calls need more patience
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
    )


    # ── TASK 3: ingest_global_market ──────────────────────
    # Same pattern as coin_prices — separate task so a failure
    # here doesn't block coin_prices and vice versa.
    def run_global_market(**context):
        import logging
        log = logging.getLogger(__name__)
        from ingestion.global_market import run

        execution_date = context["data_interval_start"]
        log.info(f"Ingesting global_market for execution_date={execution_date}")
        run(execution_date=execution_date)
        log.info("global_market ingestion complete")

    task_global_market = PythonOperator(
        task_id="ingest_global_market",
        python_callable=run_global_market,
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
    )


    # ── TASK 4: dbt_run ───────────────────────────────────
    # BashOperator runs inside the worker container.
    # --target docker: uses the "docker" target in profiles.yml
    #   which connects via host.docker.internal to crypto-db.
    # --project-dir: absolute path to crypto_transforms/ inside container.
    # --profiles-dir: absolute path to .dbt/ inside container.
    # dbt deps runs first to ensure dbt-utils is installed.
    # && chains commands — if deps fails, run never starts.
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt deps "
            f"--profiles-dir {DBT_PROFILES_PATH} && "
            f"dbt run "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH} "
            f"--target {DBT_TARGET} "
            f"--no-write-json"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )


    # ── TASK 5: dbt_test ──────────────────────────────────
    # Runs all schema tests + singular tests.
    # --target docker: same as dbt_run — must use same target
    #   to query the same database that was just populated.
    # If any test fails → task goes red → failure callback fires.
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt test "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH} "
            f"--target {DBT_TARGET} "
            f"--no-write-json"
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )


    # ── TASK 6: check_data_freshness ──────────────────────
    # Runs after dbt_test as a final validation.
    # Queries the database directly to confirm that both
    # raw tables received data in the last 25 hours.
    # This catches the "task ran but wrote nothing" failure
    # that retries and dbt tests can't detect on their own.
    def check_data_freshness(**context):
        import logging
        from datetime import timezone
        log = logging.getLogger(__name__)
        from db import get_connection

        conn = get_connection()
        cur  = conn.cursor()

        execution_date = context["data_interval_start"]
        # Ensure timezone-aware for comparison
        if execution_date.tzinfo is None:
            execution_date = execution_date.replace(tzinfo=timezone.utc)

        log.info(f"Checking data freshness for run date {execution_date}")

        freshness_checks = [
            ("raw.coin_prices",   "SELECT MAX(fetched_at) FROM raw.coin_prices"),
            ("raw.global_market", "SELECT MAX(fetched_at) FROM raw.global_market"),
        ]

        failures = []
        for table_name, query in freshness_checks:
            cur.execute(query)
            result = cur.fetchone()
            latest = result[0] if result else None

            if latest is None:
                failures.append(f"{table_name}: NO DATA AT ALL")
                continue

            # Make latest timezone-aware for comparison
            if latest.tzinfo is None:
                from datetime import timezone as tz
                latest = latest.replace(tzinfo=tz.utc)

            age_hours = (execution_date - latest).total_seconds() / 3600
            log.info(f"{table_name}: latest={latest}, age={age_hours:.1f}h")

            if age_hours > 25:
                failures.append(
                    f"{table_name}: stale — latest row is {age_hours:.1f}h old"
                )

        cur.close()
        conn.close()

        if failures:
            raise ValueError(
                f"Data freshness check FAILED:\n" + "\n".join(failures)
            )

        log.info("All freshness checks passed")

    task_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        retries=0,
        # No retries on freshness check — stale data is stale.
        # Retrying won't make yesterday's missing data appear.
    )


    # ── DEPENDENCY CHAIN ──────────────────────────────────
    # ensure_raw_tables
    #   → ingest_coin_prices  ─┐
    #   → ingest_global_market ┘→ dbt_run → dbt_test → check_data_freshness
    #
    # Both ingestion tasks run in parallel after ensure_raw_tables.
    # dbt_run waits for BOTH to complete successfully.
    # check_data_freshness is the final quality gate.
    task_ensure_tables >> [task_coin_prices, task_global_market]
    [task_coin_prices, task_global_market] >> task_dbt_run
    task_dbt_run >> task_dbt_test >> task_freshness