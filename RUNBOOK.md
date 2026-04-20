# Crypto Pipeline — Complete Build Runbook

A step-by-step guide to rebuilding this pipeline from zero on any Windows machine.
Every command, every file, every known error and fix — in order.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Project structure](#2-project-structure)
3. [Week 1 — Environment setup and first API call](#3-week-1--environment-setup-and-first-api-call)
4. [Week 2 — Ingestion script with idempotency](#4-week-2--ingestion-script-with-idempotency)
5. [Week 3 — Second endpoint, logging, package structure](#5-week-3--second-endpoint-logging-package-structure)
6. [Week 4 — dbt setup and staging models](#6-week-4--dbt-setup-and-staging-models)
7. [Week 5 — Mart models with window functions](#7-week-5--mart-models-with-window-functions)
8. [Week 6 — Tests, documentation, lineage graph](#8-week-6--tests-documentation-lineage-graph)
9. [Week 7 — dbt-utils, surrogate key, backfill parameter](#9-week-7--dbt-utils-surrogate-key-backfill-parameter)
10. [Week 8 — Airflow with Docker Compose](#10-week-8--airflow-with-docker-compose)
11. [Week 9 — Writing the DAG](#11-week-9--writing-the-dag)
12. [Week 10 — Hardening, validation, 3-day verification](#12-week-10--hardening-validation-3-day-verification)
13. [Week 11 — Metabase dashboard](#13-week-11--metabase-dashboard)
14. [Week 13 — Final README polish](#15-week-13--final-readme-polish)
15. [Known errors and fixes](#16-known-errors-and-fixes)
16. [Daily operations reference](#17-daily-operations-reference)
17. [Connection reference](#18-connection-reference)

---

## 1. Prerequisites

Install these before anything else.

| Tool | Download | Notes |
|---|---|---|
| Python 3.11+ | https://www.python.org/downloads/ | Tick "Add Python to PATH" during install |
| Git | https://git-scm.com/download/win | Use all default options |
| Docker Desktop | https://www.docker.com/products/docker-desktop/ | Download AMD64 version. Set memory to 4GB minimum in Settings → Resources |
| VS Code (optional) | https://code.visualstudio.com/ | Recommended editor |

### Enable WSL 2 (required for Docker on Windows)

Open PowerShell as Administrator and run:

```powershell
wsl --install
```

Restart when prompted. After restart a Ubuntu terminal opens — set a username and password.

Verify:
```powershell
wsl --version
```

You should see `WSL version: 2.x.x`. If not, run `wsl --update`.

---

## 2. Project structure

This is what the finished project looks like. Build towards this.

```
crypto-pipeline/
├── ingestion/
│   ├── __init__.py
│   ├── coin_prices.py
│   └── global_market.py
├── crypto_transforms/              ← dbt project
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_coin_prices.sql
│   │   │   └── stg_global_market.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── mart_daily_prices.sql
│   │       ├── mart_top_movers.sql
│   │       └── mart_market_dominance.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── tests/
│       ├── assert_price_is_positive.sql
│       └── assert_dominance_sums_to_100.sql
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   ├── dags/
│   │   └── crypto_pipeline_dag.py
│   ├── logs/                       ← gitignored
│   ├── plugins/
│   └── config/
├── images/
│   ├── lineage.png
│   └── dashboard.png
├── logs/                           ← gitignored
├── venv/                           ← gitignored
├── db.py
├── logger.py
├── main.py
├── requirements.txt
├── .env                            ← gitignored
├── .gitignore
└── README.md
```

---

## 3. Week 1 — Environment setup and first API call

**Goal:** Python environment running, PostgreSQL in Docker, GitHub repo created, CoinGecko API returning data.

### 3.1 Create GitHub repo

Go to github.com → New repository → name `crypto-pipeline` → Public → Add README → Create.

### 3.2 Clone locally

```cmd
cd C:\Users\YourName\Documents
git clone https://github.com/YOUR_USERNAME/crypto-pipeline.git
cd crypto-pipeline
```

### 3.3 Create .gitignore

Create a file named `.gitignore` in the project root with this content:

```
.env
__pycache__/
*.pyc
venv/
logs/
airflow/logs/
crypto_transforms/dbt_packages/
crypto_transforms/target/
```

### 3.4 Start PostgreSQL in Docker

```cmd
docker run --name crypto-db -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=crypto_pipeline -p 5432:5432 -d postgres:15
```

Verify it is running:
```cmd
docker ps --filter "name=crypto-db" --format "table {{.Names}}\t{{.Status}}"
```

Expected: `crypto-db   Up X minutes`

### 3.5 Create Python virtual environment

```cmd
python -m venv venv
venv\Scripts\activate
```

Your prompt now shows `(venv)` at the start.

### 3.6 Create requirements.txt and install

```
requests
psycopg2-binary
pandas
python-dotenv
dbt-postgres
```

```cmd
pip install -r requirements.txt
```

### 3.7 Create .env file

Create `.env` in the project root:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_pipeline
DB_USER=postgres
DB_PASSWORD=postgres
```

### 3.8 Test the CoinGecko API

Create `explore_api.py`:

```python
import requests
import json

BASE_URL = "https://api.coingecko.com/api/v3"
url = f"{BASE_URL}/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1
}

response = requests.get(url, params=params)
print(f"Status code: {response.status_code}")
data = response.json()
print(json.dumps(data[0], indent=2))
for coin in data:
    print(f"{coin['name']:15} ${coin['current_price']:>10,.2f}")
```

```cmd
python explore_api.py
```

Expected: Status code 200, coin names and prices printed.

### 3.9 Commit

```cmd
git add .gitignore requirements.txt explore_api.py
git commit -m "week 1: environment setup and first API call"
git push
```

---

## 4. Week 2 — Ingestion script with idempotency

**Goal:** Data flowing from CoinGecko into PostgreSQL with no duplicate rows.

### 4.1 Create raw schema and tables

```cmd
docker exec -it crypto-db psql -U postgres -d crypto_pipeline
```

Inside psql, run:

```sql
CREATE SCHEMA IF NOT EXISTS raw;

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
```

Type `\q` to exit psql.

### 4.2 Create ingest.py

```python
import os
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BASE_URL  = "https://api.coingecko.com/api/v3"
DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def fetch_coin_prices():
    url    = f"{BASE_URL}/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50, "page": 1}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def parse_coins(raw_data, fetched_at):
    rows = []
    for coin in raw_data:
        rows.append((
            coin.get("id"), coin.get("symbol"), coin.get("name"),
            coin.get("current_price"), coin.get("market_cap"),
            coin.get("total_volume"), coin.get("price_change_percentage_24h"),
            fetched_at,
        ))
    return rows

def insert_rows(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    sql  = """
        INSERT INTO raw.coin_prices
            (coin_id, symbol, name, current_price, market_cap,
             total_volume, price_change_pct_24h, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (coin_id, fetched_at) DO NOTHING
    """
    cur.executemany(sql, rows)
    conn.commit()
    n = cur.rowcount
    cur.close()
    conn.close()
    return n

def main():
    fetched_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    print(f"[{fetched_at}] Fetching...")
    raw_data = fetch_coin_prices()
    rows     = parse_coins(raw_data, fetched_at)
    inserted = insert_rows(rows)
    print(f"  Inserted {inserted} new rows (duplicates skipped)")

if __name__ == "__main__":
    main()
```

```cmd
python ingest.py
```

Run it twice. Second run should show `Inserted 0 new rows` — that is idempotency working.

### 4.3 Commit

```cmd
git add ingest.py
git commit -m "week 2: idempotent ingestion into raw.coin_prices"
git push
```

---

## 5. Week 3 — Second endpoint, logging, package structure

**Goal:** Two data sources, structured logging, clean package layout.

### 5.1 Create folder structure

```cmd
mkdir ingestion
type nul > ingestion\__init__.py
type nul > ingestion\coin_prices.py
type nul > ingestion\global_market.py
type nul > db.py
type nul > logger.py
type nul > main.py
mkdir logs
```

### 5.2 Create raw.global_market table

```cmd
docker exec -it crypto-db psql -U postgres -d crypto_pipeline
```

```sql
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
```

### 5.3 Fill in logger.py

```python
import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    os.makedirs("logs", exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    fh = logging.FileHandler(f"logs/pipeline_{today}.log", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(console)
    logger.addHandler(fh)
    return logger
```

### 5.4 Fill in db.py

```python
import os
import psycopg2
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger("db")

def get_connection():
    host     = os.getenv("CRYPTO_DB_HOST") or os.getenv("DB_HOST")
    port     = os.getenv("CRYPTO_DB_PORT") or os.getenv("DB_PORT")
    dbname   = os.getenv("CRYPTO_DB_NAME") or os.getenv("DB_NAME")
    user     = os.getenv("CRYPTO_DB_USER") or os.getenv("DB_USER")
    password = os.getenv("CRYPTO_DB_PASSWORD") or os.getenv("DB_PASSWORD")
    log.debug(f"Connecting to {host}:{port}/{dbname}")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                            user=user, password=password)
    log.debug("Connection opened")
    return conn
```

### 5.5 Fill in ingestion/coin_prices.py

```python
import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("coin_prices")
BASE_URL = "https://api.coingecko.com/api/v3"

def fetch():
    url    = f"{BASE_URL}/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50, "page": 1}
    log.info("Fetching coin prices")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    log.info(f"Fetched {len(data)} coins")
    return data

def parse(raw_data, fetched_at):
    rows = []
    for coin in raw_data:
        rows.append((
            coin.get("id"), coin.get("symbol"), coin.get("name"),
            coin.get("current_price"), coin.get("market_cap"),
            coin.get("total_volume"), coin.get("price_change_percentage_24h"),
            fetched_at,
        ))
    return rows

def insert(rows):
    conn = get_connection()
    cur  = conn.cursor()
    sql  = """
        INSERT INTO raw.coin_prices
            (coin_id, symbol, name, current_price, market_cap,
             total_volume, price_change_pct_24h, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (coin_id, fetched_at) DO NOTHING
    """
    cur.executemany(sql, rows)
    conn.commit()
    n = cur.rowcount
    cur.close()
    conn.close()
    if n == 0:
        log.info("No new rows — idempotent re-run")
    else:
        log.info(f"Inserted {n} rows into raw.coin_prices")
    return n

def run(execution_date: datetime = None):
    if execution_date is None:
        fetched_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    else:
        fetched_at = execution_date.replace(tzinfo=timezone.utc, second=0, microsecond=0) \
                     if execution_date.tzinfo is None \
                     else execution_date.replace(second=0, microsecond=0)
    log.info(f"Running coin_prices for {fetched_at}")
    data = fetch()
    rows = parse(data, fetched_at)
    insert(rows)
```

### 5.6 Fill in ingestion/global_market.py

```python
import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("global_market")
BASE_URL = "https://api.coingecko.com/api/v3"

def fetch():
    log.info("Fetching global market data")
    response = requests.get(f"{BASE_URL}/global", timeout=10)
    response.raise_for_status()
    data = response.json().get("data", {})
    log.info("Global data fetched")
    return data

def parse(raw, fetched_at):
    market_cap = raw.get("total_market_cap", {}).get("usd")
    volume     = raw.get("total_volume",     {}).get("usd")
    dominance  = raw.get("market_cap_percentage", {})
    row = (market_cap, volume, dominance.get("btc"),
           dominance.get("eth"), raw.get("active_cryptocurrencies"), fetched_at)
    return row

def insert(row):
    conn = get_connection()
    cur  = conn.cursor()
    sql  = """
        INSERT INTO raw.global_market
            (total_market_cap_usd, total_volume_usd, btc_dominance_pct,
             eth_dominance_pct, active_coins, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fetched_at) DO NOTHING
    """
    cur.execute(sql, row)
    conn.commit()
    n = cur.rowcount
    cur.close()
    conn.close()
    log.info("Inserted 1 row" if n else "Duplicate — skipped")
    return n

def run(execution_date: datetime = None):
    if execution_date is None:
        fetched_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    else:
        fetched_at = execution_date.replace(tzinfo=timezone.utc, second=0, microsecond=0) \
                     if execution_date.tzinfo is None \
                     else execution_date.replace(second=0, microsecond=0)
    log.info(f"Running global_market for {fetched_at}")
    data = fetch()
    row  = parse(data, fetched_at)
    insert(row)
```

### 5.7 Fill in main.py

```python
import sys
from datetime import datetime, timezone
from logger import get_logger
from ingestion import coin_prices, global_market

log = get_logger("main")

def main():
    execution_date = None
    if len(sys.argv) > 1:
        try:
            execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            log.info(f"Backfill mode: {sys.argv[1]}")
        except ValueError:
            log.error(f"Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    log.info("=" * 50)
    log.info("Pipeline run starting")
    errors = []
    for name, module in [("coin_prices", coin_prices), ("global_market", global_market)]:
        try:
            log.info(f"--- Starting {name} ---")
            module.run(execution_date=execution_date)
            log.info(f"--- {name} complete ---")
        except Exception as e:
            log.error(f"{name} FAILED: {e}", exc_info=True)
            errors.append(name)

    if errors:
        log.error(f"Failures: {errors}")
        sys.exit(1)
    else:
        log.info("Pipeline complete")

if __name__ == "__main__":
    main()
```

Run the full pipeline:
```cmd
python main.py
```

```cmd
git add ingestion\ db.py logger.py main.py
git commit -m "week 3: two-source pipeline with logging"
git push
```

---

## 6. Week 4 — dbt setup and staging models

**Goal:** dbt project initialised, connected to PostgreSQL, two staging views created and tested.

### 6.1 Install dbt-postgres

```cmd
venv\Scripts\activate
pip install dbt-postgres
```

### 6.2 Initialise dbt project

```cmd
dbt init crypto_transforms
```

When asked for database type, choose postgres.

### 6.3 Create profiles.yml

Location: `C:\Users\YourName\.dbt\profiles.yml`

```yaml
crypto_transforms:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      dbname: crypto_pipeline
      user: postgres
      password: postgres
      schema: public
      threads: 1

    docker:
      type: postgres
      host: host.docker.internal
      port: 5432
      dbname: crypto_pipeline
      user: postgres
      password: postgres
      schema: public
      threads: 1
```

> **Two targets are required.**
> - `dev` — for running dbt from your terminal (localhost works)
> - `docker` — for running dbt inside Airflow containers (must use host.docker.internal)

### 6.4 Test connection

```cmd
cd crypto_transforms
dbt debug --target dev
```

Expected: `Connection: OK — All checks passed!`

### 6.5 Replace dbt_project.yml

```yaml
name: crypto_transforms
version: "1.0.0"
config-version: 2

profile: crypto_transforms

model-paths: ["models"]
test-paths:  ["tests"]
seed-paths:  ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  crypto_transforms:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

### 6.6 Create generate_schema_name macro

**This macro is critical.** Without it dbt writes to `public_staging` instead of `staging`.

Create `crypto_transforms/macros/generate_schema_name.sql`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### 6.7 Create sources.yml

Create `crypto_transforms/models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: raw
    description: "Raw tables written by the Python ingestion pipeline"
    schema: raw
    loaded_at_field: fetched_at
    freshness:
      warn_after:  {count: 25, period: hour}
      error_after: {count: 48, period: hour}
    tables:
      - name: coin_prices
        description: "Top 50 coins by market cap, fetched daily"
      - name: global_market
        description: "Global crypto market snapshot"
```

### 6.8 Create stg_coin_prices.sql

Create `crypto_transforms/models/staging/stg_coin_prices.sql`:

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'coin_prices') }}
),
renamed AS (
    SELECT
        coin_id,
        symbol,
        name                          AS coin_name,
        current_price::FLOAT          AS price_usd,
        market_cap                    AS market_cap_usd,
        total_volume                  AS volume_usd,
        price_change_pct_24h,
        fetched_at::DATE              AS price_date,
        fetched_at
    FROM source
),
cleaned AS (
    SELECT * FROM renamed WHERE price_usd IS NOT NULL
)
SELECT * FROM cleaned
```

### 6.9 Create stg_global_market.sql

Create `crypto_transforms/models/staging/stg_global_market.sql`:

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'global_market') }}
),
renamed AS (
    SELECT
        total_market_cap_usd::FLOAT         AS total_market_cap_usd,
        total_volume_usd::FLOAT             AS total_volume_usd,
        btc_dominance_pct::FLOAT            AS btc_dominance_pct,
        eth_dominance_pct::FLOAT            AS eth_dominance_pct,
        active_coins,
        ROUND((btc_dominance_pct + eth_dominance_pct)::NUMERIC, 2) AS btc_eth_combined_pct,
        fetched_at::DATE                    AS market_date,
        fetched_at
    FROM source
    WHERE total_market_cap_usd IS NOT NULL
)
SELECT * FROM renamed
```

### 6.10 Create staging schema.yml

Create `crypto_transforms/models/staging/schema.yml`:

```yaml
version: 2

models:
  - name: stg_coin_prices
    description: "Cleaned coin price snapshots"
    columns:
      - name: coin_id
        tests: [not_null]
      - name: price_usd
        tests: [not_null]
      - name: price_date
        tests: [not_null]
      - name: fetched_at
        tests: [not_null]

  - name: stg_global_market
    description: "Cleaned global market snapshots"
    columns:
      - name: total_market_cap_usd
        tests: [not_null]
      - name: btc_dominance_pct
        tests: [not_null]
      - name: fetched_at
        tests:
          - not_null
          - unique
```

### 6.11 Run dbt

```cmd
dbt run
dbt test
```

Expected schema names in output: `staging.stg_coin_prices`, `staging.stg_global_market`.
If you see `public_staging` — the macro is not saved correctly. Check the file content.

```cmd
cd ..
git add crypto_transforms\ requirements.txt
git commit -m "week 4: dbt staging models and tests"
git push
```

---

## 7. Week 5 — Mart models with window functions

**Goal:** Three mart tables built from staging using window functions.

Create `crypto_transforms/models/marts/` folder.

### mart_daily_prices.sql

```sql
WITH daily_agg AS (
    SELECT
        coin_id, coin_name, symbol, price_date,
        ROUND(AVG(price_usd)::NUMERIC,       2)  AS avg_price_usd,
        ROUND(MAX(price_usd)::NUMERIC,       2)  AS high_price_usd,
        ROUND(MIN(price_usd)::NUMERIC,       2)  AS low_price_usd,
        ROUND(AVG(price_change_pct_24h)::NUMERIC, 4) AS avg_price_change_pct_24h,
        ROUND(AVG(volume_usd)::NUMERIC,      0)  AS avg_volume_usd,
        ROUND(AVG(market_cap_usd)::NUMERIC,  0)  AS avg_market_cap_usd
    FROM {{ ref('stg_coin_prices') }}
    GROUP BY coin_id, coin_name, symbol, price_date
),
with_windows AS (
    SELECT
        coin_id, coin_name, symbol, price_date,
        avg_price_usd, high_price_usd, low_price_usd,
        avg_price_change_pct_24h, avg_volume_usd, avg_market_cap_usd,
        LAG(avg_price_usd, 1) OVER (PARTITION BY coin_id ORDER BY price_date)
            AS prev_day_price_usd,
        ROUND(
            ((avg_price_usd - LAG(avg_price_usd,1) OVER (PARTITION BY coin_id ORDER BY price_date))
            / NULLIF(LAG(avg_price_usd,1) OVER (PARTITION BY coin_id ORDER BY price_date),0) * 100
            )::NUMERIC, 4)   AS day_over_day_pct_change,
        ROUND(AVG(avg_price_usd) OVER (
            PARTITION BY coin_id ORDER BY price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::NUMERIC, 2)
            AS rolling_7d_avg_price,
        RANK() OVER (PARTITION BY price_date ORDER BY avg_market_cap_usd DESC)
            AS daily_market_cap_rank
    FROM daily_agg
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['coin_id', 'price_date']) }} AS daily_price_id,
        coin_id, coin_name, symbol, price_date,
        avg_price_usd, high_price_usd, low_price_usd,
        avg_price_change_pct_24h, avg_volume_usd, avg_market_cap_usd,
        prev_day_price_usd, day_over_day_pct_change,
        rolling_7d_avg_price, daily_market_cap_rank
    FROM with_windows
)
SELECT * FROM final
ORDER BY price_date DESC, daily_market_cap_rank ASC
```

### mart_top_movers.sql

```sql
WITH ranked AS (
    SELECT
        price_date, coin_id, coin_name, symbol,
        avg_price_usd, day_over_day_pct_change, daily_market_cap_rank,
        RANK() OVER (PARTITION BY price_date ORDER BY day_over_day_pct_change DESC) AS gain_rank,
        RANK() OVER (PARTITION BY price_date ORDER BY day_over_day_pct_change ASC)  AS loss_rank
    FROM {{ ref('mart_daily_prices') }}
    WHERE day_over_day_pct_change IS NOT NULL
),
top_movers AS (
    SELECT
        price_date, coin_id, coin_name, symbol,
        avg_price_usd, day_over_day_pct_change,
        CASE
            WHEN gain_rank <= 5 AND loss_rank <= 5 THEN 'both'
            WHEN gain_rank <= 5 THEN 'top_gainer'
            WHEN loss_rank <= 5 THEN 'top_loser'
        END AS mover_type,
        LEAST(gain_rank, loss_rank) AS rank_position
    FROM ranked
    WHERE gain_rank <= 5 OR loss_rank <= 5
)
SELECT * FROM top_movers
ORDER BY price_date DESC, mover_type, rank_position
```

### mart_market_dominance.sql

> **Important:** This model must aggregate by day (GROUP BY market_date) to avoid
> duplicate rows when the pipeline runs multiple times per day. Without this the
> unique test on market_date will fail.

```sql
WITH global_agg AS (
    SELECT
        market_date,
        AVG(total_market_cap_usd)  AS total_market_cap_usd,
        AVG(total_volume_usd)      AS total_volume_usd,
        AVG(btc_dominance_pct)     AS btc_dominance_pct,
        AVG(eth_dominance_pct)     AS eth_dominance_pct,
        AVG(btc_eth_combined_pct)  AS btc_eth_combined_pct,
        AVG(active_coins)          AS active_coins
    FROM {{ ref('stg_global_market') }}
    GROUP BY market_date
),
global AS (
    SELECT
        market_date,
        ROUND(total_market_cap_usd::NUMERIC, 2)  AS total_market_cap_usd,
        ROUND(total_volume_usd::NUMERIC, 2)       AS total_volume_usd,
        ROUND(btc_dominance_pct::NUMERIC, 4)      AS btc_dominance_pct,
        ROUND(eth_dominance_pct::NUMERIC, 4)      AS eth_dominance_pct,
        ROUND(btc_eth_combined_pct::NUMERIC, 2)   AS btc_eth_combined_pct,
        ROUND(active_coins::NUMERIC, 0)::INTEGER  AS active_coins,
        ROUND(AVG(btc_dominance_pct) OVER (
            ORDER BY market_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS btc_dominance_7d_avg,
        ROUND(
            ((total_market_cap_usd - LAG(total_market_cap_usd,1) OVER (ORDER BY market_date))
            / NULLIF(LAG(total_market_cap_usd,1) OVER (ORDER BY market_date),0) * 100
            )::NUMERIC, 4) AS market_cap_day_change_pct
    FROM global_agg
),
btc_prices AS (
    SELECT price_date, avg_price_usd AS btc_price_usd
    FROM {{ ref('mart_daily_prices') }} WHERE coin_id = 'bitcoin'
),
eth_prices AS (
    SELECT price_date, avg_price_usd AS eth_price_usd
    FROM {{ ref('mart_daily_prices') }} WHERE coin_id = 'ethereum'
)
SELECT
    g.market_date, g.total_market_cap_usd, g.btc_dominance_pct,
    g.eth_dominance_pct, g.btc_eth_combined_pct, g.btc_dominance_7d_avg,
    g.market_cap_day_change_pct, g.active_coins,
    b.btc_price_usd, e.eth_price_usd
FROM global g
LEFT JOIN btc_prices b ON g.market_date = b.price_date
LEFT JOIN eth_prices e ON g.market_date = e.price_date
ORDER BY g.market_date DESC
```

### Create marts/schema.yml

```yaml
version: 2

models:
  - name: mart_daily_prices
    description: "One row per coin per day with rolling averages and day-over-day change"
    columns:
      - name: daily_price_id
        description: "Surrogate key: MD5 hash of coin_id + price_date"
        tests: [not_null, unique]
      - name: coin_id
        tests: [not_null]
      - name: price_date
        tests: [not_null]
      - name: avg_price_usd
        tests: [not_null]

  - name: mart_top_movers
    description: "Top 5 gainers and losers per day"
    columns:
      - name: mover_type
        tests:
          - not_null
          - accepted_values:
              values: ["top_gainer", "top_loser", "both"]
      - name: price_date
        tests: [not_null]

  - name: mart_market_dominance
    description: "Daily BTC/ETH dominance trends joined with coin prices"
    columns:
      - name: market_date
        tests: [not_null, unique]
      - name: btc_dominance_pct
        tests: [not_null]
```

### Install dbt-utils and run

Create `crypto_transforms/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

```cmd
cd crypto_transforms
dbt deps
dbt run
dbt test
cd ..
```

> **If mart_top_movers has 0 rows:** You need at least 2 days of data for
> day-over-day change to exist. Run `python main.py` on a second day and retry.

```cmd
git add crypto_transforms\
git commit -m "week 5: mart models with window functions and surrogate key"
git push
```

---

## 8. Week 6 — Tests, documentation, lineage graph

**Goal:** Custom singular tests, source freshness, full column documentation, lineage screenshot.

### 8.1 Create singular tests

Create `crypto_transforms/tests/assert_price_is_positive.sql`:

```sql
-- Returns bad rows. Zero rows = test passes.
SELECT coin_id, coin_name, price_date, avg_price_usd
FROM {{ ref('mart_daily_prices') }}
WHERE avg_price_usd <= 0
```

Create `crypto_transforms/tests/assert_dominance_sums_to_100.sql`:

```sql
SELECT market_date, btc_dominance_pct, eth_dominance_pct, btc_eth_combined_pct
FROM {{ ref('mart_market_dominance') }}
WHERE btc_eth_combined_pct > 100.5
```

### 8.2 Run tests

```cmd
cd crypto_transforms
dbt test --select "test_type:singular"
```

### 8.3 Check source freshness

```cmd
dbt source freshness
```

### 8.4 Generate and view lineage graph

```cmd
dbt docs generate
dbt docs serve --port 8080
```

Open http://localhost:8080, navigate to any model, click lineage graph in bottom-right.
Screenshot the graph with Win+Shift+S. Save as `images/lineage.png`.

Stop the server: Ctrl+C

```cmd
cd ..
git add crypto_transforms\ images\
git commit -m "week 6: singular tests, source freshness, lineage graph"
git push
```

---

## 9. Week 7 — dbt-utils, surrogate key, backfill parameter

> The surrogate key is already in mart_daily_prices from Week 5.
> The execution_date parameter is already in ingestion modules from Week 3.
> Week 7's main value is the fresh-state test below.

### 9.1 Fresh-state test — destroy and rebuild everything

```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS staging CASCADE; DROP SCHEMA IF EXISTS marts CASCADE;"
```

```cmd
cd crypto_transforms
dbt clean
dbt deps
cd ..
```

```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "CREATE SCHEMA IF NOT EXISTS raw;"
```

Recreate raw tables (run the SQL from Week 2 section 2.1 and section 5.2).

```cmd
python main.py
cd crypto_transforms
dbt run && dbt test
cd ..
```

If all 5 models build and all tests pass — your pipeline is reproducible from scratch.

```cmd
git add crypto_transforms\ ingestion\ main.py
git commit -m "week 7: fresh-state verified, backfill mode tested"
git push
```

---

## 10. Week 8 — Airflow with Docker Compose

**Goal:** Airflow running on http://localhost:8081 with 3 healthy containers (LocalExecutor).

### 10.1 Create folder structure

```cmd
mkdir airflow\dags airflow\logs airflow\plugins airflow\config
```

### 10.2 Create airflow/Dockerfile

```dockerfile
FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    python-dotenv

RUN pip install --no-cache-dir \
    "dbt-core>=1.8,<2.0" \
    "dbt-adapters>=1.0,<2.0" \
    "dbt-postgres>=1.8,<2.0" \
    --upgrade
```

### 10.3 Create airflow/requirements.txt

```
psycopg2-binary
requests
python-dotenv
dbt-postgres
```

### 10.4 Create airflow/docker-compose.yml

```yaml
x-airflow-common: &airflow-common
  build: .
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: '30'
    AIRFLOW__CORE__FERNET_KEY: 'ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg='
    AIRFLOW__WEBSERVER__SECRET_KEY: 'crypto-pipeline-secret-key-change-in-prod'
    CRYPTO_DB_HOST: host.docker.internal
    CRYPTO_DB_PORT: '5432'
    CRYPTO_DB_NAME: crypto_pipeline
    CRYPTO_DB_USER: postgres
    CRYPTO_DB_PASSWORD: postgres
    DBT_TARGET: docker
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
    - ..:/opt/pipeline
    - ${USERPROFILE}/.dbt:/home/airflow/.dbt:ro
  depends_on:
    airflow-postgres:
      condition: service_healthy

services:

  airflow-postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8081:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username airflow \
          --password airflow \
          --firstname Airflow \
          --lastname Admin \
          --role Admin \
          --email airflow@example.com
    restart: on-failure

volumes:
  airflow-postgres-data:
```

> **Why LocalExecutor?** CeleryExecutor requires Redis and a separate worker
> container. On Airflow 2.10.5 + Python 3.12, the Celery worker crashes with
> `AttributeError: 'NoneType' object has no attribute 'split'` due to a
> hostname resolution bug. LocalExecutor runs tasks as subprocesses in the
> scheduler — correct for single-machine use, no Redis needed.

### 10.5 Build and start

```cmd
cd airflow
docker compose config
docker compose build
docker compose up airflow-init
docker compose up -d
timeout /t 60 /nobreak
docker compose ps
```

Expected: 3 containers all showing healthy.
- airflow-airflow-postgres-1
- airflow-airflow-webserver-1
- airflow-airflow-scheduler-1

### 10.6 Verify DB connection from scheduler

```cmd
docker compose exec airflow-scheduler python -c "import psycopg2; c=psycopg2.connect(host='host.docker.internal',port=5432,dbname='crypto_pipeline',user='postgres',password='postgres'); print('CRYPTO-DB OK'); c.close()"
```

Expected: `CRYPTO-DB OK`

```cmd
cd ..
git add airflow\Dockerfile airflow\docker-compose.yml airflow\requirements.txt
git commit -m "week 8: airflow LocalExecutor setup"
git push
```

---

## 11. Week 9 — Writing the DAG

**Goal:** A single DAG file that chains ingestion → dbt run → dbt test → freshness check, running at 03:30 UTC.

### Create airflow/dags/crypto_pipeline_dag.py

```python
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PIPELINE_PATH     = "/opt/pipeline"
DBT_PROJECT_PATH  = "/opt/pipeline/crypto_transforms"
DBT_PROFILES_PATH = "/home/airflow/.dbt"
DBT_TARGET        = os.getenv("DBT_TARGET", "dev")

if PIPELINE_PATH not in sys.path:
    sys.path.insert(0, PIPELINE_PATH)


def on_task_failure(context):
    import logging
    log = logging.getLogger(__name__)
    log.error(
        f"PIPELINE FAILURE | dag={context.get('dag').dag_id} | "
        f"task={context.get('task_instance').task_id} | "
        f"date={context.get('data_interval_start')} | "
        f"error={context.get('exception')}"
    )


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


with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Daily crypto pipeline. Runs 03:30 UTC.",
    schedule="30 3 * * *",
    start_date=datetime(2025, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "pipeline"],
) as dag:

    def ensure_raw_tables(**context):
        import logging
        log = logging.getLogger(__name__)
        from db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.coin_prices (
                id SERIAL PRIMARY KEY, coin_id VARCHAR(100) NOT NULL,
                symbol VARCHAR(20) NOT NULL, name VARCHAR(100) NOT NULL,
                current_price NUMERIC(20,8), market_cap BIGINT,
                total_volume BIGINT, price_change_pct_24h NUMERIC(10,4),
                fetched_at TIMESTAMP NOT NULL, UNIQUE (coin_id, fetched_at)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.global_market (
                id SERIAL PRIMARY KEY, total_market_cap_usd NUMERIC(24,2),
                total_volume_usd NUMERIC(24,2), btc_dominance_pct NUMERIC(8,4),
                eth_dominance_pct NUMERIC(8,4), active_coins INTEGER,
                fetched_at TIMESTAMP NOT NULL, UNIQUE (fetched_at)
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        log.info("Raw tables verified")

    task_ensure_tables = PythonOperator(
        task_id="ensure_raw_tables",
        python_callable=ensure_raw_tables,
    )

    def run_coin_prices(**context):
        import logging
        log = logging.getLogger(__name__)
        from ingestion.coin_prices import run
        execution_date = context["data_interval_start"]
        log.info(f"Running coin_prices for {execution_date}")
        run(execution_date=execution_date)

    task_coin_prices = PythonOperator(
        task_id="ingest_coin_prices",
        python_callable=run_coin_prices,
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
    )

    def run_global_market(**context):
        import logging
        log = logging.getLogger(__name__)
        from ingestion.global_market import run
        execution_date = context["data_interval_start"]
        log.info(f"Running global_market for {execution_date}")
        run(execution_date=execution_date)

    task_global_market = PythonOperator(
        task_id="ingest_global_market",
        python_callable=run_global_market,
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_PATH} && "
            f"dbt run --project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH} --target {DBT_TARGET} --no-write-json"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt test --project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH} --target {DBT_TARGET} --no-write-json"
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    def check_data_freshness(**context):
        import logging
        from datetime import timezone
        log = logging.getLogger(__name__)
        from db import get_connection
        conn = get_connection()
        cur  = conn.cursor()
        execution_date = context["data_interval_start"]
        if execution_date.tzinfo is None:
            execution_date = execution_date.replace(tzinfo=timezone.utc)
        failures = []
        for table, query in [
            ("raw.coin_prices",   "SELECT MAX(fetched_at) FROM raw.coin_prices"),
            ("raw.global_market", "SELECT MAX(fetched_at) FROM raw.global_market"),
        ]:
            cur.execute(query)
            latest = cur.fetchone()[0]
            if latest is None:
                failures.append(f"{table}: NO DATA")
                continue
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            age_hours = (execution_date - latest).total_seconds() / 3600
            log.info(f"{table}: latest={latest}, age={age_hours:.1f}h")
            if age_hours > 25:
                failures.append(f"{table}: stale — {age_hours:.1f}h old")
        cur.close()
        conn.close()
        if failures:
            raise ValueError("Freshness FAILED:\n" + "\n".join(failures))
        log.info("All freshness checks passed")

    task_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        retries=0,
    )

    task_ensure_tables >> [task_coin_prices, task_global_market]
    [task_coin_prices, task_global_market] >> task_dbt_run
    task_dbt_run >> task_dbt_test >> task_freshness
```

```cmd
git add airflow\dags\
git commit -m "week 9: crypto_pipeline DAG at 03:30 UTC"
git push
```

---

## 12. Week 10 — Hardening, validation, 3-day verification

### Validation checklist — run in order

**Check 1** — crypto-db running:
```cmd
docker ps --filter "name=crypto-db" --format "table {{.Names}}\t{{.Status}}"
```

**Check 2** — raw tables exist:
```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "\dn" -c "\dt raw.*"
```

**Check 3** — data in raw tables:
```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "SELECT 'coin_prices' AS tbl, COUNT(*) FROM raw.coin_prices UNION ALL SELECT 'global_market', COUNT(*) FROM raw.global_market;"
```

**Check 4** — dbt connection locally:
```cmd
cd crypto_transforms
venv\Scripts\activate
dbt debug --target dev
cd ..
```

**Check 5** — dbt run locally, schema names correct:
```cmd
cd crypto_transforms
dbt run --target dev
```
Must show `staging.stg_*` and `marts.mart_*` — NOT `public_staging` or `public_marts`.

**Check 6** — DB connection from scheduler container:
```cmd
cd airflow
docker compose exec airflow-scheduler python -c "import psycopg2; c=psycopg2.connect(host='host.docker.internal',port=5432,dbname='crypto_pipeline',user='postgres',password='postgres'); print('CRYPTO-DB OK'); c.close()"
```

**Check 7** — dbt connection from inside scheduler:
```cmd
docker compose exec airflow-scheduler bash -c "cd /opt/pipeline/crypto_transforms && dbt debug --target docker --profiles-dir /home/airflow/.dbt"
```

**Check 8** — schema names correct from inside scheduler:
```cmd
docker compose exec airflow-scheduler bash -c "cd /opt/pipeline/crypto_transforms && dbt run --target docker --profiles-dir /home/airflow/.dbt --no-write-json"
```

**Check 9** — all dbt tests pass from inside scheduler:
```cmd
docker compose exec airflow-scheduler bash -c "cd /opt/pipeline/crypto_transforms && dbt test --target docker --profiles-dir /home/airflow/.dbt --no-write-json"
```

### Trigger DAG and verify

1. Open http://localhost:8081
2. Unpause `crypto_pipeline`
3. Click ▷ Trigger DAG
4. Watch all 6 tasks go green in Graph view

Verify data after the run:
```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "SELECT 'raw.coin_prices' AS layer, COUNT(*) AS rows, MAX(fetched_at) AS latest FROM raw.coin_prices UNION ALL SELECT 'raw.global_market', COUNT(*), MAX(fetched_at) FROM raw.global_market UNION ALL SELECT 'marts.mart_daily_prices', COUNT(*), NULL FROM marts.mart_daily_prices UNION ALL SELECT 'marts.mart_top_movers', COUNT(*), NULL FROM marts.mart_top_movers;"
```

### 3-day verification (after leaving pipeline running)

```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT fetched_at::DATE) AS distinct_days, COUNT(DISTINCT coin_id) AS distinct_coins FROM raw.coin_prices;"
```

Expected: `total_rows=150, distinct_days=3, distinct_coins=50`

---

## 13. Week 11 — Metabase dashboard

```cmd
docker run -d --name metabase -p 3000:3000 metabase/metabase
```

Open http://localhost:3000. Complete setup wizard.

Database connection settings:
- Type: PostgreSQL
- Host: `host.docker.internal`
- Port: 5432
- Database: `crypto_pipeline`
- Username: `postgres`
- Password: `postgres`

Build 4 charts:

| Chart | Table | X-axis | Y-axis | Type |
|---|---|---|---|---|
| BTC price trend | marts.mart_daily_prices | price_date | avg_price_usd, rolling_7d_avg_price | Line |
| Top gainers | marts.mart_top_movers | coin_name | day_over_day_pct_change | Bar |
| BTC dominance | marts.mart_market_dominance | market_date | btc_dominance_pct | Line |
| Market cap | marts.mart_market_dominance | market_date | total_market_cap_usd | Area |

Group into a Dashboard. Screenshot → save as `images/dashboard.png`.

---

## 14. Week 12 — Final README polish

Add `images/dashboard.png` to git, update README.md with final architecture table, dashboard image, key engineering decisions section, and full How-to-run instructions.

```cmd
git add README.md images\dashboard.png
git commit -m "week 13: final README and dashboard screenshot"
git push
```

---

## 15. Known errors and fixes

### Error: YAML cycle detected in docker-compose.yml

```
yaml: construct errors: line 1: cycle detected: node at path
services.airflow-worker.environment references node at path services.airflow-worker
```

**Cause:** The `airflow-worker` environment block used `<<: *airflow-common` which
references the whole service block — including the worker itself.

**Fix:** Define a second anchor `&airflow-common-env` on the environment block
and use that in the worker. See `airflow/docker-compose.yml` in Week 8.

---

### Error: Celery worker hostname None

```
AttributeError: 'NoneType' object has no attribute 'split'
in celery/utils/nodenames.py
```

**Cause:** Airflow 2.10.5 + Python 3.12 Celery provider bug where the worker
pidfile hostname resolves as None. Affects CeleryExecutor only.

**Fix:** Switch to LocalExecutor. Remove Redis and worker service from
docker-compose.yml entirely. Set `AIRFLOW__CORE__EXECUTOR: LocalExecutor`.
LocalExecutor is correct for single-machine use anyway.

---

### Error: dbt writes to public_staging instead of staging

**Cause:** dbt appends the profile schema to the model's custom schema by default.
`profile schema = public` + `model schema = staging` = `public_staging`.

**Fix:** Add `generate_schema_name` macro to `crypto_transforms/macros/` that
returns `custom_schema_name` directly when set. See Week 6 section 6.6.

---

### Error: unique test fails on mart_market_dominance.market_date

```
Failure in test unique_mart_market_dominance_market_date
Got 1 result, configured to fail if != 0
```

**Cause:** The pipeline ran multiple times on the same day. The original
`mart_market_dominance` model had no daily aggregation — multiple rows per
day passed through from `stg_global_market`.

**Fix:** Add a `global_agg` CTE with `GROUP BY market_date` at the start of
`mart_market_dominance.sql`. See Week 5 mart model in this runbook — it
already includes the fix.

---

### Error: dbt debug shows git ERROR

```
Error from git --help: User does not have permissions for this command: "git"
```

**This is not a real error.** dbt checks for git as part of `dbt debug`
but only uses it for package management. Your pipeline uses pip-installed
packages, not git-sourced ones. Ignore this. The connection test below it
shows OK which is all that matters.

---

### Error: connection refused when connecting to crypto-db

**Cause:** The crypto-db container is not running.

**Fix:**
```cmd
docker start crypto-db
```

This happens after restarting Docker Desktop or your PC. Add it to your
startup routine when working on the pipeline.

---

### Error: dbt profiles not found inside Airflow container

```
Could not find profile named 'crypto_transforms'
```

**Cause:** The `.dbt` volume mount failed. Either `USERPROFILE` env var
is not expanding or the path is wrong.

**Fix:** Check the volume line in docker-compose.yml:
```
- ${USERPROFILE}/.dbt:/home/airflow/.dbt:ro
```

Verify it expands correctly:
```cmd
echo %USERPROFILE%
```

Should print `C:\Users\YourName`. Then confirm profiles.yml exists:
```cmd
dir %USERPROFILE%\.dbt\
```

---

### Error: mart_top_movers is empty

**Cause:** You only have 1 day of data. Day-over-day change requires at
least 2 consecutive days.

**Fix:** Run `python main.py` on a second day then re-run `dbt run`.

---

## 16. Daily operations reference

### Start everything for a working session

```cmd
docker start crypto-db
cd "E:\Projects\Pipeline Test\crypto-pipeline\airflow"
docker compose up -d
```

Wait 60 seconds then open http://localhost:8081

### Stop everything cleanly

```cmd
cd "E:\Projects\Pipeline Test\crypto-pipeline\airflow"
docker compose down
docker stop crypto-db
```

### Run pipeline manually

```cmd
cd "E:\Projects\Pipeline Test\crypto-pipeline"
venv\Scripts\activate
python main.py
```

### Backfill a specific date

```cmd
python main.py 2025-04-15
```

### Run dbt locally

```cmd
cd "E:\Projects\Pipeline Test\crypto-pipeline\crypto_transforms"
venv\Scripts\activate
dbt run --target dev
dbt test --target dev
```

### View today's pipeline logs

```cmd
type "E:\Projects\Pipeline Test\crypto-pipeline\logs\pipeline_2025-04-20.log"
```

### Check data row counts

```cmd
docker exec crypto-db psql -U postgres -d crypto_pipeline -c "SELECT COUNT(*), COUNT(DISTINCT fetched_at::DATE) AS days FROM raw.coin_prices;"
```

### Restart Airflow from scratch (wipes Airflow state, not your data)

```cmd
cd "E:\Projects\Pipeline Test\crypto-pipeline\airflow"
docker compose down -v
docker compose up airflow-init
docker compose up -d
```

---

## 17. Connection reference

| Context | Host to use | Why |
|---|---|---|
| Python locally (`python main.py`) | `localhost` | Running on your machine directly |
| dbt locally (`dbt run --target dev`) | `localhost` | Running on your machine directly |
| dbt inside Airflow container (`--target docker`) | `host.docker.internal` | Inside a container, localhost = the container itself |
| Metabase database connection | `host.docker.internal` | Inside a container |
| Airflow webserver URL | `localhost:8081` | Port mapped from container to your machine |
| Metabase URL | `localhost:3000` | Port mapped from container to your machine |

### Port reference

| Service | Port | URL |
|---|---|---|
| crypto-db (PostgreSQL) | 5432 | — |
| Airflow webserver | 8081 | http://localhost:8081 |
| Metabase | 3000 | http://localhost:3000 |
| dbt docs (when running) | 8080 | http://localhost:8080 |

### Credentials

| Service | Username | Password |
|---|---|---|
| crypto-db PostgreSQL | postgres | postgres |
| Airflow UI | airflow | airflow |
| Metabase | Set during setup | Set during setup |

---


