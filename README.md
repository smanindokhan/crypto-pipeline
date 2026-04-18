# Crypto Pipeline

A production-style data pipeline built with Python, PostgreSQL, dbt, and Airflow.

## Architecture
CoinGecko API → Python ingestion → PostgreSQL (raw layer) → dbt (coming) → Airflow (coming)

## Data Sources
- `/coins/markets` — top 50 coins by market cap (price, volume, 24h change)
- `/global` — total crypto market cap, BTC/ETH dominance, active coins

## How to run
```
# Activate virtual environment
venv\Scripts\activate

# Run full pipeline
python main.py
```

## Logs
Written to `logs/pipeline_YYYY-MM-DD.log` daily. INFO to terminal, DEBUG to file.

## Project structure
- `ingestion/` — one module per API source
- `db.py` — shared database connection
- `logger.py` — shared logging setup
- `main.py` — pipeline entry point