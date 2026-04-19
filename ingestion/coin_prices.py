import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("coin_prices")

BASE_URL = "https://api.coingecko.com/api/v3"


def fetch():
    url    = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order":       "market_cap_desc",
        "per_page":    50,
        "page":        1,
    }
    log.info("Fetching coin prices from CoinGecko")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    log.info(f"Fetched {len(data)} coins")
    log.debug(f"First coin raw: {data[0]}")
    return data


def parse(raw_data, fetched_at):
    rows = []
    for coin in raw_data:
        rows.append((
            coin.get("id"),
            coin.get("symbol"),
            coin.get("name"),
            coin.get("current_price"),
            coin.get("market_cap"),
            coin.get("total_volume"),
            coin.get("price_change_percentage_24h"),
            fetched_at,
        ))
    log.debug(f"Parsed {len(rows)} rows")
    return rows


def insert(rows):
    conn = get_connection()
    cur  = conn.cursor()
    sql  = """
        INSERT INTO raw.coin_prices
            (coin_id, symbol, name, current_price,
             market_cap, total_volume,
             price_change_pct_24h, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (coin_id, fetched_at) DO NOTHING
    """
    cur.executemany(sql, rows)
    conn.commit()
    n = cur.rowcount
    cur.close()
    conn.close()
    if n == 0:
        log.info("No new rows — all duplicates skipped (idempotent re-run)")
    else:
        log.info(f"Inserted {n} new rows into raw.coin_prices")
    return n


# ── KEY CHANGE ────────────────────────────────────────
# run() now accepts an optional execution_date parameter.
#
# When called with no argument:    uses current UTC time
#   → used when you run manually: python main.py
#
# When called with a datetime:     uses that datetime
#   → used by Airflow: module.run(execution_date=ds)
#
# This single change makes the ingestion Airflow-compatible
# without breaking any existing manual usage.
def run(execution_date: datetime = None):
    if execution_date is None:
        fetched_at = datetime.now(timezone.utc).replace(
            second=0, microsecond=0
        )
    else:
        # Airflow passes naive datetimes — add UTC timezone
        # so our DB timestamps are always timezone-aware
        if execution_date.tzinfo is None:
            fetched_at = execution_date.replace(
                tzinfo=timezone.utc, second=0, microsecond=0
            )
        else:
            fetched_at = execution_date.replace(
                second=0, microsecond=0
            )

    log.info(f"Running coin_prices ingestion for {fetched_at}")
    data = fetch()
    rows = parse(data, fetched_at)
    insert(rows)