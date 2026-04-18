import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("coin_prices")

BASE_URL = "https://api.coingecko.com/api/v3"

def fetch():
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order":       "market_cap_desc",
        "per_page":    50,
        "page":        1,
    }
    # log.info() → shown in terminal AND written to log file
    # log.debug() → written to log file only (not terminal)
    # Use INFO for events you always want to see.
    # Use DEBUG for detail that's only useful when debugging.
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
    # Log different messages depending on outcome.
    # This is useful when reviewing logs days later.
    if n == 0:
        log.info("No new rows — all duplicates skipped (idempotent re-run)")
    else:
        log.info(f"Inserted {n} new rows into raw.coin_prices")
    return n

def run():
    fetched_at = datetime.now(timezone.utc).replace(
        second=0, microsecond=0
    )
    data = fetch()
    rows = parse(data, fetched_at)
    insert(rows)