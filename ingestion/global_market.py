import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("global_market")

BASE_URL = "https://api.coingecko.com/api/v3"

def fetch():
    # /global returns one object with total market stats:
    # total_market_cap, total_volume, market_cap_percentage
    # (which contains BTC and ETH dominance), active_coins etc.
    url = f"{BASE_URL}/global"
    log.info("Fetching global market data from CoinGecko")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    # The response wraps everything in a "data" key
    # {"data": {"active_cryptocurrencies": 123, ...}}
    # We extract just the inner dict
    data = response.json().get("data", {})
    log.info("Global market data fetched")
    log.debug(f"Raw global data keys: {list(data.keys())}")
    return data

def parse(raw, fetched_at):
    # total_market_cap and total_volume are dicts keyed by currency
    # e.g. {"usd": 2400000000000, "eur": 2200000000000, ...}
    # We want just the USD value, with .get() fallback to None
    market_cap = raw.get("total_market_cap", {}).get("usd")
    volume     = raw.get("total_volume",     {}).get("usd")

    # market_cap_percentage holds dominance % for each coin
    # e.g. {"btc": 52.3, "eth": 17.1, ...}
    dominance  = raw.get("market_cap_percentage", {})
    btc_dom    = dominance.get("btc")
    eth_dom    = dominance.get("eth")
    active     = raw.get("active_cryptocurrencies")

    row = (market_cap, volume, btc_dom, eth_dom, active, fetched_at)
    log.debug(f"Parsed global row: market_cap={market_cap}, btc_dom={btc_dom}")
    return row

def insert(row):
    conn = get_connection()
    cur  = conn.cursor()
    sql  = """
        INSERT INTO raw.global_market
            (total_market_cap_usd, total_volume_usd,
             btc_dominance_pct, eth_dominance_pct,
             active_coins, fetched_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fetched_at) DO NOTHING
    """
    # execute() for a single row (not executemany)
    # We pass the row as a tuple directly
    cur.execute(sql, row)
    conn.commit()
    n = cur.rowcount
    cur.close()
    conn.close()
    if n == 0:
        log.info("Global market row skipped — duplicate timestamp")
    else:
        log.info("Inserted 1 row into raw.global_market")
    return n

def run():
    fetched_at = datetime.now(timezone.utc).replace(
        second=0, microsecond=0
    )
    data = fetch()
    row  = parse(data, fetched_at)
    insert(row)