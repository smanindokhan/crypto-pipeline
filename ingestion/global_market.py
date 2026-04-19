import requests
from datetime import datetime, timezone
from db import get_connection
from logger import get_logger

log = get_logger("global_market")

BASE_URL = "https://api.coingecko.com/api/v3"


def fetch():
    url = f"{BASE_URL}/global"
    log.info("Fetching global market data from CoinGecko")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json().get("data", {})
    log.info("Global market data fetched")
    log.debug(f"Raw global data keys: {list(data.keys())}")
    return data


def parse(raw, fetched_at):
    market_cap = raw.get("total_market_cap", {}).get("usd")
    volume     = raw.get("total_volume",     {}).get("usd")
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


def run(execution_date: datetime = None):
    if execution_date is None:
        fetched_at = datetime.now(timezone.utc).replace(
            second=0, microsecond=0
        )
    else:
        if execution_date.tzinfo is None:
            fetched_at = execution_date.replace(
                tzinfo=timezone.utc, second=0, microsecond=0
            )
        else:
            fetched_at = execution_date.replace(
                second=0, microsecond=0
            )

    log.info(f"Running global_market ingestion for {fetched_at}")
    data = fetch()
    row  = parse(data, fetched_at)
    insert(row)