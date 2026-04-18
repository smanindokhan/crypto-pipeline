import os
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

# load_dotenv() reads your .env file and puts the values
# into os.environ so you can access them safely
# This means DB_PASSWORD never appears in your code
load_dotenv()

# ── CONFIG ────────────────────────────────────────────
BASE_URL  = "https://api.coingecko.com/api/v3"
DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ── FETCH ─────────────────────────────────────────────
# Separate function — does ONE thing: talk to the API
# If it fails, only this function needs fixing
def fetch_coin_prices():
    url    = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order":       "market_cap_desc",
        "per_page":    50,
        "page":        1,
    }
    # timeout=10 means: if the API doesn't respond in
    # 10 seconds, raise an error instead of hanging forever
    response = requests.get(url, params=params, timeout=10)

    # raise_for_status() checks the HTTP status code.
    # If it's 4xx or 5xx (error), it raises an exception.
    # If it's 200 (success), nothing happens and we continue.
    # This is cleaner than manually checking response.status_code
    response.raise_for_status()

    return response.json()


# ── PARSE ─────────────────────────────────────────────
# Transform raw API response into clean rows for the DB
# .get() is safer than ['key'] — returns None if missing
# instead of crashing with a KeyError
def parse_coins(raw_data, fetched_at):
    rows = []
    for coin in raw_data:
        row = (
            coin.get("id"),
            coin.get("symbol"),
            coin.get("name"),
            coin.get("current_price"),
            coin.get("market_cap"),
            coin.get("total_volume"),
            coin.get("price_change_percentage_24h"),
            fetched_at,
        )
        rows.append(row)
    return rows


# ── INSERT ────────────────────────────────────────────
# This is where idempotency lives
def insert_rows(rows):
    # psycopg2.connect() opens a connection to PostgreSQL
    # using the values from your .env file
    conn = psycopg2.connect(**DB_CONFIG)

    # A cursor is how you send SQL commands through the connection
    # Think of conn as the phone line, cursor as what you speak into
    cur = conn.cursor()

    # THE IDEMPOTENCY SQL — this is the key concept for Week 2
    #
    # INSERT INTO ... VALUES (%s, %s, ...) — the %s are
    # placeholders. psycopg2 fills them in safely from your
    # tuple. Never use f-strings to build SQL — that's how
    # SQL injection attacks happen.
    #
    # ON CONFLICT (coin_id, fetched_at) DO NOTHING — if a row
    # with this exact coin_id + fetched_at already exists,
    # skip it silently instead of crashing with a duplicate error.
    # This means you can safely re-run the script and it will
    # never create duplicate rows.
    sql = """
        INSERT INTO raw.coin_prices
            (coin_id, symbol, name, current_price,
             market_cap, total_volume,
             price_change_pct_24h, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (coin_id, fetched_at) DO NOTHING
    """

    # executemany() runs the same SQL for every row in your list.
    # Much faster than calling execute() in a loop.
    cur.executemany(sql, rows)

    # conn.commit() actually saves the changes to disk.
    # Until you commit, the inserts exist only in a transaction
    # that hasn't been finalised — a crash before commit = no data.
    # cur.rowcount tells you how many rows were actually inserted
    # (rows skipped by ON CONFLICT are not counted)
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    conn.close()
    return inserted


# ── MAIN ──────────────────────────────────────────────
# Ties everything together. This only runs when you execute
# the file directly — not when another file imports it.
def main():
    # Capture the timestamp BEFORE fetching.
    # All 50 coins in one batch share the same fetched_at.
    # timezone.utc means UTC time — always use UTC in pipelines,
    # never local time (it changes with daylight saving)
    fetched_at = datetime.now(timezone.utc).replace(
        second=0, microsecond=0
    )

    print(f"[{fetched_at}] Fetching coin prices...")
    raw_data = fetch_coin_prices()
    print(f"  Fetched {len(raw_data)} coins from API")

    rows = parse_coins(raw_data, fetched_at)
    print(f"  Parsed {len(rows)} rows")

    inserted = insert_rows(rows)
    print(f"  Inserted {inserted} new rows (duplicates skipped)")
    print("Done.")


if __name__ == "__main__":
    main()