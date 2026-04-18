/*
  One row per coin per day.
  Aggregates intra-day fetches into a single daily snapshot,
  then adds window functions for trend analysis.

  This mart answers:
    - What was Bitcoin's closing price on each day?
    - How much did it change vs the day before?
    - What is the 7-day rolling average price?
*/

WITH daily_agg AS (

    -- Step 1: collapse multiple fetches per day into one row.
    -- We take the LAST price of the day as the "closing" price.
    -- MAX(fetched_at) picks the latest fetch timestamp per day.
    SELECT
        coin_id,
        coin_name,
        symbol,
        price_date,

        -- AVG across all intra-day fetches for a smoother daily price
        ROUND(AVG(price_usd)::NUMERIC,       2)  AS avg_price_usd,
        ROUND(MAX(price_usd)::NUMERIC,       2)  AS high_price_usd,
        ROUND(MIN(price_usd)::NUMERIC,       2)  AS low_price_usd,
        ROUND(AVG(price_change_pct_24h)::NUMERIC, 4) AS avg_price_change_pct_24h,

        -- SUM of volume gives total daily trading volume
        ROUND(AVG(volume_usd)::NUMERIC,      0)  AS avg_volume_usd,
        ROUND(AVG(market_cap_usd)::NUMERIC,  0)  AS avg_market_cap_usd

    FROM {{ ref('stg_coin_prices') }}
    GROUP BY coin_id, coin_name, symbol, price_date

),

with_windows AS (

    SELECT
        coin_id,
        coin_name,
        symbol,
        price_date,
        avg_price_usd,
        high_price_usd,
        low_price_usd,
        avg_price_change_pct_24h,
        avg_volume_usd,
        avg_market_cap_usd,

        -- WINDOW FUNCTION 1: LAG
        -- "What was this coin's price yesterday?"
        -- PARTITION BY coin_id  → separate window per coin
        -- ORDER BY price_date   → yesterday = the row before today
        -- LAG(..., 1)           → go back exactly 1 row
        -- The result is NULL on the first day (no prior day)
        LAG(avg_price_usd, 1)
            OVER (
                PARTITION BY coin_id
                ORDER BY price_date
            )                                   AS prev_day_price_usd,

        -- WINDOW FUNCTION 2: Day-over-day % change
        -- Derived from LAG — how much did price move since yesterday?
        -- Formula: (today - yesterday) / yesterday * 100
        -- NULLIF prevents division by zero if yesterday's price = 0
        ROUND(
            (
                (avg_price_usd - LAG(avg_price_usd, 1) OVER (
                    PARTITION BY coin_id ORDER BY price_date
                ))
                /
                NULLIF(LAG(avg_price_usd, 1) OVER (
                    PARTITION BY coin_id ORDER BY price_date
                ), 0)
                * 100
            )::NUMERIC,
        4)                                      AS day_over_day_pct_change,

        -- WINDOW FUNCTION 3: 7-day rolling average
        -- "What is the average price over the last 7 days including today?"
        -- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        --   = current row + the 6 rows before it = 7 rows total
        -- This smooths out daily noise to show the trend
        ROUND(
            AVG(avg_price_usd) OVER (
                PARTITION BY coin_id
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::NUMERIC,
        2)                                      AS rolling_7d_avg_price,

        -- WINDOW FUNCTION 4: Rank by market cap on each day
        -- "Was Bitcoin #1 on this date? Was Ethereum #2?"
        -- PARTITION BY price_date → rank resets for each day
        -- ORDER BY market_cap DESC → #1 = largest market cap
        RANK() OVER (
            PARTITION BY price_date
            ORDER BY avg_market_cap_usd DESC
        )                                       AS daily_market_cap_rank

    FROM daily_agg

)

SELECT * FROM with_windows
ORDER BY price_date DESC, daily_market_cap_rank ASC