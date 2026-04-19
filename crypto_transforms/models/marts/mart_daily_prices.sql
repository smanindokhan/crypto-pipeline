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

        LAG(avg_price_usd, 1)
            OVER (
                PARTITION BY coin_id
                ORDER BY price_date
            )                                   AS prev_day_price_usd,

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

        ROUND(
            AVG(avg_price_usd) OVER (
                PARTITION BY coin_id
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::NUMERIC,
        2)                                      AS rolling_7d_avg_price,

        RANK() OVER (
            PARTITION BY price_date
            ORDER BY avg_market_cap_usd DESC
        )                                       AS daily_market_cap_rank

    FROM daily_agg

),

-- New CTE: add the surrogate key using dbt_utils macro.
-- generate_surrogate_key hashes the list of columns using MD5
-- and returns a consistent 32-character hex string.
-- The same coin_id + price_date always produces the same key.
final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['coin_id', 'price_date']) }}
                                                AS daily_price_id,
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
        prev_day_price_usd,
        day_over_day_pct_change,
        rolling_7d_avg_price,
        daily_market_cap_rank

    FROM with_windows

)

SELECT * FROM final
ORDER BY price_date DESC, daily_market_cap_rank ASC