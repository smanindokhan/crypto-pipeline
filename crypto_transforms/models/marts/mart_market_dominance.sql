/*
  Daily market dominance trends for BTC and ETH.
  Joins global market data with individual coin prices
  to show dominance alongside actual price movement.

  This mart answers:
    - Is BTC dominance rising or falling over time?
    - Does ETH price correlate with its dominance shift?
*/

WITH global AS (

    SELECT
        market_date,
        total_market_cap_usd,
        btc_dominance_pct,
        eth_dominance_pct,
        btc_eth_combined_pct,
        active_coins,

        -- 7-day rolling average of BTC dominance
        -- shows the trend, not just the daily noise
        ROUND(
            AVG(btc_dominance_pct) OVER (
                ORDER BY market_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::NUMERIC,
        2)                                      AS btc_dominance_7d_avg,

        -- Day-over-day change in total market cap
        -- positive = market grew, negative = market shrank
        ROUND(
            (
                (total_market_cap_usd - LAG(total_market_cap_usd, 1)
                    OVER (ORDER BY market_date))
                /
                NULLIF(LAG(total_market_cap_usd, 1)
                    OVER (ORDER BY market_date), 0)
                * 100
            )::NUMERIC,
        4)                                      AS market_cap_day_change_pct

    FROM {{ ref('stg_global_market') }}

),

-- Pull just BTC and ETH rows from the daily prices mart
-- so we can show their price alongside their dominance
btc_prices AS (
    SELECT price_date, avg_price_usd AS btc_price_usd
    FROM {{ ref('mart_daily_prices') }}
    WHERE coin_id = 'bitcoin'
),

eth_prices AS (
    SELECT price_date, avg_price_usd AS eth_price_usd
    FROM {{ ref('mart_daily_prices') }}
    WHERE coin_id = 'ethereum'
)

SELECT
    g.market_date,
    g.total_market_cap_usd,
    g.btc_dominance_pct,
    g.eth_dominance_pct,
    g.btc_eth_combined_pct,
    g.btc_dominance_7d_avg,
    g.market_cap_day_change_pct,
    g.active_coins,
    b.btc_price_usd,
    e.eth_price_usd

FROM global g

-- LEFT JOIN so global market rows are never dropped
-- even if we somehow have no BTC or ETH price for that day
LEFT JOIN btc_prices b ON g.market_date = b.price_date
LEFT JOIN eth_prices e ON g.market_date = e.price_date

ORDER BY g.market_date DESC