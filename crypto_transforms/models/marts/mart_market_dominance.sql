/*
  Daily BTC/ETH dominance trends joined with coin prices.
  Collapses multiple intra-day fetches into one row per day
  using AVG — consistent with mart_daily_prices approach.
*/

WITH global_agg AS (

    -- Step 1: collapse multiple fetches per day into one daily row.
    -- This is what was missing before — without this GROUP BY,
    -- multiple pipeline runs on the same day produce duplicate
    -- market_date values and fail the unique test.
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

        -- 7-day rolling average of BTC dominance
        ROUND(
            AVG(btc_dominance_pct) OVER (
                ORDER BY market_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::NUMERIC,
        2)                                        AS btc_dominance_7d_avg,

        -- Day-over-day change in total market cap
        ROUND(
            (
                (total_market_cap_usd - LAG(total_market_cap_usd, 1)
                    OVER (ORDER BY market_date))
                /
                NULLIF(LAG(total_market_cap_usd, 1)
                    OVER (ORDER BY market_date), 0)
                * 100
            )::NUMERIC,
        4)                                        AS market_cap_day_change_pct

    FROM global_agg

),

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
LEFT JOIN btc_prices b ON g.market_date = b.price_date
LEFT JOIN eth_prices e ON g.market_date = e.price_date

ORDER BY g.market_date DESC