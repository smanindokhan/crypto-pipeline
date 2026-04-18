/*
  Top 5 gainers and top 5 losers per day, by day-over-day % change.
  Excludes the first day per coin (no prior day = no change to rank).

  This mart answers:
    - Which coins gained the most on a given day?
    - Which coins lost the most?
*/

WITH ranked AS (

    SELECT
        price_date,
        coin_id,
        coin_name,
        symbol,
        avg_price_usd,
        day_over_day_pct_change,
        daily_market_cap_rank,

        -- Rank coins by biggest GAIN on each day
        -- PARTITION BY price_date → ranking resets per day
        -- ORDER BY DESC → rank 1 = biggest gainer
        RANK() OVER (
            PARTITION BY price_date
            ORDER BY day_over_day_pct_change DESC
        )                                       AS gain_rank,

        -- Rank coins by biggest LOSS on each day
        -- ORDER BY ASC → rank 1 = biggest loser (most negative)
        RANK() OVER (
            PARTITION BY price_date
            ORDER BY day_over_day_pct_change ASC
        )                                       AS loss_rank

    FROM {{ ref('mart_daily_prices') }}

    -- Exclude rows where we have no prior day to compare against.
    -- day_over_day_pct_change is NULL on the first day of data.
    WHERE day_over_day_pct_change IS NOT NULL

),

top_movers AS (

    SELECT
        price_date,
        coin_id,
        coin_name,
        symbol,
        avg_price_usd,
        day_over_day_pct_change,

        -- Label each row so the dashboard knows which list it belongs to
        CASE
            WHEN gain_rank <= 5 AND loss_rank <= 5
                THEN 'both'        -- rare but possible near 0% change
            WHEN gain_rank <= 5
                THEN 'top_gainer'
            WHEN loss_rank <= 5
                THEN 'top_loser'
        END                                     AS mover_type,

        LEAST(gain_rank, loss_rank)             AS rank_position

    FROM ranked
    WHERE gain_rank <= 5 OR loss_rank <= 5

)

SELECT * FROM top_movers
ORDER BY price_date DESC, mover_type, rank_position