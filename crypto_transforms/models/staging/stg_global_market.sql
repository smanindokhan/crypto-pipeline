/*
  Staging model for raw.global_market.
  Cleans and renames the global market snapshot data.
*/

WITH src AS (

    SELECT * FROM {{ source('raw', 'global_market') }}

),

renamed AS (

    SELECT
        total_market_cap_usd::FLOAT               AS total_market_cap_usd,
        total_volume_usd::FLOAT                   AS total_volume_usd,
        btc_dominance_pct::FLOAT                  AS btc_dominance_pct,
        eth_dominance_pct::FLOAT                  AS eth_dominance_pct,
        active_coins,

        -- Derived column: combined BTC + ETH dominance.
        -- This is the simplest possible business logic —
        -- and even this arguably belongs in a mart, not staging.
        -- We include it here as a teaching example of where the
        -- line is: a simple arithmetic derivation is ok in staging,
        -- a conditional or join is not.
        ROUND(
            (btc_dominance_pct + eth_dominance_pct)::NUMERIC, 2
        )                                         AS btc_eth_combined_pct,

        fetched_at::DATE                          AS market_date,
        fetched_at                                AS fetched_at

    FROM src

    WHERE total_market_cap_usd IS NOT NULL

)

SELECT * FROM renamed