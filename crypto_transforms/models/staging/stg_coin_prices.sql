/*
  Staging model for raw.coin_prices.

  What we do here:
    - Rename columns to consistent snake_case
    - Cast price to FLOAT for easier downstream math
    - Add a date column derived from fetched_at
    - Filter out rows with null price (bad API data)

  What we do NOT do here:
    - No aggregations
    - No joins to other tables
    - No business logic like "is this coin a top mover"
*/

WITH src AS (

    SELECT * FROM {{ source('raw', 'coin_prices') }}

),

renamed AS (

    SELECT
        coin_id,
        symbol,
        name AS coin_name,
        current_price::FLOAT AS price_usd,
        market_cap AS market_cap_usd,
        total_volume AS volume_usd,
        price_change_pct_24h,
        fetched_at::DATE AS price_date,
        fetched_at
    FROM src

),

cleaned AS (

    SELECT *
    FROM renamed
    WHERE price_usd IS NOT NULL

)

SELECT * FROM cleaned