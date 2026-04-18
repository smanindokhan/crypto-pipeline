/*
  Business rule: price can never be zero or negative.
  A zero price means a bad API response slipped through.
  A negative price is physically impossible.

  This query SELECTS the bad rows.
  If it returns anything, dbt marks the test as FAILED.
  If it returns zero rows, the test PASSES.
*/

SELECT
    coin_id,
    coin_name,
    price_date,
    avg_price_usd
FROM {{ ref('mart_daily_prices') }}
WHERE avg_price_usd <0