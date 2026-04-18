/*
  Business rule: total crypto market dominance across all
  coins must be close to 100%.

  We check that BTC + ETH dominance doesn't EXCEED 100%,
  which would indicate a data corruption issue.

  We use > 100.5 (not > 100) to allow for minor rounding
  in the CoinGecko API response.
*/

SELECT
    market_date,
    btc_dominance_pct,
    eth_dominance_pct,
    btc_eth_combined_pct
FROM {{ ref('mart_market_dominance') }}
WHERE btc_eth_combined_pct > 100.5