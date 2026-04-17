import requests
import json

# The base URL for all CoinGecko API calls
BASE_URL = "https://api.coingecko.com/api/v3"

# Build the full URL with parameters
# ?vs_currency=usd → prices in US dollars
# &order=market_cap_desc → biggest coins first
# &per_page=10 → only top 10 coins
# &page=1 → first page of results
url = f"{BASE_URL}/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1
}

# Make the HTTP GET request
response = requests.get(url, params=params)

# .status_code 200 = success, 429 = rate limited, 500 = server error
print(f"Status code: {response.status_code}")

# Parse the JSON response into a Python list
data = response.json()

# Print just the first coin to understand the structure
print("\n--- First coin in response ---")
print(json.dumps(data[0], indent=2))

# Print a summary of all 10 coins
print("\n--- All 10 coins ---")
for coin in data:
    print(f"{coin['name']:15} ${coin['current_price']:>10,.2f} market cap rank #{coin['market_cap_rank']}")