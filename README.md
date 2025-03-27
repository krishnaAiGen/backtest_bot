# Cryptocurrency Data Collection Scripts

This repository contains scripts for collecting cryptocurrency-related data for backtesting and analysis.

## Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Make sure you have a `config.json` file with the necessary Firebase credentials for `get_data.py`.

## Scripts

### 1. get_data.py

Retrieves proposal data from Firebase and saves it to a CSV file.

Usage:
```
python get_data.py
```

### 2. get_price.py

Fetches historical price data for various cryptocurrencies from Binance API.

- Date range: March 10, 2024 to current date
- Interval: 1-day
- Price currency: USD (paired as USDT)

Usage:
```
python get_price.py
```

The script will:
1. Create a `price_data` directory if it doesn't exist
2. Download price data for each cryptocurrency from Binance
3. Save individual CSV files named `{token_name}_price.csv` in the `price_data` directory

Note: The script uses Binance's public API which has generous rate limits. If a token is not available on Binance or has a different symbol, you may need to adjust the symbols in the code.

## Output Files

- `ai_posts.csv`: Contains proposal data from Firebase
- `price_data/{token_name}_price.csv`: Individual price data files for each cryptocurrency 