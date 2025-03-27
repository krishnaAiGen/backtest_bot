import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime, timedelta
import os
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Create a directory to store price data if it doesn't exist
os.makedirs('price_data', exist_ok=True)

# Initialize Binance client - No API key needed for historical data
client = Client("", "")

# Mapping tokens to Binance trading symbols
# Note: Some tokens might not be available on Binance or might have different symbols
tokens = {
    'pankcakeswap': 'CAKEUSDT',
    'balancer': 'BALUSDT',
    'aave': 'AAVEUSDT',
    'arbitrum': 'ARBUSDT',
    'apecoin': 'APEUSDT',
    'sushi': 'SUSHIUSDT',
    'curve': 'CRVUSDT',
    'uniswap': 'UNIUSDT',
    'venus': 'XVSUSDT',
    'badger': 'BADGERUSDT',
    'optimism': 'OPUSDT',
    'quickswap': 'QUICKUSDT',
    'synapse': 'SYNUSDT',
    'pendle': 'PENDLEUSDT',
    '0xprotocol': 'ZRXUSDT',
    'lido': 'LDOUSDT',
    'compound': 'COMPUSDT',
    'starknet': 'STRK',
    'etherfi': 'EFIUSDT',
    'immutablex': 'IMXUSDT',
    'renzo': 'RENUSDT'
}

# Define the date range
start_date = "2024-03-10"
end_date = datetime.now().strftime("%Y-%m-%d")  # Current date as end date since future data isn't available

def get_price_data(symbol, start_date, end_date):
    """
    Get historical price data from Binance
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        
    Returns:
        DataFrame with timestamp and price or None if error
    """
    try:
        # Convert date strings to datetime objects
        start_datetime = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_datetime = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
        
        # Get klines (candlestick data)
        klines = client.get_historical_klines(
            symbol=symbol,
            interval=Client.KLINE_INTERVAL_1DAY,
            start_str=start_datetime,
            end_str=end_datetime
        )
        
        # Create DataFrame
        df = pd.DataFrame(
            klines,
            columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ]
        )
        
        # Keep only timestamp and close price
        df = df[['timestamp', 'close']]
        df.columns = ['timestamp', 'price']
        
        # Convert to proper types
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['price'] = df['price'].astype(float)
        
        return df
    
    except BinanceAPIException as e:
        print(f"Error retrieving data for {symbol}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error for {symbol}: {e}")
        return None

def fetch_all_token_prices():
    """Fetch price data for all tokens and save to CSV files"""
    for token_name, symbol in tokens.items():
        print(f"Fetching price data for {token_name} ({symbol})...")
        
        try:
            df = get_price_data(symbol, start_date, end_date)
            
            if df is not None and not df.empty:
                # Save to CSV
                filename = f"price_data/{token_name}_price.csv"
                df.to_csv(filename, index=False)
                print(f"Saved price data for {token_name} to {filename}")
            else:
                print(f"No data available for {token_name} ({symbol})")
                
            # Add delay to be nice to the API
            time.sleep(1)
            
        except Exception as e:
            print(f"Failed to process {token_name}: {e}")

if __name__ == "__main__":
    print("Starting price data collection using Binance API...")
    fetch_all_token_prices()
    print("Price data collection completed!") 