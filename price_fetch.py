import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def fetch_pump_hourly_prices(days=30):
    """
    Fetch hourly historical prices for Pump.fun token from CoinGecko Pro API
    
    Args:
        days: Number of days of historical data to fetch (default: 30)
    
    Returns:
        DataFrame with timestamp, price, market_cap, and volume
    """
    api_key = os.getenv('COINGECKO_PRO_API_KEY')
    
    if not api_key:
        raise ValueError("COINGECKO_PRO_API_KEY not found in .env file")
    
    # CoinGecko Pro API endpoint
    base_url = "https://pro-api.coingecko.com/api/v3"
    
    # Pump.fun token ID on CoinGecko
    coin_id = "pump-fun"
    
    # Headers with API key
    headers = {
        "x-cg-pro-api-key": api_key
    }
    
    # Fetch market chart data (hourly granularity automatic for 2-90 days)
    url = f"{base_url}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days
        # Note: interval is automatic - hourly for 2-90 days, daily for >90 days
    }
    
    print(f"Fetching {days} days of hourly price data for Pump.fun...")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract prices, market caps, and volumes
        prices = data.get('prices', [])
        market_caps = data.get('market_caps', [])
        volumes = data.get('total_volumes', [])
        
        # Convert to DataFrame
        df = pd.DataFrame({
            'timestamp': [datetime.fromtimestamp(p[0]/1000) for p in prices],
            'price_usd': [p[1] for p in prices],
            'market_cap': [m[1] for m in market_caps],
            'volume_24h': [v[1] for v in volumes]
        })
        
        print(f"Successfully fetched {len(df)} hourly data points")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        raise

def save_to_csv(df, filename="pump_fun_hourly_prices.csv"):
    """Save DataFrame to CSV file"""
    df.to_csv(filename, index=False)
    print(f"\nData saved to {filename}")

if __name__ == "__main__":
    # Fetch 365 days of data (will be daily granularity for >90 days)
    df = fetch_pump_hourly_prices(days=365)
    
    # Save to CSV
    save_to_csv(df)
    
    # Display summary statistics
    print("\nSummary Statistics:")
    print(f"Current Price: ${df['price_usd'].iloc[-1]:.6f}")
    print(f"Highest Price: ${df['price_usd'].max():.6f}")
    print(f"Lowest Price: ${df['price_usd'].min():.6f}")
    print(f"Average Price: ${df['price_usd'].mean():.6f}")