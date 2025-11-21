import os
import requests
import joblib
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

dune_api_key = os.getenv("DEFI_JOSH_DUNE_QUERY_API_KEY")

query_ids = [
            4903533,		# Lifetime Revenue
            3759856,	    # Daily Revenue
            4861426,	    # Daily Tokens Created + Graduated Tokens
            3756231,		# Lifetime tokens
            3979030,		# Tokens Launched (last 24 hours)
            3979025,		# Tokens Graduated (last 24 hours)
            4124453,		# Graduated Tokens List (last 24 hours)
            4160410,		# Weekly Graduation Rate
            4048901,		# Monthly Token Stats
            4160369,		# Weekly Volume
            4512652,		# Average Volume by day of the week (last 2 months)
            4511889,		# Average Hourly Volume (last 2 months)
            4903519,		# Daily Active Pump.fun Users



            5508754,		# Daily $PUMP holders
            5508692,		# Latest $PUMP holders
            5629742,		# Latest top 1000 $PUMP holders list, their latest balances and holding change in the last 7 days
            5508992,		# $PUMP Holders distribution
            5508970,		# Latest $PUMP Holders Distribution
            5508493,		# Daily $PUMP Buybacks
            5485685,		# Total $PUMP Buybacks


            4893631,		# Daily PumpSwap Volume
            4893899,		# PumpSwap Volume (24 hour, 7days, Lifetime)
            4894737,		# Daily Active Pumpswap Users
            4894925,		# Daily Pumpswap swaps/trades
            4894704,		# Lifetime Protocol and LP fees
            5220565,		# Lifetime Creator Revenue Fees (Sol)
            4896097,		# Top 20 Graduated Tokens on Pumpswap (last 24 hours)
            4894743		    # Top 20 Pools on Pumpswap (last 24 hours)
]

os.makedirs("data", exist_ok=True)

for query_id in query_ids:
    print(f"\nFetching data for query ID: {query_id}")
    
    url = f"https://api.dune.com/api/v1/query/{query_id}/results/csv"
    headers = {"X-DUNE-API-KEY": dune_api_key}
    
    joblib_file = f"data/query_{query_id}_data.joblib"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        
        query_data = {
            "query_id": query_id,
            "data": response.text,
            "last_updated": datetime.now().isoformat(),
            "status": "success"
        }
        
        joblib.dump(query_data, joblib_file)
        
        print(f"✓ Successfully fetched and saved query {query_id} to {joblib_file}")
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching query {query_id}: {e}")
        
        query_data = {
            "query_id": query_id,
            "data": None,
            "last_updated": datetime.now().isoformat(),
            "status": f"error: {str(e)}"
        }
        joblib.dump(query_data, joblib_file)

print("\n✓ All queries processed")