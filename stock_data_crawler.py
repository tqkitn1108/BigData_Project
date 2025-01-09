import pandas as pd
import time
import os
import json
from datetime import datetime
from vnstock3 import Vnstock

# Initialize Vnstock instance
vnstock_instance = Vnstock()

# Configure display settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Ensure the 'raw_data' directory exists
os.makedirs("./raw_data", exist_ok=True)

# Fetch stock symbols and industry data
stock = vnstock_instance.stock(symbol='AAA', source='VCI')
all_symbols = stock.listing.all_symbols()  # All stock symbols
industry_data = stock.listing.symbols_by_industries()  # Industry data
industry_dict = industry_data.set_index('symbol').to_dict(orient='index')

# Define batch size as 5% of total tickers
batch_size = max(1, len(all_symbols) // 20)  # Ensure at least one symbol per batch

# Get the current date for the end date
current_date = datetime.now().strftime('%Y-%m-%d')

def fetch_data_for_batch(batch):
    batch_data = []
    for _, row in batch.iterrows():
        ticker = row['ticker']
        industry_info = industry_dict.get(ticker, {})
        history = stock.quote.history(symbol=ticker, start='2003-1-1', end=current_date, interval='1D')
        if not history.empty:
            history['ticker'] = ticker
            for key, value in industry_info.items():
                history[key] = value
            batch_data.extend(history.to_dict(orient='records'))
    return batch_data

# Iterate through all symbols in batches
for i in range(0, len(all_symbols), batch_size):
    batch = all_symbols.iloc[i:i + batch_size]
    batch_index = i // batch_size + 1  # Batch index for file naming
    file_name = f"./raw_data/stock_{batch_index:02d}.json"
    print(f"Processing batch {batch_index} and saving to {file_name}...")
    
    # Fetch data for the current batch
    batch_data = fetch_data_for_batch(batch)
    
    # Write batch data to its own file
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(batch_data, f, ensure_ascii=False, indent=4)
    
    print(f"Batch {batch_index} saved to {file_name}.")
    time.sleep(1)  # Optional sleep to prevent API rate limits

print("All batches processed and saved.")
