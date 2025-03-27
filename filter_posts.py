import pandas as pd
from datetime import datetime
import pytz

def filter_posts_by_date_range():
    # File paths
    input_file = "ai_posts.csv"
    output_file = "ai_posts_2024.csv"
    
    # Date range for filtering (make timezone-aware)
    start_date = datetime.strptime("2024-03-10", "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    end_date = datetime.strptime("2025-03-25", "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    
    print(f"Loading data from {input_file}...")
    # Load the data
    try:
        df = pd.read_csv(input_file)
        original_row_count = len(df)
        print(f"Original dataset has {original_row_count} rows")
        
        # Convert timestamp column to datetime with mixed format handling and always use UTC
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce', utc=True)
        
        # Drop rows with invalid timestamps
        invalid_dates = df['timestamp'].isna().sum()
        if invalid_dates > 0:
            print(f"Found {invalid_dates} rows with invalid timestamps. These will be dropped.")
            df = df.dropna(subset=['timestamp'])
            
        # Filter by date range
        filtered_df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
        filtered_row_count = len(filtered_df)
        
        print(f"Filtered dataset has {filtered_row_count} rows")
        print(f"Removed {original_row_count - filtered_row_count - invalid_dates} rows outside the date range")
        
        # Save filtered data
        filtered_df.to_csv(output_file, index=False)
        print(f"Filtered data saved to {output_file}")
        
        # Display distribution of protocols in filtered data
        protocol_counts = filtered_df['protocol'].value_counts()
        print("\nProtocol distribution in filtered data:")
        for protocol, count in protocol_counts.items():
            print(f"{protocol}: {count}")
        
    except Exception as e:
        print(f"Error processing the data: {e}")

if __name__ == "__main__":
    filter_posts_by_date_range() 