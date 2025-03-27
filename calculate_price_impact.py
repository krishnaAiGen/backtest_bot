import pandas as pd
import os
from datetime import datetime, timedelta
import pytz

def calculate_price_impact():
    # File paths
    posts_file = "ai_posts_2024.csv"
    output_file = "ai_posts_2024_with_price_impact.csv"
    price_data_dir = "price_data"
    
    print(f"Loading posts data from {posts_file}...")
    try:
        # Load the posts data
        posts_df = pd.read_csv(posts_file)
        total_posts = len(posts_df)
        print(f"Loaded {total_posts} posts")
        
        # Ensure timestamp is in datetime format and timezone-aware
        posts_df['timestamp'] = pd.to_datetime(posts_df['timestamp'], utc=True)
        
        # Initialize the columns for price analysis
        posts_df['percent_increase'] = None
        posts_df['max_percent_gain'] = None
        posts_df['min_percent_loss'] = None
        posts_df['max_price_date'] = None
        posts_df['min_price_date'] = None
        posts_df['days_to_max'] = None
        posts_df['days_to_min'] = None
        
        # Dictionary to store loaded price dataframes to avoid reloading
        price_dfs = {}
        
        # Process each post
        processed_count = 0
        for index, row in posts_df.iterrows():
            protocol = row['protocol']
            post_date = row['timestamp']
            
            # Calculate date 5 days after post
            future_date = post_date + timedelta(days=5)
            
            # Load price data for this protocol if not already loaded
            if protocol not in price_dfs:
                price_file = os.path.join(price_data_dir, f"{protocol}_price.csv")
                if os.path.exists(price_file):
                    try:
                        price_df = pd.read_csv(price_file)
                        price_df['timestamp'] = pd.to_datetime(price_df['timestamp'], utc=True)
                        price_dfs[protocol] = price_df
                        print(f"Loaded price data for {protocol}")
                    except Exception as e:
                        print(f"Error loading price data for {protocol}: {e}")
                        continue
                else:
                    print(f"No price data found for {protocol}")
                    continue
            
            price_df = price_dfs[protocol]
            
            # Find closest price points to post date
            closest_post_price = find_closest_price(price_df, post_date)
            if closest_post_price is None:
                continue
                
            # Find the closest price point to the future date
            closest_future_price = find_closest_price(price_df, future_date)
            
            # Get all price points and dates during the 5-day window
            window_data = get_price_data_in_time_window(price_df, post_date, future_date)
            
            if closest_post_price is not None and closest_future_price is not None and len(window_data) > 0:
                prices = [d['price'] for d in window_data]
                dates = [d['timestamp'] for d in window_data]
                
                # Calculate percent increase from start to end
                percent_increase = ((closest_future_price - closest_post_price) / closest_post_price) * 100
                posts_df.at[index, 'percent_increase'] = percent_increase
                
                # Calculate max gain and min loss during the period
                if prices:
                    max_price = max(prices)
                    min_price = min(prices)
                    
                    # Get indices of max and min prices
                    max_idx = prices.index(max_price)
                    min_idx = prices.index(min_price)
                    
                    # Get dates of max and min prices
                    max_date = dates[max_idx]
                    min_date = dates[min_idx]
                    
                    # Calculate days from post to max/min price
                    days_to_max = (max_date - post_date).total_seconds() / 86400  # Convert seconds to days
                    days_to_min = (min_date - post_date).total_seconds() / 86400
                    
                    # Calculate as percentage relative to the starting price
                    max_percent_gain = ((max_price - closest_post_price) / closest_post_price) * 100
                    min_percent_loss = ((min_price - closest_post_price) / closest_post_price) * 100
                    
                    posts_df.at[index, 'max_percent_gain'] = max_percent_gain
                    posts_df.at[index, 'min_percent_loss'] = min_percent_loss
                    posts_df.at[index, 'max_price_date'] = max_date
                    posts_df.at[index, 'min_price_date'] = min_date
                    posts_df.at[index, 'days_to_max'] = days_to_max
                    posts_df.at[index, 'days_to_min'] = days_to_min
                
                processed_count += 1
        
        print(f"Processed price data for {processed_count} out of {total_posts} posts")
        
        # Save the updated dataframe
        posts_df.to_csv(output_file, index=False)
        print(f"Updated data saved to {output_file}")
        
        # Summary statistics
        posts_with_price_data = posts_df.dropna(subset=['percent_increase'])
        if not posts_with_price_data.empty:
            avg_increase = posts_with_price_data['percent_increase'].mean()
            median_increase = posts_with_price_data['percent_increase'].median()
            avg_max_gain = posts_with_price_data['max_percent_gain'].mean()
            avg_min_loss = posts_with_price_data['min_percent_loss'].mean()
            avg_days_to_max = posts_with_price_data['days_to_max'].mean()
            avg_days_to_min = posts_with_price_data['days_to_min'].mean()
            
            positive_impact = (posts_with_price_data['percent_increase'] > 0).sum()
            negative_impact = (posts_with_price_data['percent_increase'] < 0).sum()
            
            print("\nSummary statistics:")
            print(f"Average percent increase after 5 days: {avg_increase:.2f}%")
            print(f"Median percent increase after 5 days: {median_increase:.2f}%")
            print(f"Average maximum gain within 5 days: {avg_max_gain:.2f}%")
            print(f"Average minimum loss within 5 days: {avg_min_loss:.2f}%")
            print(f"Average days to reach maximum price: {avg_days_to_max:.2f} days")
            print(f"Average days to reach minimum price: {avg_days_to_min:.2f} days")
            print(f"Posts with positive price impact: {positive_impact} ({positive_impact/len(posts_with_price_data)*100:.2f}%)")
            print(f"Posts with negative price impact: {negative_impact} ({negative_impact/len(posts_with_price_data)*100:.2f}%)")
            
            # Protocol-wise average impact
            protocol_impact = posts_with_price_data.groupby('protocol')['percent_increase'].agg(['mean', 'count']).sort_values('mean', ascending=False)
            print("\nAverage impact by protocol (minimum 5 posts):")
            for protocol, row in protocol_impact[protocol_impact['count'] >= 5].iterrows():
                print(f"{protocol}: {row['mean']:.2f}% (based on {int(row['count'])} posts)")
    
    except Exception as e:
        print(f"Error processing the data: {e}")
        import traceback
        traceback.print_exc()

def find_closest_price(price_df, target_date):
    """Find the price closest to the target date"""
    if price_df.empty:
        return None
    
    # Find the closest date (forward or backward)
    closest_row = price_df.iloc[(price_df['timestamp'] - target_date).abs().argsort()[0]]
    
    # Check if the date is within a reasonable range (2 days)
    if abs((closest_row['timestamp'] - target_date).total_seconds()) > 172800:  # 2 days in seconds
        return None
    
    return float(closest_row['price'])

def get_price_data_in_time_window(price_df, start_date, end_date):
    """Get all price data within a time window"""
    if price_df.empty:
        return []
    
    # Filter prices that fall within the time window
    mask = (price_df['timestamp'] >= start_date) & (price_df['timestamp'] <= end_date)
    window_df = price_df[mask]
    
    if window_df.empty:
        return []
    
    # Return list of dictionaries with price and timestamp
    return [{'price': float(row['price']), 'timestamp': row['timestamp']} 
            for _, row in window_df.iterrows()]

def get_prices_in_time_window(price_df, start_date, end_date):
    """Get all prices within a time window (backward compatibility)"""
    data = get_price_data_in_time_window(price_df, start_date, end_date)
    return [item['price'] for item in data]

if __name__ == "__main__":
    calculate_price_impact() 