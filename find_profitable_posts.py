import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta

def find_profitable_posts():
    """
    Identify posts that led to significant price increases without triggering stop loss.
    
    Specifically, find posts where:
    1. Price increased by more than 10%, 20%, or 40%
    2. Price never dropped by 4% or more from the post date (stop loss)
    """
    # File paths
    input_file = "ai_posts_2024_with_price_impact.csv"
    output_10pct = "profitable_posts_10pct.csv"
    output_20pct = "profitable_posts_20pct.csv"
    output_40pct = "profitable_posts_40pct.csv"
    
    print(f"Loading data from {input_file}...")
    try:
        # Load the data with price impact information
        df = pd.read_csv(input_file)
        total_posts = len(df)
        print(f"Loaded {total_posts} posts")
        
        # Ensure timestamps are in datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['max_price_date'] = pd.to_datetime(df['max_price_date'], utc=True)
        df['min_price_date'] = pd.to_datetime(df['min_price_date'], utc=True)
        
        # Calculate stop loss hit (when price drops by 4% or more)
        df['stop_loss_hit'] = df['min_percent_loss'] <= -4.0
        
        # Create profit categories
        df['profit_above_10pct'] = (df['max_percent_gain'] > 10) & (~df['stop_loss_hit'])
        df['profit_above_20pct'] = (df['max_percent_gain'] > 20) & (~df['stop_loss_hit'])
        df['profit_above_40pct'] = (df['max_percent_gain'] > 40) & (~df['stop_loss_hit'])
        
        # Filter for each profit category
        profitable_10pct = df[df['profit_above_10pct']].copy()
        profitable_20pct = df[df['profit_above_20pct']].copy()
        profitable_40pct = df[df['profit_above_40pct']].copy()
        
        # Function to check if stop loss would be hit before max gain
        def stop_loss_before_max_gain(row):
            """Check if stop loss would be hit before reaching maximum gain"""
            if pd.isnull(row['min_price_date']) or pd.isnull(row['max_price_date']):
                return False
            
            # If min loss is worse than stop loss and it happens before max gain
            return (row['min_percent_loss'] <= -4.0) and (row['min_price_date'] < row['max_price_date'])
        
        # Apply the function to each dataset separately
        for df_profit in [profitable_10pct, profitable_20pct, profitable_40pct]:
            if not df_profit.empty:
                df_profit['stop_loss_before_gain'] = df_profit.apply(stop_loss_before_max_gain, axis=1)
                
        # Further filter to remove cases where stop loss occurs before maximum gain
        profitable_10pct = profitable_10pct[~profitable_10pct['stop_loss_before_gain']].copy() if not profitable_10pct.empty else profitable_10pct
        profitable_20pct = profitable_20pct[~profitable_20pct['stop_loss_before_gain']].copy() if not profitable_20pct.empty else profitable_20pct
        profitable_40pct = profitable_40pct[~profitable_40pct['stop_loss_before_gain']].copy() if not profitable_40pct.empty else profitable_40pct
        
        # Add actual max gain column for clarity
        for df_profit in [profitable_10pct, profitable_20pct, profitable_40pct]:
            if not df_profit.empty:
                df_profit['actual_gain_pct'] = df_profit['max_percent_gain']
        
        # Save results
        if not profitable_10pct.empty:
            profitable_10pct.sort_values('max_percent_gain', ascending=False, inplace=True)
            profitable_10pct.to_csv(output_10pct, index=False)
            print(f"Found {len(profitable_10pct)} posts with >10% gain without hitting 4% stop loss")
            print(f"Results saved to {output_10pct}")
            
            # Display top 5 posts with highest gains
            top_posts = profitable_10pct.head(5)
            print("\nTop 5 posts with >10% gain:")
            for idx, row in top_posts.iterrows():
                print(f"{row['protocol']}: {row['title']} - Gain: {row['max_percent_gain']:.2f}% after {row['days_to_max']:.1f} days")
        else:
            print("No posts found with >10% gain without hitting 4% stop loss")
        
        if not profitable_20pct.empty:
            profitable_20pct.sort_values('max_percent_gain', ascending=False, inplace=True)
            profitable_20pct.to_csv(output_20pct, index=False)
            print(f"\nFound {len(profitable_20pct)} posts with >20% gain without hitting 4% stop loss")
            print(f"Results saved to {output_20pct}")
            
            # Display top 5 posts with highest gains
            top_posts = profitable_20pct.head(5)
            print("\nTop 5 posts with >20% gain:")
            for idx, row in top_posts.iterrows():
                print(f"{row['protocol']}: {row['title']} - Gain: {row['max_percent_gain']:.2f}% after {row['days_to_max']:.1f} days")
        else:
            print("\nNo posts found with >20% gain without hitting 4% stop loss")
        
        if not profitable_40pct.empty:
            profitable_40pct.sort_values('max_percent_gain', ascending=False, inplace=True)
            profitable_40pct.to_csv(output_40pct, index=False)
            print(f"\nFound {len(profitable_40pct)} posts with >40% gain without hitting 4% stop loss")
            print(f"Results saved to {output_40pct}")
            
            # Display top 5 posts with highest gains
            top_posts = profitable_40pct.head(5)
            print("\nTop 5 posts with >40% gain:")
            for idx, row in top_posts.iterrows():
                print(f"{row['protocol']}: {row['title']} - Gain: {row['max_percent_gain']:.2f}% after {row['days_to_max']:.1f} days")
        else:
            print("\nNo posts found with >40% gain without hitting 4% stop loss")
        
        # Protocol-wise statistics for profitable posts
        if not profitable_10pct.empty:
            protocol_stats = profitable_10pct.groupby('protocol').agg({
                'max_percent_gain': ['mean', 'max', 'count'],
                'days_to_max': 'mean'
            })
            
            protocol_stats.columns = ['avg_gain_pct', 'max_gain_pct', 'post_count', 'avg_days_to_max']
            protocol_stats.sort_values('post_count', ascending=False, inplace=True)
            
            print("\nProtocol-wise statistics for profitable posts (>10% gain):")
            for protocol, stats in protocol_stats.iterrows():
                if stats['post_count'] >= 3:  # Only show protocols with at least 3 posts
                    print(f"{protocol}: {stats['post_count']} posts, Avg gain: {stats['avg_gain_pct']:.2f}%, " +
                          f"Max gain: {stats['max_gain_pct']:.2f}%, Avg days to max: {stats['avg_days_to_max']:.1f}")
        
    except Exception as e:
        print(f"Error processing the data: {e}")
        import traceback
        traceback.print_exc()
        
if __name__ == "__main__":
    find_profitable_posts() 