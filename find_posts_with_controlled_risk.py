import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta

def find_posts_with_controlled_risk():
    """
    Identify posts that led to significant price increases with controlled downside risk.
    
    Specifically, find posts where:
    1. Maximum gain within 5 days reaches >10%, >20%, or >40%
    2. Minimum price during the 5-day period never drops below -5% (controlled risk)
    """
    # File paths
    input_file = "ai_posts_2024_with_price_impact.csv"
    output_10pct = "controlled_risk_posts_10pct.csv"
    output_20pct = "controlled_risk_posts_20pct.csv"
    output_40pct = "controlled_risk_posts_40pct.csv"
    
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
        
        # Filter conditions:
        # 1. Minimum loss must not be worse than -5% (controlled risk)
        # 2. Maximum gain must be above the threshold
        df['controlled_risk'] = df['min_percent_loss'] >= -5.0
        
        # Create categories for different gain thresholds with controlled risk
        df['gain_above_10pct_with_risk_control'] = (df['max_percent_gain'] > 10) & df['controlled_risk']
        df['gain_above_20pct_with_risk_control'] = (df['max_percent_gain'] > 20) & df['controlled_risk']
        df['gain_above_40pct_with_risk_control'] = (df['max_percent_gain'] > 40) & df['controlled_risk']
        
        # Filter for each category
        controlled_10pct = df[df['gain_above_10pct_with_risk_control']].copy()
        controlled_20pct = df[df['gain_above_20pct_with_risk_control']].copy()
        controlled_40pct = df[df['gain_above_40pct_with_risk_control']].copy()
        
        # Add a column for the gain-to-risk ratio (maximum gain / absolute minimum loss)
        # This helps identify posts with the best risk-reward profiles
        def calculate_gain_risk_ratio(row):
            if row['min_percent_loss'] >= 0:  # No drawdown
                return float('inf')  # Return infinity for no drawdown
            else:
                return abs(row['max_percent_gain'] / row['min_percent_loss'])
                
        for df_risk in [controlled_10pct, controlled_20pct, controlled_40pct]:
            if not df_risk.empty:
                df_risk['gain_risk_ratio'] = df_risk.apply(calculate_gain_risk_ratio, axis=1)
        
        # Save results
        if not controlled_10pct.empty:
            # Sort by gain-to-risk ratio (best risk-reward first)
            controlled_10pct.sort_values('gain_risk_ratio', ascending=False, inplace=True)
            controlled_10pct.to_csv(output_10pct, index=False)
            print(f"\nFound {len(controlled_10pct)} posts with >10% gain and minimum loss above -5%")
            print(f"Results saved to {output_10pct}")
            
            # Display top 5 posts with best gain-to-risk ratio
            top_posts = controlled_10pct.head(5)
            print("\nTop 5 posts with >10% gain and controlled risk (best gain-to-risk ratio):")
            for idx, row in top_posts.iterrows():
                ratio_str = "∞" if row['gain_risk_ratio'] == float('inf') else f"{row['gain_risk_ratio']:.2f}"
                print(f"{row['protocol']}: {row['title']} - " +
                     f"Gain: {row['max_percent_gain']:.2f}%, Min: {row['min_percent_loss']:.2f}%, " +
                     f"Ratio: {ratio_str}")
        else:
            print("\nNo posts found with >10% gain and minimum loss above -5%")
        
        if not controlled_20pct.empty:
            controlled_20pct.sort_values('gain_risk_ratio', ascending=False, inplace=True)
            controlled_20pct.to_csv(output_20pct, index=False)
            print(f"\nFound {len(controlled_20pct)} posts with >20% gain and minimum loss above -5%")
            print(f"Results saved to {output_20pct}")
            
            # Display top 5 posts with best gain-to-risk ratio
            top_posts = controlled_20pct.head(5)
            print("\nTop 5 posts with >20% gain and controlled risk (best gain-to-risk ratio):")
            for idx, row in top_posts.iterrows():
                ratio_str = "∞" if row['gain_risk_ratio'] == float('inf') else f"{row['gain_risk_ratio']:.2f}"
                print(f"{row['protocol']}: {row['title']} - " +
                     f"Gain: {row['max_percent_gain']:.2f}%, Min: {row['min_percent_loss']:.2f}%, " +
                     f"Ratio: {ratio_str}")
        else:
            print("\nNo posts found with >20% gain and minimum loss above -5%")
        
        if not controlled_40pct.empty:
            controlled_40pct.sort_values('gain_risk_ratio', ascending=False, inplace=True)
            controlled_40pct.to_csv(output_40pct, index=False)
            print(f"\nFound {len(controlled_40pct)} posts with >40% gain and minimum loss above -5%")
            print(f"Results saved to {output_40pct}")
            
            # Display top 5 posts with best gain-to-risk ratio
            top_posts = controlled_40pct.head(5)
            print("\nTop 5 posts with >40% gain and controlled risk (best gain-to-risk ratio):")
            for idx, row in top_posts.iterrows():
                ratio_str = "∞" if row['gain_risk_ratio'] == float('inf') else f"{row['gain_risk_ratio']:.2f}"
                print(f"{row['protocol']}: {row['title']} - " +
                     f"Gain: {row['max_percent_gain']:.2f}%, Min: {row['min_percent_loss']:.2f}%, " +
                     f"Ratio: {ratio_str}")
        else:
            print("\nNo posts found with >40% gain and minimum loss above -5%")
        
        # Protocol-wise statistics for controlled risk posts
        if not controlled_10pct.empty:
            protocol_stats = controlled_10pct.groupby('protocol').agg({
                'max_percent_gain': ['mean', 'max', 'count'],
                'min_percent_loss': 'mean',
                'days_to_max': 'mean',
                'gain_risk_ratio': 'mean'
            })
            
            protocol_stats.columns = ['avg_gain_pct', 'max_gain_pct', 'post_count', 
                                      'avg_min_loss_pct', 'avg_days_to_max', 'avg_gain_risk_ratio']
            protocol_stats.sort_values('post_count', ascending=False, inplace=True)
            
            print("\nProtocol-wise statistics for controlled risk posts (>10% gain, min loss > -5%):")
            for protocol, stats in protocol_stats.iterrows():
                if stats['post_count'] >= 3:  # Only show protocols with at least 3 posts
                    print(f"{protocol}: {stats['post_count']} posts, " +
                          f"Avg gain: {stats['avg_gain_pct']:.2f}%, " +
                          f"Avg min loss: {stats['avg_min_loss_pct']:.2f}%, " +
                          f"Avg gain-risk ratio: {stats['avg_gain_risk_ratio']:.2f}")
        
        # Calculate overall trading performance metrics
        if not controlled_10pct.empty:
            avg_gain = controlled_10pct['max_percent_gain'].mean()
            avg_min = controlled_10pct['min_percent_loss'].mean()
            
            print("\nOverall trading metrics for controlled risk approach:")
            print(f"Average maximum gain: {avg_gain:.2f}%")
            print(f"Average minimum loss: {avg_min:.2f}%")
            print(f"Overall gain-to-risk ratio: {abs(avg_gain / avg_min) if avg_min < 0 else '∞'}")
            
            # Count by gain range
            gain_ranges = {
                "10-15%": len(controlled_10pct[(controlled_10pct['max_percent_gain'] >= 10) & (controlled_10pct['max_percent_gain'] < 15)]),
                "15-20%": len(controlled_10pct[(controlled_10pct['max_percent_gain'] >= 15) & (controlled_10pct['max_percent_gain'] < 20)]),
                "20-30%": len(controlled_10pct[(controlled_10pct['max_percent_gain'] >= 20) & (controlled_10pct['max_percent_gain'] < 30)]),
                "30%+": len(controlled_10pct[controlled_10pct['max_percent_gain'] >= 30])
            }
            
            print("\nGain distribution:")
            for range_name, count in gain_ranges.items():
                print(f"{range_name}: {count} posts ({count/len(controlled_10pct)*100:.1f}%)")
    
    except Exception as e:
        print(f"Error processing the data: {e}")
        import traceback
        traceback.print_exc()
        
if __name__ == "__main__":
    find_posts_with_controlled_risk()