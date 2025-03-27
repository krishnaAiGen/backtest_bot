import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def visualize_price_impact():
    # File path
    input_file = "ai_posts_2024_with_price_impact.csv"
    
    print(f"Loading data from {input_file}...")
    try:
        # Load the data
        df = pd.read_csv(input_file)
        
        # Drop rows without price impact data
        df = df.dropna(subset=['percent_increase'])
        print(f"Analyzing {len(df)} posts with price impact data")
        
        # Set up the plotting style
        sns.set(style="whitegrid")
        plt.figure(figsize=(14, 8))
        
        # 1. Box plot of price impact by protocol (for protocols with at least 5 posts)
        protocol_counts = df['protocol'].value_counts()
        protocols_to_include = protocol_counts[protocol_counts >= 5].index.tolist()
        filtered_df = df[df['protocol'].isin(protocols_to_include)]
        
        # Sort protocols by median price impact
        protocol_medians = filtered_df.groupby('protocol')['percent_increase'].median().sort_values(ascending=False)
        protocols_ordered = protocol_medians.index.tolist()
        
        plt.figure(figsize=(14, 8))
        ax = sns.boxplot(x='protocol', y='percent_increase', data=filtered_df, 
                         order=protocols_ordered, palette='viridis')
        plt.title('Distribution of Price Impact 5 Days After Posts by Protocol', fontsize=16)
        plt.xlabel('Protocol', fontsize=14)
        plt.ylabel('Percent Increase (%)', fontsize=14)
        plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('price_impact_boxplot.png')
        plt.close()
        
        # 2. Histogram of overall price impact distribution
        plt.figure(figsize=(12, 6))
        sns.histplot(df['percent_increase'], bins=50, kde=True)
        plt.title('Distribution of Price Impacts 5 Days After Posts', fontsize=16)
        plt.xlabel('Percent Increase (%)', fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.axvline(x=0, color='r', linestyle='-', alpha=0.3)
        plt.axvline(x=df['percent_increase'].mean(), color='g', linestyle='--', 
                   label=f'Mean: {df["percent_increase"].mean():.2f}%')
        plt.axvline(x=df['percent_increase'].median(), color='b', linestyle='--', 
                   label=f'Median: {df["percent_increase"].median():.2f}%')
        plt.legend()
        plt.tight_layout()
        plt.savefig('price_impact_histogram.png')
        plt.close()
        
        # 3. Timeline of price impacts
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        plt.figure(figsize=(16, 8))
        for protocol in protocols_ordered[:5]:  # Top 5 protocols by median impact
            protocol_df = df[df['protocol'] == protocol]
            plt.scatter(protocol_df['timestamp'], protocol_df['percent_increase'], 
                       label=protocol, alpha=0.7, s=50)
        
        plt.title('Timeline of Price Impacts for Top 5 Protocols', fontsize=16)
        plt.xlabel('Date', fontsize=14)
        plt.ylabel('Percent Increase (%)', fontsize=14)
        plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('price_impact_timeline.png')
        plt.close()
        
        # 4. Create a summary table
        summary = df.groupby('protocol')['percent_increase'].agg(['mean', 'median', 'std', 'count'])
        summary = summary.sort_values('median', ascending=False)
        summary.columns = ['Mean (%)', 'Median (%)', 'Std Dev (%)', 'Count']
        summary = summary[summary['Count'] >= 5]  # Only include protocols with at least 5 posts
        
        # Format the summary table
        summary['Mean (%)'] = summary['Mean (%)'].round(2)
        summary['Median (%)'] = summary['Median (%)'].round(2)
        summary['Std Dev (%)'] = summary['Std Dev (%)'].round(2)
        
        # Add percentage of positive and negative impacts
        positive_counts = df[df['percent_increase'] > 0].groupby('protocol').size()
        summary['Positive (%)'] = ((positive_counts / summary['Count']) * 100).round(2)
        summary['Negative (%)'] = (100 - summary['Positive (%)']).round(2)
        
        # Save to CSV
        summary.to_csv('protocol_price_impact_summary.csv')
        
        print("Visualizations created and saved as PNG files")
        print("Summary table saved as protocol_price_impact_summary.csv")
        
    except Exception as e:
        print(f"Error processing the data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    visualize_price_impact() 