import pandas as pd
import os

def filter_high_sentiment_posts():
    """
    Filters posts from the controlled_risk_posts_10pct_with_sentiment.csv file
    that have a sentiment_score_normalized greater than 0.8.
    Saves the filtered posts to a new file.
    """
    input_file = "controlled_risk_posts_10pct_with_sentiment.csv"
    output_file = "high_sentiment_posts.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Please run the sentiment analysis first.")
        return
        
    try:
        # Load the CSV file
        print(f"Loading data from {input_file}...")
        df = pd.read_csv(input_file)
        total_rows = len(df)
        
        if total_rows == 0:
            print(f"No data found in {input_file}.")
            return
            
        # Check if sentiment column exists
        if 'sentiment_score' not in df.columns:
            print(f"Error: 'sentiment_score' column not found in {input_file}.")
            return
            
        # Filter posts with sentiment_score > 0.8
        filtered_df = df[df['sentiment_score'] > 0.8]
        filtered_rows = len(filtered_df)
        
        if filtered_rows == 0:
            print("No posts found with sentiment score > 0.8.")
            return
            
        # Add new column for sentiment-gain correlation product
        filtered_df['sentiment_gain_product'] = filtered_df['sentiment_score'] * filtered_df['max_percent_gain']
        
        # Calculate z-scores for sentiment and max gain
        filtered_df['sentiment_z_score'] = (filtered_df['sentiment_score'] - filtered_df['sentiment_score'].mean()) / filtered_df['sentiment_score'].std()
        filtered_df['gain_z_score'] = (filtered_df['max_percent_gain'] - filtered_df['max_percent_gain'].mean()) / filtered_df['max_percent_gain'].std()
        
        # Calculate a combined score based on both metrics
        filtered_df['combined_score'] = (filtered_df['sentiment_z_score'] + filtered_df['gain_z_score']) / 2
            
        # Save the filtered posts to a new file
        filtered_df.to_csv(output_file, index=False)
        print(f"Saved {filtered_rows} high sentiment posts to {output_file}")
        print(f"Found {filtered_rows} out of {total_rows} posts with sentiment score > 0.8 ({filtered_rows/total_rows*100:.2f}%)")
        
        # Calculate statistics for the filtered posts
        avg_sentiment = filtered_df['sentiment_score'].mean()
        avg_max_gain = filtered_df['max_percent_gain'].mean()
        avg_product = filtered_df['sentiment_gain_product'].mean()
        
        print("\nStatistics for high sentiment posts:")
        print(f"Average sentiment score: {avg_sentiment:.4f}")
        print(f"Average maximum gain: {avg_max_gain:.2f}%")
        print(f"Average sentiment-gain product: {avg_product:.2f}")
        
        # Correlation between sentiment and max gain for filtered posts
        correlation = filtered_df['sentiment_score'].corr(filtered_df['max_percent_gain'])
        print(f"Correlation between sentiment score and maximum gain: {correlation:.4f}")
        
        # Group by protocol and calculate statistics
        protocol_stats = filtered_df.groupby('protocol').agg({
            'sentiment_score': 'mean',
            'max_percent_gain': 'mean',
            'sentiment_gain_product': 'mean',
            'protocol': 'count'
        }).rename(columns={'protocol': 'count'}).sort_values('count', ascending=False)
        
        print("\nProtocol-wise statistics for high sentiment posts:")
        print(protocol_stats)
        
        # Print top 10 posts with highest combined scores
        print("\nTop 10 posts with highest combined sentiment and gain scores:")
        top_combined_posts = filtered_df.sort_values('combined_score', ascending=False).head(10)
        for idx, row in top_combined_posts.iterrows():
            post_id = row.get('id', 'N/A')
            print(f"ID: {post_id} | {row['protocol']}: Sentiment: {row['sentiment_score']:.2f}, Max Gain: {row['max_percent_gain']:.2f}%, Product: {row['sentiment_gain_product']:.2f}, Title: {row['title'][:70]}...")
            
        # Print top 10 posts with highest sentiment-gain products
        print("\nTop 10 posts with highest sentiment-gain products:")
        top_product_posts = filtered_df.sort_values('sentiment_gain_product', ascending=False).head(10)
        for idx, row in top_product_posts.iterrows():
            post_id = row.get('post_id', 'N/A')
            print(f"ID: {post_id} | {row['protocol']}: Sentiment: {row['sentiment_score']:.2f}, Max Gain: {row['max_percent_gain']:.2f}%, Product: {row['sentiment_gain_product']:.2f}, Title: {row['title'][:70]}...")
        
        # Add additional breakdowns
        print("\nTop 5 posts with highest sentiment scores:")
        top_sentiment_posts = filtered_df.sort_values('sentiment_score', ascending=False).head(5)
        for idx, row in top_sentiment_posts.iterrows():
            post_id = row.get('id', 'N/A')
            print(f"ID: {post_id} | {row['protocol']}: Sentiment: {row['sentiment_score']:.2f}, Max Gain: {row['max_percent_gain']:.2f}%, Title: {row['title'][:70]}...")
        
        print("\nTop 5 posts with highest maximum gains:")
        top_gain_posts = filtered_df.sort_values('max_percent_gain', ascending=False).head(5)
        for idx, row in top_gain_posts.iterrows():
            post_id = row.get('id', 'N/A')
            print(f"ID: {post_id} | {row['protocol']}: Sentiment: {row['sentiment_score']:.2f}, Max Gain: {row['max_percent_gain']:.2f}%, Title: {row['title'][:70]}...")
            
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    filter_high_sentiment_posts() 