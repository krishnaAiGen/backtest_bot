import pandas as pd
import os
import json
import re
import time
import ast
from typing import Tuple, Optional
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

class SentimentAnalyzer:
    def __init__(self):
        self.max_attempts = 3
        self.retry_delay = 1  # seconds between retries
        
        # Get API credentials from environment variables
        agent_endpoint = os.getenv("AGENT_ENDPOINT")
        agent_key = os.getenv("AGENT_KEY")
        
        if not agent_endpoint or not agent_key:
            raise ValueError("Missing Deepseek API configuration. Please check your .env file.")
        
        # Initialize Deepseek client
        self.client = OpenAI(
            base_url=agent_endpoint,
            api_key=agent_key,
        )
    
    def get_deepseek_sentiment(self, description: str) -> Tuple[Optional[str], Optional[float]]:
        """
        Get sentiment score from Deepseek model with retry logic.
        Returns a tuple of (sentiment, score) or (None, None) if the sentiment couldn't be retrieved.
        """
        max_retries = self.max_attempts
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Prepare a reasonably sized input (truncate if too long)
                # trimmed_description = description[:4000] if len(description) > 4000 else description
                trimmed_description = description
                
                response = self.client.chat.completions.create(
                    model="n/a",
                    messages=[
                        {"role": "system", "content": """
                         You are a financial and trading expert. Based on the content of this text, evaluate its sentiment and immediate impact on market prices.
                         Output your result in JSON format as {'positive': x} or {'negative': x}, where:
                         - x represents the score that can be in between 0 to 1.
                         Output only the JSON object.
                         """},
                        {"role": "user", "content": trimmed_description}
                    ]
                )
                
                for choice in response.choices:
                    content = choice.message.content
                    # Find JSON pattern between curly braces, including the braces
                    json_match = re.search(r'\{[^{}]*\}', content)
                    if json_match:
                        json_str = json_match.group()
                        json_str_fixed = json_str.replace("'", '"')
                        try:
                            result = json.loads(json_str_fixed)  # Parse JSON string to dict
                            sentiment, score = next(iter(result.items()))
                            return sentiment, float(score)
                        except json.JSONDecodeError:
                            # If first attempt fails, try with ast.literal_eval
                            try:
                                result = ast.literal_eval(json_str)
                                sentiment, score = next(iter(result.items()))
                                return sentiment, float(score)
                            except (ValueError, SyntaxError):
                                # Continue to next retry if both parsing methods fail
                                pass
                
                # If we didn't find JSON in the response, increment retry counter
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(self.retry_delay)  # Add a small delay between retries
                    continue
                else:
                    return None, None
                    
            except Exception as e:
                print(f"Error getting sentiment: {e}")
                retry_count += 1
                if retry_count == max_retries:
                    return None, None
                time.sleep(self.retry_delay)  # Add a small delay between retries
                continue

def process_file(input_file, output_file, analyzer):
    """Process a CSV file, add sentiment scores, and save to a new file."""
    print(f"Processing {input_file}...")
    
    try:
        # Load the CSV file
        df = pd.read_csv(input_file)
        total_rows = len(df)
        
        if total_rows == 0:
            print(f"No data found in {input_file}. Skipping.")
            return
        
        # Initialize new columns
        df['sentiment'] = None
        df['sentiment_score'] = None
        df['sentiment_score_normalized'] = None  # Normalized to between 0-1 for trading signals
        
        # Process each row
        print("Total rows: ", total_rows)
        for idx, row in df.iterrows():
            print(f"Processing post {idx+1}/{total_rows}")
            description = row['description']
            title = row['title']
            
            # Combine title and description for better context
            full_text = f"Title: {title}\n\nDescription: {description}"
            
            # Get sentiment from Deepseek
            print(f"Analyzing post {idx+1}/{total_rows}: {title[:50]}...")
            sentiment, score = analyzer.get_deepseek_sentiment(full_text)
            
            # Store results
            df.at[idx, 'sentiment'] = sentiment
            df.at[idx, 'sentiment_score'] = score
            
            # Normalize score for trading signals (positive -> 0.5-1.0, negative -> 0.0-0.5)
            if sentiment == 'positive' and score is not None:
                df.at[idx, 'sentiment_score_normalized'] = 0.5 + (score * 0.5)
            elif sentiment == 'negative' and score is not None:
                df.at[idx, 'sentiment_score_normalized'] = 0.5 - (score * 0.5)
            else:
                df.at[idx, 'sentiment_score_normalized'] = 0.5  # Neutral if no sentiment
        
        # Save to the output file
        df.to_csv(output_file, index=False)
        print(f"Saved results to {output_file}")
        
        # Print some statistics
        sentiment_counts = df['sentiment'].value_counts()
        print("\nSentiment distribution:")
        for sentiment, count in sentiment_counts.items():
            print(f"{sentiment}: {count} posts ({count/total_rows*100:.1f}%)")
        
        # Calculate average scores
        avg_positive = df[df['sentiment'] == 'positive']['sentiment_score'].mean()
        avg_negative = df[df['sentiment'] == 'negative']['sentiment_score'].mean()
        
        print(f"\nAverage positive score: {avg_positive:.2f}")
        print(f"Average negative score: {avg_negative:.2f}")
        
        # Correlation with max gain
        correlation = df['sentiment_score_normalized'].corr(df['max_percent_gain'])
        print(f"\nCorrelation between sentiment score and maximum gain: {correlation:.2f}")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()

def main():
    # Check if .env file exists and API keys are set
    if not os.path.exists('.env'):
        print("Warning: .env file not found. Please create one with AGENT_ENDPOINT and AGENT_KEY.")
        return
    
    # Input and output files
    input_files = [
        "controlled_risk_posts_10pct.csv",
    ]
    
    output_files = [
        "controlled_risk_posts_10pct_with_sentiment.csv",
    ]
    
    # Initialize the sentiment analyzer
    try:
        analyzer = SentimentAnalyzer()
        print("Sentiment analyzer initialized successfully.")
    except ValueError as e:
        print(f"Error initializing sentiment analyzer: {e}")
        return
    
    # Process each file
    for input_file, output_file in zip(input_files, output_files):
        if os.path.exists(input_file):
            process_file(input_file, output_file, analyzer)
        else:
            print(f"File {input_file} not found. Skipping.")
    
    print("\nSentiment analysis completed for all files.")

if __name__ == "__main__":
    main() 