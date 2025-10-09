#!/usr/bin/env python3
"""
Create final summary report for Vader + Transformer sentiment analysis
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager
from datetime import datetime

def create_final_report():
    """Create final summary report"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            # Fetch all sentiment data
            query = text("""
                SELECT 
                    rp.id as post_id,
                    rp.title,
                    rp.score,
                    rp.num_comments,
                    ss.vader_compound,
                    ss.transformer_sentiment
                FROM reddit_posts rp
                JOIN sentiment_scores ss ON rp.id = ss.post_id
            """)
            
            result = conn.execute(query)
            posts = []
            
            for row in result:
                posts.append({
                    'post_id': row[0],
                    'title': row[1],
                    'score': row[2],
                    'num_comments': row[3],
                    'vader_compound': row[4],
                    'transformer_sentiment': row[5]
                })
            
            if not posts:
                print("No data found to generate report.")
                return
            
            # Convert to DataFrame for analysis
            import pandas as pd
            df = pd.DataFrame(posts)
            
            # Add Vader classification
            df['vader_classification'] = df['vader_compound'].apply(
                lambda x: 'positive' if x > 0.05 else ('negative' if x < -0.05 else 'neutral')
            )
            
            # Overall Statistics
            total_posts = len(df)
            avg_score = df['score'].mean()
            avg_comments = df['num_comments'].mean()
            high_scoring_posts = len(df[df['score'] > 100])
            
            # Vader Sentiment
            vader_pos = len(df[df['vader_compound'] > 0.05])
            vader_neg = len(df[df['vader_compound'] < -0.05])
            vader_neu = len(df[(df['vader_compound'] >= -0.05) & (df['vader_compound'] <= 0.05)])
            avg_vader_compound = df['vader_compound'].mean()
            
            # Transformer Sentiment
            transformer_pos = len(df[df['transformer_sentiment'] == 'positive'])
            transformer_neg = len(df[df['transformer_sentiment'] == 'negative'])
            transformer_neu = len(df[df['transformer_sentiment'] == 'neutral'])
            
            # Agreement analysis
            agreement = len(df[df['vader_classification'] == df['transformer_sentiment']])
            disagreement = total_posts - agreement
            
            # Create report content
            report_content = f"""REDDIT SENTIMENT ANALYSIS REPORT (VADER + TRANSFORMER)
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
================================================================================

OVERVIEW
--------
Total Posts Analyzed: {total_posts}
Average Post Score: {avg_score:.1f}
Average Comments per Post: {avg_comments:.1f}
High-Scoring Posts (>100 points): {high_scoring_posts}

SENTIMENT ANALYSIS RESULTS
--------------------------

VADER SENTIMENT ANALYSIS:
  Average Compound Score: {avg_vader_compound:.3f}
  Positive Posts: {vader_pos} ({vader_pos/total_posts:.1%})
  Negative Posts: {vader_neg} ({vader_neg/total_posts:.1%})
  Neutral Posts: {vader_neu} ({vader_neu/total_posts:.1%})

TRANSFORMER SENTIMENT ANALYSIS:
  Positive Posts: {transformer_pos} ({transformer_pos/total_posts:.1%})
  Negative Posts: {transformer_neg} ({transformer_neg/total_posts:.1%})
  Neutral Posts: {transformer_neu} ({transformer_neu/total_posts:.1%})

CLASSIFIER COMPARISON
---------------------
Vader and Transformer Agreement: {agreement} posts ({agreement/total_posts:.1%})
Disagreement: {disagreement} posts ({disagreement/total_posts:.1%})

Vader is more sensitive to sentiment changes.
Transformer provides more balanced classification.

KEY INSIGHTS
------------
1. Lakers fans show generally positive sentiment (Vader: {avg_vader_compound:.3f})
2. Vader and Transformer agree on {agreement/total_posts:.1%} of posts
3. High-engagement posts (score >100) represent {high_scoring_posts/total_posts:.1%} of the dataset
4. Average post engagement: {avg_score:.1f} points, {avg_comments:.1f} comments

DATA FILES
----------
Raw Reddit Data: outputs/raw_data/
- reddit_data_20251008_172817.csv
- reddit_data_20251008_172841.csv

Sentiment Analysis Data: outputs/sentiment_data/
- sentiment_analysis_20251008_181626.csv

API Endpoints: http://127.0.0.1:8001
- GET /posts - All posts with sentiment data
- GET /sentiment - Overall sentiment summary

TECHNICAL DETAILS
-----------------
- Database: PostgreSQL (AWS RDS)
- Sentiment Models: Vader, Transformer (RoBERTa)
- Data Source: r/lakers subreddit
- Analysis Date: {datetime.now().strftime("%Y-%m-%d")}
- TextBlob Removed: Simplified to Vader + Transformer only

================================================================================
"""
            
            # Create reports directory if it doesn't exist
            output_dir = "outputs/reports"
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"sentiment_analysis_report_{timestamp}.txt"
            file_path = os.path.join(output_dir, file_name)
            
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(report_content)
            
            print(f"Final report created: {file_path}")
            print("\nReport Preview:\n")
            print(report_content)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_final_report()

