#!/usr/bin/env python3
"""
Analyze sentiment of Reddit comments and export to CSV
"""

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager
from data_processors.sentiment_analyzer import LakersSentimentAnalyzer

def analyze_comment_sentiment():
    """Analyze sentiment of comments and export to CSV"""
    
    db_manager = DatabaseManager()
    analyzer = LakersSentimentAnalyzer()
    
    try:
        with db_manager.engine.connect() as conn:
            print("ANALYZING COMMENT SENTIMENT")
            print("=" * 50)
            
            # Get all comments
            comments_query = text("""
                SELECT 
                    rc.id,
                    rc.post_id,
                    rc.author,
                    rc.body,
                    rc.score,
                    rc.created_utc,
                    rc.parent_id,
                    rc.is_submitter,
                    rc.edited,
                    rp.title as post_title
                FROM reddit_comments rc
                JOIN reddit_posts rp ON rc.post_id = rp.id
                ORDER BY rc.score DESC
            """)
            
            result = conn.execute(comments_query)
            comments_data = []
            
            for row in result:
                comments_data.append({
                    'comment_id': row[0],
                    'post_id': row[1],
                    'author': row[2],
                    'body': row[3],
                    'score': row[4],
                    'created_utc': row[5],
                    'parent_id': row[6],
                    'is_submitter': row[7],
                    'edited': row[8],
                    'post_title': row[9]
                })
            
            print(f"Found {len(comments_data)} comments to analyze")
            
            if len(comments_data) == 0:
                print("No comments found!")
                return
            
            # Convert to DataFrame
            comments_df = pd.DataFrame(comments_data)
            
            # Filter out very short comments
            comments_df = comments_df[comments_df['body'].str.len() > 10]
            print(f"Analyzing {len(comments_df)} comments with meaningful content")
            
            # Analyze sentiment for each comment
            print("Running sentiment analysis on comments...")
            analyzed_comments = []
            
            for i, (_, comment) in enumerate(comments_df.iterrows()):
                if i % 20 == 0:
                    print(f"  Processing comment {i+1}/{len(comments_df)}")
                
                try:
                    # Analyze the comment body
                    sentiment_result = analyzer.analyze_sentiment_comprehensive(comment['body'])
                    
                    analyzed_comment = {
                        'comment_id': comment['comment_id'],
                        'post_id': comment['post_id'],
                        'post_title': comment['post_title'],
                        'author': comment['author'],
                        'body': comment['body'],
                        'score': comment['score'],
                        'created_utc': comment['created_utc'],
                        'parent_id': comment['parent_id'],
                        'is_submitter': comment['is_submitter'],
                        'edited': comment['edited'],
                        'vader_positive': sentiment_result.get('vader_positive', 0.0),
                        'vader_negative': sentiment_result.get('vader_negative', 0.0),
                        'vader_neutral': sentiment_result.get('vader_neutral', 1.0),
                        'vader_compound': sentiment_result.get('vader_compound', 0.0),
                        'transformer_positive': sentiment_result.get('transformer_positive', 0.0),
                        'transformer_negative': sentiment_result.get('transformer_negative', 0.0),
                        'transformer_neutral': sentiment_result.get('transformer_neutral', 1.0),
                        'transformer_sentiment': sentiment_result.get('transformer_sentiment', 'neutral'),
                        'mentioned_players': ', '.join(sentiment_result.get('mentioned_players', [])),
                        'analysis_timestamp': sentiment_result.get('analysis_timestamp', datetime.now().isoformat())
                    }
                    
                    analyzed_comments.append(analyzed_comment)
                    
                except Exception as e:
                    print(f"    Error analyzing comment {comment['comment_id']}: {e}")
                    continue
            
            print(f"Successfully analyzed {len(analyzed_comments)} comments")
            
            # Convert to DataFrame
            analyzed_df = pd.DataFrame(analyzed_comments)
            
            # Add derived columns
            analyzed_df['vader_classification'] = analyzed_df['vader_compound'].apply(
                lambda x: 'positive' if x > 0.05 else ('negative' if x < -0.05 else 'neutral')
            )
            
            analyzed_df['agreement'] = analyzed_df.apply(lambda row: 
                'agree' if row['vader_classification'] == row['transformer_sentiment'] else 'disagree', axis=1
            )
            
            # Create outputs directory if it doesn't exist
            output_dir = "outputs/sentiment_data"
            os.makedirs(output_dir, exist_ok=True)
            
            # Export to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"comment_sentiment_analysis_{timestamp}.csv"
            file_path = os.path.join(output_dir, file_name)
            
            analyzed_df.to_csv(file_path, index=False)
            
            print(f"Comment sentiment analysis exported to: {file_path}")
            print(f"Total comments analyzed: {len(analyzed_df)}")
            
            # Show summary statistics
            print("\nCOMMENT SENTIMENT SUMMARY:")
            print("-" * 30)
            print(f"Total comments: {len(analyzed_df)}")
            print(f"Average Vader compound: {analyzed_df['vader_compound'].mean():.3f}")
            print(f"Positive comments (Vader): {len(analyzed_df[analyzed_df['vader_compound'] > 0.05])}")
            print(f"Negative comments (Vader): {len(analyzed_df[analyzed_df['vader_compound'] < -0.05])}")
            print(f"Neutral comments (Vader): {len(analyzed_df[(analyzed_df['vader_compound'] >= -0.05) & (analyzed_df['vader_compound'] <= 0.05)])}")
            print(f"Positive comments (Transformer): {len(analyzed_df[analyzed_df['transformer_sentiment'] == 'positive'])}")
            print(f"Negative comments (Transformer): {len(analyzed_df[analyzed_df['transformer_sentiment'] == 'negative'])}")
            print(f"Neutral comments (Transformer): {len(analyzed_df[analyzed_df['transformer_sentiment'] == 'neutral'])}")
            print(f"Agreement between classifiers: {len(analyzed_df[analyzed_df['agreement'] == 'agree'])}")
            
            # Show sample results
            print("\nSAMPLE COMMENT ANALYSIS:")
            print("-" * 30)
            sample_cols = ['comment_id', 'post_title', 'vader_compound', 'transformer_sentiment', 'agreement']
            try:
                print(analyzed_df[sample_cols].head(5).to_string(index=False))
            except UnicodeEncodeError:
                print("(Sample data contains special characters - skipping display)")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_comment_sentiment()

