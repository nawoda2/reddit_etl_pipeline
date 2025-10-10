#!/usr/bin/env python3
"""
Test enhanced sentiment analysis with title+selftext and comment collection
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_collectors.reddit_collector import connect_reddit, extract_posts_with_comments, transform_data
from data_processors.sentiment_analyzer import analyze_reddit_sentiment_enhanced
from data_processors.database_manager import DatabaseManager
from utils.constants import CLIENT_ID, SECRET, USER_AGENT

def test_enhanced_analysis():
    """Test enhanced sentiment analysis and comment collection"""
    
    print("TESTING ENHANCED SENTIMENT ANALYSIS")
    print("=" * 50)
    
    try:
        # Connect to Reddit
        print("1. Connecting to Reddit...")
        reddit = connect_reddit(CLIENT_ID, SECRET, USER_AGENT)
        
        # Extract posts with comments (small sample for testing)
        print("\n2. Extracting posts with comments...")
        posts_list, comments_list = extract_posts_with_comments(
            reddit, 
            'lakers', 
            'day', 
            limit=5,  # Small sample for testing
            comment_limit=20  # Limit comments per post
        )
        
        print(f"   Posts extracted: {len(posts_list)}")
        print(f"   Comments extracted: {len(comments_list)}")
        
        # Transform posts data
        print("\n3. Transforming posts data...")
        posts_df = pd.DataFrame(posts_list)
        posts_df = transform_data(posts_df)
        
        # Show sample of posts with selftext
        posts_with_selftext = posts_df[posts_df['selftext'].str.len() > 10]
        print(f"   Posts with meaningful selftext: {len(posts_with_selftext)}")
        
        if len(posts_with_selftext) > 0:
            print("   Sample posts with selftext:")
            for i, (_, post) in enumerate(posts_with_selftext.head(2).iterrows()):
                print(f"     {i+1}. Title: {post['title'][:50]}...")
                print(f"        Selftext: {post['selftext'][:100]}...")
                print(f"        Score: {post['score']}")
                print()
        
        # Test enhanced sentiment analysis
        print("4. Running enhanced sentiment analysis...")
        processed_df, summary = analyze_reddit_sentiment_enhanced(posts_df)
        
        print(f"   Total posts analyzed: {summary['total_posts']}")
        print(f"   Posts with selftext: {summary['posts_with_selftext']}")
        print(f"   Posts title only: {summary['posts_title_only']}")
        print(f"   Average sentiment: {summary['average_sentiment']:.3f}")
        print(f"   Positive posts: {summary['positive_posts']}")
        print(f"   Negative posts: {summary['negative_posts']}")
        print(f"   Neutral posts: {summary['neutral_posts']}")
        
        # Show sample results
        if len(processed_df) > 0:
            print("\n   Sample analysis results:")
            sample_cols = ['title', 'combined_text', 'vader_compound', 'transformer_sentiment']
            try:
                print(processed_df[sample_cols].head(3).to_string(index=False))
            except UnicodeEncodeError:
                print("   (Sample data contains special characters - skipping display)")
        
        # Test comment collection
        print("\n5. Testing comment collection...")
        if comments_list:
            comments_df = pd.DataFrame(comments_list)
            print(f"   Comments collected: {len(comments_df)}")
            print(f"   Unique posts with comments: {comments_df['post_id'].nunique()}")
            
            # Show sample comments
            print("   Sample comments:")
            for i, (_, comment) in enumerate(comments_df.head(3).iterrows()):
                print(f"     {i+1}. Post: {comment['post_id']}")
                print(f"        Author: {comment['author']}")
                print(f"        Body: {comment['body'][:100]}...")
                print(f"        Score: {comment['score']}")
                print()
        
        # Test database operations
        print("\n6. Testing database operations...")
        db_manager = DatabaseManager()
        
        # Test inserting comments
        if comments_list:
            comments_df = pd.DataFrame(comments_list)
            comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'], unit='s')
            
            success = db_manager.insert_reddit_comments(comments_df)
            if success:
                print("   SUCCESS: Comments inserted into database")
            else:
                print("   ERROR: Failed to insert comments")
        
        # Test retrieving comments for a post
        if posts_list:
            test_post_id = posts_list[0]['id']
            retrieved_comments = db_manager.get_comments_for_post(test_post_id)
            print(f"   Retrieved {len(retrieved_comments)} comments for post {test_post_id}")
        
        print("\nSUCCESS: Enhanced analysis test completed successfully!")
        
    except Exception as e:
        print(f"ERROR: Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_analysis()
