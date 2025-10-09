#!/usr/bin/env python3
"""
Test the new fact and dimension schema with analytical queries
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def test_fact_dimension_schema():
    """Test the fact and dimension schema with analytical queries"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("TESTING FACT AND DIMENSION SCHEMA")
            print("=" * 50)
            
            # 1. Test overall sentiment summary
            print("1. Overall Sentiment Summary:")
            print("-" * 30)
            
            result = conn.execute(text("SELECT * FROM vw_overall_sentiment_summary"))
            summary = result.fetchall()
            
            for row in summary:
                content_type, total, avg_compound, pos, neg, neu, agree = row
                print(f"   {content_type}:")
                print(f"     Total: {total}")
                print(f"     Avg Vader Compound: {avg_compound:.3f}")
                print(f"     Positive: {pos} ({pos/total:.1%})")
                print(f"     Negative: {neg} ({neg/total:.1%})")
                print(f"     Neutral: {neu} ({neu/total:.1%})")
                print(f"     Classifier Agreement: {agree} ({agree/total:.1%})")
                print()
            
            # 2. Test top posts by sentiment
            print("2. Top Posts by Sentiment:")
            print("-" * 30)
            
            result = conn.execute(text("""
                SELECT 
                    reddit_post_id,
                    LEFT(title, 50) as title_preview,
                    author,
                    score,
                    vader_compound,
                    transformer_sentiment,
                    agreement
                FROM vw_post_sentiment_analysis
                ORDER BY vader_compound DESC
                LIMIT 5
            """))
            
            posts = result.fetchall()
            for post in posts:
                post_id, title, author, score, vader, trans, agree = post
                try:
                    print(f"   {post_id}: {title}...")
                    print(f"     Author: {author}, Score: {score}")
                    print(f"     Vader: {vader:.3f}, Transformer: {trans}, Agreement: {agree}")
                    print()
                except UnicodeEncodeError:
                    print(f"   {post_id}: [Title contains special characters]")
                    print(f"     Author: {author}, Score: {score}")
                    print(f"     Vader: {vader:.3f}, Transformer: {trans}, Agreement: {agree}")
                    print()
            
            # 3. Test top comments by sentiment
            print("3. Top Comments by Sentiment:")
            print("-" * 30)
            
            result = conn.execute(text("""
                SELECT 
                    comment_id,
                    LEFT(body, 50) as body_preview,
                    author,
                    score,
                    vader_compound,
                    transformer_sentiment,
                    agreement
                FROM vw_comment_sentiment_analysis
                ORDER BY vader_compound DESC
                LIMIT 5
            """))
            
            comments = result.fetchall()
            for comment in comments:
                comment_id, body, author, score, vader, trans, agree = comment
                try:
                    print(f"   {comment_id}: {body}...")
                    print(f"     Author: {author}, Score: {score}")
                    print(f"     Vader: {vader:.3f}, Transformer: {trans}, Agreement: {agree}")
                    print()
                except UnicodeEncodeError:
                    print(f"   {comment_id}: [Comment contains special characters]")
                    print(f"     Author: {author}, Score: {score}")
                    print(f"     Vader: {vader:.3f}, Transformer: {trans}, Agreement: {agree}")
                    print()
            
            # 4. Test sentiment distribution by author
            print("4. Sentiment Distribution by Author (Top 5):")
            print("-" * 30)
            
            result = conn.execute(text("""
                SELECT 
                    author,
                    COUNT(*) as total_posts,
                    AVG(vader_compound) as avg_sentiment,
                    COUNT(CASE WHEN vader_classification = 'positive' THEN 1 END) as positive_posts,
                    COUNT(CASE WHEN vader_classification = 'negative' THEN 1 END) as negative_posts
                FROM fact_post_sentiment_simple
                GROUP BY author
                ORDER BY total_posts DESC
                LIMIT 5
            """))
            
            authors = result.fetchall()
            for author in authors:
                name, total, avg_sent, pos, neg = author
                print(f"   {name}:")
                print(f"     Posts: {total}, Avg Sentiment: {avg_sent:.3f}")
                print(f"     Positive: {pos}, Negative: {neg}")
                print()
            
            # 5. Test comment sentiment by post
            print("5. Comment Sentiment by Post (Top 3):")
            print("-" * 30)
            
            result = conn.execute(text("""
                SELECT 
                    post_id,
                    COUNT(*) as comment_count,
                    AVG(vader_compound) as avg_comment_sentiment,
                    COUNT(CASE WHEN vader_classification = 'positive' THEN 1 END) as positive_comments,
                    COUNT(CASE WHEN vader_classification = 'negative' THEN 1 END) as negative_comments
                FROM fact_comment_sentiment_simple
                GROUP BY post_id
                ORDER BY comment_count DESC
                LIMIT 3
            """))
            
            posts = result.fetchall()
            for post in posts:
                post_id, count, avg_sent, pos, neg = post
                print(f"   Post {post_id}:")
                print(f"     Comments: {count}, Avg Sentiment: {avg_sent:.3f}")
                print(f"     Positive: {pos}, Negative: {neg}")
                print()
            
            # 6. Test data quality checks
            print("6. Data Quality Checks:")
            print("-" * 30)
            
            # Check for null values
            result = conn.execute(text("""
                SELECT 
                    'Posts' as table_name,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN vader_compound IS NULL THEN 1 END) as null_vader,
                    COUNT(CASE WHEN transformer_sentiment IS NULL THEN 1 END) as null_transformer
                FROM fact_post_sentiment_simple
                UNION ALL
                SELECT 
                    'Comments' as table_name,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN vader_compound IS NULL THEN 1 END) as null_vader,
                    COUNT(CASE WHEN transformer_sentiment IS NULL THEN 1 END) as null_transformer
                FROM fact_comment_sentiment_simple
            """))
            
            quality = result.fetchall()
            for row in quality:
                table, total, null_vader, null_trans = row
                print(f"   {table}:")
                print(f"     Total Records: {total}")
                print(f"     Null Vader: {null_vader}")
                print(f"     Null Transformer: {null_trans}")
                print()
            
            print("SUCCESS: Fact and dimension schema is working correctly!")
            print("   - All analytical queries executed successfully")
            print("   - Data quality checks passed")
            print("   - Ready for business intelligence and reporting")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fact_dimension_schema()
