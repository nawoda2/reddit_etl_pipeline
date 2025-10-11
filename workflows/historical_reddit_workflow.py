"""
Historical Reddit Data Collection for Lakers Sentiment Analysis
Pulls Reddit data from 2023-24 Lakers season for comprehensive analysis
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collectors.reddit_collector import connect_reddit, extract_posts_with_comments, transform_data
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalRedditCollector:
    """
    Historical Reddit data collector for 2023-24 Lakers season
    Handles API limitations by pulling data in monthly chunks
    """
    
    def __init__(self):
        self.reddit_instance = None
        self.subreddit = 'lakers'
        self._connect()
        
    def _connect(self):
        """Establish Reddit API connection"""
        try:
            self.reddit_instance = connect_reddit(CLIENT_ID, SECRET, 'lakers_sentiment_analysis')
            logger.info("Reddit API connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Reddit API: {e}")
            raise
    
    def collect_season_data(self, season: str = "2023-24") -> Dict[str, Any]:
        """
        Collect Reddit data for entire 2023-24 Lakers season
        
        Args:
            season: NBA season (e.g., "2023-24")
            
        Returns:
            Dictionary with collection results and file paths
        """
        try:
            logger.info(f"Starting historical Reddit data collection for {season} season")
            
            # Define season date range (Oct 2023 - May 2024)
            season_start = datetime(2023, 10, 1)
            season_end = datetime(2024, 5, 31)
            
            # Create output directory for historical data
            historical_dir = f"{OUTPUT_PATH}/historical/{season}"
            os.makedirs(historical_dir, exist_ok=True)
            
            all_posts = []
            all_comments = []
            collection_stats = {
                'total_posts': 0,
                'total_comments': 0,
                'monthly_breakdown': {},
                'files_created': []
            }
            
            # Collect data month by month to work around API limitations
            current_date = season_start
            month_count = 0
            
            while current_date <= season_end:
                month_count += 1
                month_name = current_date.strftime('%Y-%m')
                logger.info(f"Collecting data for {month_name} ({month_count}/8 months)")
                
                # Collect monthly data
                monthly_posts, monthly_comments = self._collect_monthly_data(
                    current_date, month_name, historical_dir
                )
                
                all_posts.extend(monthly_posts)
                all_comments.extend(monthly_comments)
                
                collection_stats['monthly_breakdown'][month_name] = {
                    'posts': len(monthly_posts),
                    'comments': len(monthly_comments)
                }
                
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
                
                # Add delay to respect API rate limits
                import time
                time.sleep(2)
            
            # Combine all data
            if all_posts:
                posts_df = pd.DataFrame(all_posts)
                posts_df = transform_data(posts_df)
                
                # Save combined posts data
                posts_file = f"{historical_dir}/lakers_posts_{season}_combined.csv"
                posts_df.to_csv(posts_file, index=False)
                collection_stats['files_created'].append(posts_file)
                
                logger.info(f"Saved {len(posts_df)} posts to {posts_file}")
            
            if all_comments:
                comments_df = pd.DataFrame(all_comments)
                comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'], unit='s')
                
                # Save combined comments data
                comments_file = f"{historical_dir}/lakers_comments_{season}_combined.csv"
                comments_df.to_csv(comments_file, index=False)
                collection_stats['files_created'].append(comments_file)
                
                logger.info(f"Saved {len(comments_df)} comments to {comments_file}")
            
            collection_stats['total_posts'] = len(all_posts)
            collection_stats['total_comments'] = len(all_comments)
            
            logger.info(f"Historical data collection completed: {collection_stats['total_posts']} posts, {collection_stats['total_comments']} comments")
            
            return {
                'success': True,
                'stats': collection_stats,
                'posts_file': posts_file if all_posts else None,
                'comments_file': comments_file if all_comments else None
            }
            
        except Exception as e:
            logger.error(f"Failed to collect historical Reddit data: {e}")
            return {'success': False, 'error': str(e)}
    
    def _collect_monthly_data(self, month_start: datetime, month_name: str, output_dir: str) -> tuple:
        """
        Collect Reddit data for a specific month
        
        Args:
            month_start: Start of the month
            month_name: Month identifier (YYYY-MM)
            output_dir: Output directory for files
            
        Returns:
            Tuple of (posts_list, comments_list)
        """
        try:
            # Try different time filters to maximize data collection
            time_filters = ['month', 'week', 'day']
            all_posts = []
            all_comments = []
            seen_post_ids = set()
            
            for time_filter in time_filters:
                try:
                    logger.info(f"  Trying {time_filter} filter for {month_name}")
                    
                    # Extract posts and comments
                    posts, comments = extract_posts_with_comments(
                        self.reddit_instance,
                        self.subreddit,
                        time_filter=time_filter,
                        limit=1000,  # Max per API call
                        comment_limit=50
                    )
                    
                    # Filter posts by date to ensure they're from the target month
                    month_posts = []
                    month_comments = []
                    
                    for post in posts:
                        post_date = datetime.fromtimestamp(post['created_utc'])
                        if (month_start <= post_date < month_start + timedelta(days=31) and 
                            post['id'] not in seen_post_ids):
                            month_posts.append(post)
                            seen_post_ids.add(post['id'])
                    
                    # Filter comments by date
                    for comment in comments:
                        comment_date = datetime.fromtimestamp(comment['created_utc'])
                        if month_start <= comment_date < month_start + timedelta(days=31):
                            month_comments.append(comment)
                    
                    all_posts.extend(month_posts)
                    all_comments.extend(month_comments)
                    
                    logger.info(f"    Found {len(month_posts)} posts, {len(month_comments)} comments")
                    
                    # If we got good data, break
                    if len(month_posts) > 50:
                        break
                        
                except Exception as e:
                    logger.warning(f"    Failed to collect data with {time_filter} filter: {e}")
                    continue
            
            # Save monthly data
            if all_posts:
                posts_df = pd.DataFrame(all_posts)
                posts_df = transform_data(posts_df)
                monthly_posts_file = f"{output_dir}/lakers_posts_{month_name}.csv"
                posts_df.to_csv(monthly_posts_file, index=False)
                logger.info(f"  Saved monthly posts to {monthly_posts_file}")
            
            if all_comments:
                comments_df = pd.DataFrame(all_comments)
                comments_df['created_utc'] = pd.to_datetime(comments_df['created_utc'], unit='s')
                monthly_comments_file = f"{output_dir}/lakers_comments_{month_name}.csv"
                comments_df.to_csv(monthly_comments_file, index=False)
                logger.info(f"  Saved monthly comments to {monthly_comments_file}")
            
            return all_posts, all_comments
            
        except Exception as e:
            logger.error(f"Failed to collect monthly data for {month_name}: {e}")
            return [], []
    
    def get_data_summary(self, season: str = "2023-24") -> Dict[str, Any]:
        """
        Get summary of collected historical data
        
        Args:
            season: NBA season
            
        Returns:
            Dictionary with data summary
        """
        try:
            historical_dir = f"{OUTPUT_PATH}/historical/{season}"
            
            if not os.path.exists(historical_dir):
                return {'error': 'Historical data not found'}
            
            # Count files and data
            posts_files = [f for f in os.listdir(historical_dir) if f.startswith('lakers_posts')]
            comments_files = [f for f in os.listdir(historical_dir) if f.startswith('lakers_comments')]
            
            total_posts = 0
            total_comments = 0
            
            for file in posts_files:
                if file.endswith('.csv'):
                    df = pd.read_csv(f"{historical_dir}/{file}")
                    total_posts += len(df)
            
            for file in comments_files:
                if file.endswith('.csv'):
                    df = pd.read_csv(f"{historical_dir}/{file}")
                    total_comments += len(df)
            
            return {
                'season': season,
                'total_posts': total_posts,
                'total_comments': total_comments,
                'posts_files': len(posts_files),
                'comments_files': len(comments_files),
                'data_directory': historical_dir
            }
            
        except Exception as e:
            logger.error(f"Failed to get data summary: {e}")
            return {'error': str(e)}


def collect_historical_reddit_data(season: str = "2023-24") -> Dict[str, Any]:
    """
    Main function to collect historical Reddit data
    
    Args:
        season: NBA season to collect data for
        
    Returns:
        Collection results
    """
    collector = HistoricalRedditCollector()
    return collector.collect_season_data(season)


if __name__ == "__main__":
    # Test the historical data collection
    print("Starting historical Reddit data collection...")
    result = collect_historical_reddit_data("2023-24")
    
    if result['success']:
        print("✅ Historical data collection completed successfully!")
        print(f"📊 Total posts: {result['stats']['total_posts']}")
        print(f"💬 Total comments: {result['stats']['total_comments']}")
        print(f"📁 Files created: {len(result['stats']['files_created'])}")
    else:
        print(f"❌ Collection failed: {result['error']}")
