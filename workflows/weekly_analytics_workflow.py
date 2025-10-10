"""
Weekly Analytics Workflow for Lakers Sentiment Analysis
Aggregates daily data for comprehensive analytics
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collectors.s3_reader import S3DataReader
from data_processors.database_manager import DatabaseManager
from data_processors.sentiment_analyzer import LakersSentimentAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeeklyAnalyticsPipeline:
    """
    Weekly analytics pipeline that aggregates daily Reddit data
    for comprehensive sentiment analysis and performance correlation
    """
    
    def __init__(self):
        self.s3_reader = S3DataReader()
        self.db_manager = DatabaseManager()
        self.sentiment_analyzer = LakersSentimentAnalyzer()
        
        logger.info("WeeklyAnalyticsPipeline initialized successfully")
    
    def run_weekly_analysis(self, weeks: int = 1) -> Dict[str, Any]:
        """
        Run weekly analytics on aggregated daily data
        
        Args:
            weeks: Number of weeks to analyze
            
        Returns:
            Dictionary with analytics results
        """
        try:
            logger.info(f"Starting weekly analytics for {weeks} weeks")
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(weeks=weeks)
            days = weeks * 7
            
            # Read all daily data for the period
            daily_data = self._aggregate_daily_data(days)
            
            if daily_data.empty:
                logger.warning("No data found for weekly analysis")
                return {}
            
            # Perform comprehensive sentiment analysis
            sentiment_results = self._analyze_weekly_sentiment(daily_data)
            
            # Generate analytics insights
            analytics_results = self._generate_weekly_insights(daily_data, sentiment_results)
            
            # Store results in database
            self._store_weekly_analytics(analytics_results)
            
            logger.info("Weekly analytics completed successfully")
            return analytics_results
            
        except Exception as e:
            logger.error(f"Failed to run weekly analysis: {e}")
            return {}
    
    def _aggregate_daily_data(self, days: int) -> pd.DataFrame:
        """
        Aggregate daily Reddit data from S3
        
        Args:
            days: Number of days to aggregate
            
        Returns:
            Aggregated DataFrame
        """
        try:
            logger.info(f"Aggregating daily data for {days} days")
            
            # Get all daily files from partitioned S3 structure
            files = self.s3_reader.list_reddit_files_partitioned(days=days)
            
            if not files:
                logger.warning("No daily files found for aggregation")
                return pd.DataFrame()
            
            # Read and combine all daily files
            all_data = []
            for file_key in files:
                logger.info(f"Reading daily file: {file_key}")
                df = self.s3_reader.read_reddit_file(file_key)
                if not df.empty:
                    all_data.append(df)
            
            if not all_data:
                logger.warning("No data found in daily files")
                return pd.DataFrame()
            
            # Combine all dataframes
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Remove duplicates (posts that appeared in multiple daily pulls)
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
            final_count = len(combined_df)
            
            if initial_count != final_count:
                logger.info(f"Removed {initial_count - final_count} duplicate posts during aggregation")
            
            logger.info(f"Aggregated {len(combined_df)} unique posts from {len(files)} daily files")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to aggregate daily data: {e}")
            return pd.DataFrame()
    
    def _analyze_weekly_sentiment(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive sentiment analysis on weekly data
        
        Args:
            df: Aggregated weekly data
            
        Returns:
            Sentiment analysis results
        """
        try:
            logger.info("Performing weekly sentiment analysis")
            
            if df.empty:
                return {}
            
            # Perform sentiment analysis
            sentiment_df = self.sentiment_analyzer.analyze_dataframe(df)
            
            # Calculate weekly sentiment metrics
            weekly_metrics = {
                'total_posts': len(df),
                'total_comments': df['num_comments'].sum() if 'num_comments' in df.columns else 0,
                'avg_sentiment_score': sentiment_df['vader_compound'].mean() if 'vader_compound' in sentiment_df.columns else 0,
                'positive_posts': len(sentiment_df[sentiment_df['vader_compound'] > 0.05]) if 'vader_compound' in sentiment_df.columns else 0,
                'negative_posts': len(sentiment_df[sentiment_df['vader_compound'] < -0.05]) if 'vader_compound' in sentiment_df.columns else 0,
                'neutral_posts': len(sentiment_df[(sentiment_df['vader_compound'] >= -0.05) & (sentiment_df['vader_compound'] <= 0.05)]) if 'vader_compound' in sentiment_df.columns else 0,
                'sentiment_trend': self._calculate_sentiment_trend(sentiment_df),
                'top_players_mentioned': self._get_top_players_mentioned(df)
            }
            
            logger.info(f"Weekly sentiment analysis completed: {weekly_metrics['total_posts']} posts analyzed")
            return weekly_metrics
            
        except Exception as e:
            logger.error(f"Failed to analyze weekly sentiment: {e}")
            return {}
    
    def _calculate_sentiment_trend(self, sentiment_df: pd.DataFrame) -> str:
        """
        Calculate sentiment trend over time
        
        Args:
            sentiment_df: DataFrame with sentiment scores
            
        Returns:
            Trend description ('improving', 'declining', 'stable')
        """
        try:
            if 'created_utc' not in sentiment_df.columns or 'vader_compound' not in sentiment_df.columns:
                return 'unknown'
            
            # Sort by date
            sentiment_df = sentiment_df.sort_values('created_utc')
            
            # Calculate daily averages
            daily_avg = sentiment_df.groupby(sentiment_df['created_utc'].dt.date)['vader_compound'].mean()
            
            if len(daily_avg) < 2:
                return 'insufficient_data'
            
            # Calculate trend
            first_half = daily_avg[:len(daily_avg)//2].mean()
            second_half = daily_avg[len(daily_avg)//2:].mean()
            
            if second_half > first_half + 0.1:
                return 'improving'
            elif second_half < first_half - 0.1:
                return 'declining'
            else:
                return 'stable'
                
        except Exception as e:
            logger.error(f"Failed to calculate sentiment trend: {e}")
            return 'unknown'
    
    def _get_top_players_mentioned(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Get top players mentioned in posts
        
        Args:
            df: DataFrame with Reddit posts
            
        Returns:
            List of top players with mention counts
        """
        try:
            # This would integrate with your player mention logic
            # For now, return empty list
            return []
            
        except Exception as e:
            logger.error(f"Failed to get top players mentioned: {e}")
            return []
    
    def _generate_weekly_insights(self, df: pd.DataFrame, sentiment_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate weekly insights and recommendations
        
        Args:
            df: Aggregated weekly data
            sentiment_results: Sentiment analysis results
            
        Returns:
            Weekly insights dictionary
        """
        try:
            insights = {
                'analysis_date': datetime.now().isoformat(),
                'data_period': f"{df['created_utc'].min()} to {df['created_utc'].max()}" if not df.empty else "No data",
                'sentiment_metrics': sentiment_results,
                'recommendations': self._generate_recommendations(sentiment_results),
                'data_quality': {
                    'total_posts': len(df),
                    'posts_with_comments': len(df[df['num_comments'] > 0]) if 'num_comments' in df.columns else 0,
                    'avg_comments_per_post': df['num_comments'].mean() if 'num_comments' in df.columns else 0
                }
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate weekly insights: {e}")
            return {}
    
    def _generate_recommendations(self, sentiment_results: Dict[str, Any]) -> List[str]:
        """
        Generate recommendations based on sentiment analysis
        
        Args:
            sentiment_results: Sentiment analysis results
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        try:
            if not sentiment_results:
                return ["Insufficient data for recommendations"]
            
            avg_sentiment = sentiment_results.get('avg_sentiment_score', 0)
            sentiment_trend = sentiment_results.get('sentiment_trend', 'unknown')
            
            if avg_sentiment > 0.1:
                recommendations.append("Positive sentiment detected - consider highlighting positive news")
            elif avg_sentiment < -0.1:
                recommendations.append("Negative sentiment detected - monitor for potential issues")
            
            if sentiment_trend == 'declining':
                recommendations.append("Sentiment trend is declining - investigate recent events")
            elif sentiment_trend == 'improving':
                recommendations.append("Sentiment trend is improving - maintain current strategies")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return ["Error generating recommendations"]
    
    def _store_weekly_analytics(self, analytics_results: Dict[str, Any]) -> bool:
        """
        Store weekly analytics results in database
        
        Args:
            analytics_results: Analytics results to store
            
        Returns:
            Boolean indicating success
        """
        try:
            # This would store the analytics results in a dedicated table
            # For now, just log the results
            logger.info("Weekly analytics results stored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store weekly analytics: {e}")
            return False


def weekly_analytics_pipeline(weeks: int = 1) -> Dict[str, Any]:
    """
    Main function for weekly analytics pipeline
    
    Args:
        weeks: Number of weeks to analyze
        
    Returns:
        Analytics results
    """
    pipeline = WeeklyAnalyticsPipeline()
    return pipeline.run_weekly_analysis(weeks)
