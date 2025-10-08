"""
Streamlined Sentiment Analysis Pipeline for Lakers Project
Uses S3 as single source of truth for Reddit data, then processes for sentiment analysis
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.sentiment_analysis import LakersSentimentAnalyzer
from etls.database_etl import DatabaseManager
from etls.nba_data_etl_2025_26 import NBADataCollector2025_26
from etls.s3_data_reader import S3DataReader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamlinedSentimentPipeline:
    """
    Streamlined pipeline for Lakers sentiment analysis
    - Reads Reddit data from S3 (single source of truth)
    - Performs sentiment analysis
    - Collects NBA performance data
    - Stores results in PostgreSQL
    - Analyzes correlations
    """
    
    def __init__(self):
        self.sentiment_analyzer = LakersSentimentAnalyzer()
        self.db_manager = DatabaseManager()
        self.nba_collector = NBADataCollector2025_26()
        self.s3_reader = S3DataReader()
        
        # Initialize database tables
        self.db_manager.create_tables()
        
        logger.info("StreamlinedSentimentPipeline initialized successfully")
    
    def read_reddit_data_from_s3(self, days: int = 30) -> pd.DataFrame:
        """
        Read Reddit data from S3 for specified time period
        
        Args:
            days: Number of days to look back for data
            
        Returns:
            DataFrame with Reddit data from S3
        """
        try:
            logger.info(f"Reading Reddit data from S3 for last {days} days")
            
            # Read data from S3
            df = self.s3_reader.get_reddit_data_for_period(days=days)
            
            if df.empty:
                logger.warning("No Reddit data found in S3 for the specified period")
                return pd.DataFrame()
            
            logger.info(f"Successfully read {len(df)} Reddit posts from S3")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Reddit data from S3: {e}")
            return pd.DataFrame()
    
    def analyze_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform sentiment analysis on Reddit data from S3
        
        Args:
            df: DataFrame with Reddit posts from S3
            
        Returns:
            DataFrame with sentiment analysis results
        """
        try:
            logger.info("Starting sentiment analysis on S3 data")
            
            if df.empty:
                logger.warning("No data to analyze")
                return pd.DataFrame()
            
            # Perform comprehensive sentiment analysis
            sentiment_df = self.sentiment_analyzer.analyze_dataframe(df)
            
            logger.info(f"Sentiment analysis completed for {len(sentiment_df)} posts")
            return sentiment_df
            
        except Exception as e:
            logger.error(f"Failed to analyze sentiment: {e}")
            return pd.DataFrame()
    
    def extract_nba_data(self, days: int = 30) -> pd.DataFrame:
        """
        Extract NBA player performance data for 2025-26 Lakers
        
        Args:
            days: Number of days to look back for data
            
        Returns:
            DataFrame with NBA player performance data
        """
        try:
            logger.info(f"Extracting NBA data for 2025-26 Lakers for last {days} days")
            
            # Get all Lakers player statistics
            nba_df = self.nba_collector.get_all_lakers_player_stats(
                start_date=(datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                end_date=datetime.now().strftime('%Y-%m-%d')
            )
            
            if nba_df.empty:
                logger.warning("No NBA data extracted")
                return pd.DataFrame()
            
            logger.info(f"Successfully extracted {len(nba_df)} NBA player game records")
            return nba_df
            
        except Exception as e:
            logger.error(f"Failed to extract NBA data: {e}")
            return pd.DataFrame()
    
    def store_data(self, reddit_df: pd.DataFrame, sentiment_df: pd.DataFrame, 
                   nba_df: pd.DataFrame) -> bool:
        """
        Store all data in PostgreSQL database
        
        Args:
            reddit_df: Reddit posts data from S3
            sentiment_df: Sentiment analysis results
            nba_df: NBA player performance data
            
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("Storing data in database")
            
            success = True
            
            # Store Reddit posts (only if not already stored)
            if not reddit_df.empty:
                # Check if data already exists to avoid duplicates
                existing_posts = self.db_manager.get_existing_post_ids(reddit_df['id'].tolist())
                new_posts = reddit_df[~reddit_df['id'].isin(existing_posts)]
                
                if not new_posts.empty:
                    success &= self.db_manager.insert_reddit_posts(new_posts)
                    logger.info(f"Stored {len(new_posts)} new Reddit posts")
                else:
                    logger.info("No new Reddit posts to store")
            
            # Store sentiment scores
            if not sentiment_df.empty:
                success &= self.db_manager.insert_sentiment_scores(sentiment_df)
                logger.info(f"Stored sentiment scores for {len(sentiment_df)} posts")
            
            # Store NBA performance data
            if not nba_df.empty:
                success &= self.db_manager.insert_player_performance(nba_df)
                logger.info(f"Stored {len(nba_df)} NBA player performance records")
            
            if success:
                logger.info("All data stored successfully in database")
            else:
                logger.error("Some data failed to store in database")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to store data: {e}")
            return False
    
    def analyze_correlations(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze correlations between sentiment and performance for 2025-26 Lakers
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with correlation analysis results
        """
        try:
            logger.info("Analyzing sentiment-performance correlations for 2025-26 Lakers")
            
            correlation_results = {}
            
            # Get all 2025-26 Lakers players
            for player_key in self.nba_collector.lakers_roster.keys():
                if self.nba_collector.lakers_roster[player_key]['player_id'] == 1503:
                    continue  # Skip placeholder entries
                
                logger.info(f"Analyzing correlations for {player_key}")
                
                # Calculate correlations
                correlations = self.db_manager.calculate_sentiment_performance_correlation(
                    player_key, days
                )
                
                if 'error' not in correlations:
                    correlation_results[player_key] = correlations
                    
                    # Save to database
                    self.db_manager.save_correlation_analysis(correlations)
            
            logger.info(f"Correlation analysis completed for {len(correlation_results)} players")
            return correlation_results
            
        except Exception as e:
            logger.error(f"Failed to analyze correlations: {e}")
            return {}
    
    def generate_insights(self, correlation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from correlation analysis
        
        Args:
            correlation_results: Results from correlation analysis
            
        Returns:
            Dictionary with insights
        """
        try:
            logger.info("Generating insights from correlation analysis")
            
            insights = {
                'analysis_timestamp': datetime.now().isoformat(),
                'total_players_analyzed': len(correlation_results),
                'key_findings': [],
                'player_rankings': {},
                'correlation_summary': {}
            }
            
            # Analyze each player's correlations
            for player, results in correlation_results.items():
                correlations = results.get('correlations', {})
                
                # Calculate average correlation strength
                correlation_values = []
                for corr_type, corr_data in correlations.items():
                    if isinstance(corr_data, dict) and 'correlation_coefficient' in corr_data:
                        correlation_values.append(abs(corr_data['correlation_coefficient']))
                
                if correlation_values:
                    avg_correlation = sum(correlation_values) / len(correlation_values)
                    insights['player_rankings'][player] = {
                        'average_correlation_strength': avg_correlation,
                        'total_correlations': len(correlation_values),
                        'sample_size': results.get('total_data_points', 0)
                    }
            
            # Sort players by correlation strength
            sorted_players = sorted(
                insights['player_rankings'].items(),
                key=lambda x: x[1]['average_correlation_strength'],
                reverse=True
            )
            
            # Generate key findings
            if sorted_players:
                strongest_correlation_player = sorted_players[0]
                insights['key_findings'].append(
                    f"Strongest sentiment-performance correlation: {strongest_correlation_player[0]} "
                    f"(strength: {strongest_correlation_player[1]['average_correlation_strength']:.3f})"
                )
            
            # Calculate overall correlation statistics
            all_correlations = []
            for player, results in correlation_results.items():
                for corr_type, corr_data in results.get('correlations', {}).items():
                    if isinstance(corr_data, dict) and 'correlation_coefficient' in corr_data:
                        all_correlations.append(corr_data['correlation_coefficient'])
            
            if all_correlations:
                insights['correlation_summary'] = {
                    'total_correlations': len(all_correlations),
                    'average_correlation': sum(all_correlations) / len(all_correlations),
                    'positive_correlations': len([c for c in all_correlations if c > 0]),
                    'negative_correlations': len([c for c in all_correlations if c < 0]),
                    'strong_correlations': len([c for c in all_correlations if abs(c) > 0.5]),
                    'moderate_correlations': len([c for c in all_correlations if 0.3 <= abs(c) <= 0.5]),
                    'weak_correlations': len([c for c in all_correlations if abs(c) < 0.3])
                }
            
            logger.info("Insights generation completed")
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return {}
    
    def run_full_pipeline(self, days: int = 30) -> Dict[str, Any]:
        """
        Run the complete streamlined sentiment-performance analysis pipeline
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with pipeline results
        """
        try:
            logger.info("Starting streamlined sentiment-performance analysis pipeline")
            
            pipeline_results = {
                'pipeline_start_time': datetime.now().isoformat(),
                'parameters': {
                    'days': days,
                    'data_source': 'S3',
                    'roster': '2025-26 Lakers'
                },
                'results': {}
            }
            
            # Step 1: Read Reddit data from S3
            logger.info("Step 1: Reading Reddit data from S3")
            reddit_df = self.read_reddit_data_from_s3(days)
            pipeline_results['results']['reddit_posts_count'] = len(reddit_df)
            
            # Step 2: Analyze sentiment
            logger.info("Step 2: Analyzing sentiment")
            sentiment_df = self.analyze_sentiment(reddit_df)
            pipeline_results['results']['sentiment_analysis_count'] = len(sentiment_df)
            
            # Step 3: Extract NBA data
            logger.info("Step 3: Extracting NBA data for 2025-26 Lakers")
            nba_df = self.extract_nba_data(days)
            pipeline_results['results']['nba_games_count'] = len(nba_df)
            
            # Step 4: Store data
            logger.info("Step 4: Storing data in database")
            storage_success = self.store_data(reddit_df, sentiment_df, nba_df)
            pipeline_results['results']['data_storage_success'] = storage_success
            
            # Step 5: Analyze correlations
            logger.info("Step 5: Analyzing correlations")
            correlation_results = self.analyze_correlations(days)
            pipeline_results['results']['correlation_analysis'] = correlation_results
            
            # Step 6: Generate insights
            logger.info("Step 6: Generating insights")
            insights = self.generate_insights(correlation_results)
            pipeline_results['results']['insights'] = insights
            
            pipeline_results['pipeline_end_time'] = datetime.now().isoformat()
            pipeline_results['pipeline_success'] = True
            
            logger.info("Streamlined pipeline completed successfully")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                'pipeline_start_time': datetime.now().isoformat(),
                'pipeline_end_time': datetime.now().isoformat(),
                'pipeline_success': False,
                'error': str(e)
            }
    
    def get_player_sentiment_summary(self, player_name: str, days: int = 30) -> Dict[str, Any]:
        """
        Get comprehensive sentiment summary for a specific 2025-26 Lakers player
        
        Args:
            player_name: Player name key
            days: Number of days to analyze
            
        Returns:
            Dictionary with player sentiment summary
        """
        try:
            # Get sentiment data from database
            sentiment_df = self.db_manager.get_player_sentiment_summary(player_name, days)
            
            # Get performance data from database
            performance_df = self.db_manager.get_player_performance_summary(player_name, days)
            
            # Get correlation analysis
            correlations = self.db_manager.calculate_sentiment_performance_correlation(player_name, days)
            
            summary = {
                'player_name': player_name,
                'analysis_period_days': days,
                'sentiment_data': sentiment_df.to_dict('records') if not sentiment_df.empty else [],
                'performance_data': performance_df.to_dict('records') if not performance_df.empty else [],
                'correlations': correlations,
                'summary_timestamp': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get player sentiment summary: {e}")
            return {'error': str(e)}
    
    def get_all_players_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Get summary for all 2025-26 Lakers players
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with all players summary
        """
        try:
            # Get sentiment summary for all players
            sentiment_summary = self.db_manager.get_all_players_sentiment_summary(days)
            
            summary = {
                'analysis_period_days': days,
                'total_players': len(sentiment_summary),
                'sentiment_summary': sentiment_summary.to_dict('records') if not sentiment_summary.empty else [],
                'summary_timestamp': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get all players summary: {e}")
            return {'error': str(e)}
    
    def get_s3_data_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Get summary of Reddit data available in S3
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with S3 data summary
        """
        try:
            summary = self.s3_reader.get_reddit_data_summary(days)
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get S3 data summary: {e}")
            return {'error': str(e)}


def test_streamlined_pipeline():
    """Test function for the streamlined pipeline"""
    try:
        print("Testing Streamlined Sentiment Pipeline:")
        print("=" * 50)
        
        pipeline = StreamlinedSentimentPipeline()
        
        # Test S3 data reading
        print("Testing S3 data reading...")
        reddit_df = pipeline.read_reddit_data_from_s3(days=7)
        print(f"S3 data reading: {'PASSED' if not reddit_df.empty else 'FAILED'}")
        
        if not reddit_df.empty:
            print("Testing sentiment analysis...")
            sentiment_df = pipeline.analyze_sentiment(reddit_df)
            print(f"Sentiment analysis: {'PASSED' if not sentiment_df.empty else 'FAILED'}")
        
        print("Testing NBA data extraction...")
        nba_df = pipeline.extract_nba_data(days=7)
        print(f"NBA data extraction: {'PASSED' if not nba_df.empty else 'FAILED'}")
        
        print("Testing S3 data summary...")
        s3_summary = pipeline.get_s3_data_summary(days=7)
        print(f"S3 data summary: {'PASSED' if 'error' not in s3_summary else 'FAILED'}")
        
        print("Streamlined pipeline test completed successfully!")
        
    except Exception as e:
        print(f"Streamlined pipeline test failed: {e}")


if __name__ == "__main__":
    test_streamlined_pipeline()
