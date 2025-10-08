"""
Enhanced Pipeline for Lakers Sentiment Analysis and Performance Correlation
Integrates Reddit sentiment analysis with NBA player performance data
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from etls.sentiment_analysis import LakersSentimentAnalyzer
from etls.database_etl import DatabaseManager
from etls.nba_data_etl import NBADataCollector
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentPerformancePipeline:
    """
    Main pipeline for Lakers sentiment analysis and performance correlation
    """
    
    def __init__(self):
        self.sentiment_analyzer = LakersSentimentAnalyzer()
        self.db_manager = DatabaseManager()
        self.nba_collector = NBADataCollector()
        
        # Initialize database tables
        self.db_manager.create_tables()
        
        logger.info("SentimentPerformancePipeline initialized successfully")
    
    def extract_reddit_data(self, subreddit: str = 'lakers', 
                           time_filter: str = 'day', 
                           limit: int = 100) -> pd.DataFrame:
        """
        Extract Reddit data from Lakers subreddit
        
        Args:
            subreddit: Subreddit name
            time_filter: Time filter for posts
            limit: Maximum number of posts to extract
            
        Returns:
            DataFrame with Reddit posts
        """
        try:
            logger.info(f"Extracting Reddit data from r/{subreddit}")
            
            # Connect to Reddit
            reddit_instance = connect_reddit(CLIENT_ID, SECRET, 'lakers_sentiment_analyzer')
            
            # Extract posts
            posts = extract_posts(reddit_instance, subreddit, time_filter, limit)
            
            if not posts:
                logger.warning("No posts extracted from Reddit")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(posts)
            
            # Transform data
            df = transform_data(df)
            
            logger.info(f"Successfully extracted {len(df)} Reddit posts")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract Reddit data: {e}")
            return pd.DataFrame()
    
    def analyze_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform sentiment analysis on Reddit data
        
        Args:
            df: DataFrame with Reddit posts
            
        Returns:
            DataFrame with sentiment analysis results
        """
        try:
            logger.info("Starting sentiment analysis")
            
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
        Extract NBA player performance data
        
        Args:
            days: Number of days to look back for data
            
        Returns:
            DataFrame with NBA player performance data
        """
        try:
            logger.info(f"Extracting NBA data for last {days} days")
            
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
            reddit_df: Reddit posts data
            sentiment_df: Sentiment analysis results
            nba_df: NBA player performance data
            
        Returns:
            Boolean indicating success
        """
        try:
            logger.info("Storing data in database")
            
            success = True
            
            # Store Reddit posts
            if not reddit_df.empty:
                success &= self.db_manager.insert_reddit_posts(reddit_df)
            
            # Store sentiment scores
            if not sentiment_df.empty:
                success &= self.db_manager.insert_sentiment_scores(sentiment_df)
            
            # Store NBA performance data
            if not nba_df.empty:
                success &= self.db_manager.insert_player_performance(nba_df)
            
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
        Analyze correlations between sentiment and performance
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with correlation analysis results
        """
        try:
            logger.info("Analyzing sentiment-performance correlations")
            
            correlation_results = {}
            
            # Get all Lakers players
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
    
    def run_full_pipeline(self, subreddit: str = 'lakers', 
                         time_filter: str = 'day', 
                         limit: int = 100,
                         nba_days: int = 30,
                         correlation_days: int = 30) -> Dict[str, Any]:
        """
        Run the complete sentiment-performance analysis pipeline
        
        Args:
            subreddit: Reddit subreddit to analyze
            time_filter: Time filter for Reddit posts
            limit: Maximum number of Reddit posts
            nba_days: Days of NBA data to collect
            correlation_days: Days to analyze for correlations
            
        Returns:
            Dictionary with pipeline results
        """
        try:
            logger.info("Starting full sentiment-performance analysis pipeline")
            
            pipeline_results = {
                'pipeline_start_time': datetime.now().isoformat(),
                'parameters': {
                    'subreddit': subreddit,
                    'time_filter': time_filter,
                    'limit': limit,
                    'nba_days': nba_days,
                    'correlation_days': correlation_days
                },
                'results': {}
            }
            
            # Step 1: Extract Reddit data
            logger.info("Step 1: Extracting Reddit data")
            reddit_df = self.extract_reddit_data(subreddit, time_filter, limit)
            pipeline_results['results']['reddit_posts_count'] = len(reddit_df)
            
            # Step 2: Analyze sentiment
            logger.info("Step 2: Analyzing sentiment")
            sentiment_df = self.analyze_sentiment(reddit_df)
            pipeline_results['results']['sentiment_analysis_count'] = len(sentiment_df)
            
            # Step 3: Extract NBA data
            logger.info("Step 3: Extracting NBA data")
            nba_df = self.extract_nba_data(nba_days)
            pipeline_results['results']['nba_games_count'] = len(nba_df)
            
            # Step 4: Store data
            logger.info("Step 4: Storing data in database")
            storage_success = self.store_data(reddit_df, sentiment_df, nba_df)
            pipeline_results['results']['data_storage_success'] = storage_success
            
            # Step 5: Analyze correlations
            logger.info("Step 5: Analyzing correlations")
            correlation_results = self.analyze_correlations(correlation_days)
            pipeline_results['results']['correlation_analysis'] = correlation_results
            
            # Step 6: Generate insights
            logger.info("Step 6: Generating insights")
            insights = self.generate_insights(correlation_results)
            pipeline_results['results']['insights'] = insights
            
            pipeline_results['pipeline_end_time'] = datetime.now().isoformat()
            pipeline_results['pipeline_success'] = True
            
            logger.info("Full pipeline completed successfully")
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
        Get comprehensive sentiment summary for a specific player
        
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
        Get summary for all Lakers players
        
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


def test_sentiment_performance_pipeline():
    """Test function for the sentiment-performance pipeline"""
    try:
        print("Testing Sentiment-Performance Pipeline:")
        print("=" * 50)
        
        pipeline = SentimentPerformancePipeline()
        
        # Test individual components
        print("Testing Reddit data extraction...")
        reddit_df = pipeline.extract_reddit_data(limit=10)
        print(f"Reddit data extraction: {'PASSED' if not reddit_df.empty else 'FAILED'}")
        
        if not reddit_df.empty:
            print("Testing sentiment analysis...")
            sentiment_df = pipeline.analyze_sentiment(reddit_df)
            print(f"Sentiment analysis: {'PASSED' if not sentiment_df.empty else 'FAILED'}")
        
        print("Testing NBA data extraction...")
        nba_df = pipeline.extract_nba_data(days=7)
        print(f"NBA data extraction: {'PASSED' if not nba_df.empty else 'FAILED'}")
        
        print("Testing player sentiment summary...")
        lebron_summary = pipeline.get_player_sentiment_summary('lebron', days=7)
        print(f"Player sentiment summary: {'PASSED' if 'error' not in lebron_summary else 'FAILED'}")
        
        print("Sentiment-Performance Pipeline test completed successfully!")
        
    except Exception as e:
        print(f"Sentiment-Performance Pipeline test failed: {e}")


if __name__ == "__main__":
    test_sentiment_performance_pipeline()
