"""
Correlation Engine for Lakers Sentiment Analysis
Calculates correlations between Reddit sentiment and NBA player performance
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from scipy import stats
from scipy.stats import pearsonr, spearmanr
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processors.database_manager import DatabaseManager
from analytics.player_mention_filter import PlayerMentionFilter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentPerformanceCorrelationEngine:
    """
    Engine for calculating correlations between sentiment and performance
    """
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.mention_filter = PlayerMentionFilter()
        
        logger.info("SentimentPerformanceCorrelationEngine initialized")
    
    def calculate_player_correlations(self, player_name: str, 
                                    start_date: str = "2023-10-01",
                                    end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Calculate correlations for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with correlation results
        """
        try:
            logger.info(f"Calculating correlations for {player_name}")
            
            # Get sentiment data for player
            sentiment_data = self._get_player_sentiment_data(player_name, start_date, end_date)
            
            # Get performance data for player
            performance_data = self._get_player_performance_data(player_name, start_date, end_date)
            
            if sentiment_data.empty or performance_data.empty:
                return {
                    'success': False,
                    'error': 'Insufficient data for correlation analysis',
                    'sentiment_records': len(sentiment_data),
                    'performance_records': len(performance_data)
                }
            
            # Calculate daily sentiment aggregates
            daily_sentiment = self._aggregate_daily_sentiment(sentiment_data)
            
            # Calculate correlations
            correlations = self._calculate_correlations(daily_sentiment, performance_data)
            
            # Generate insights
            insights = self._generate_correlation_insights(correlations, player_name)
            
            logger.info(f"Correlation analysis completed for {player_name}")
            
            return {
                'success': True,
                'player_name': player_name,
                'analysis_period': f"{start_date} to {end_date}",
                'data_points': len(daily_sentiment),
                'correlations': correlations,
                'insights': insights,
                'sentiment_summary': self._get_sentiment_summary(daily_sentiment),
                'performance_summary': self._get_performance_summary(performance_data)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate correlations for {player_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_player_sentiment_data(self, player_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get sentiment data for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with sentiment data
        """
        try:
            # This would query the actual database
            # For now, generate mock sentiment data
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Generate mock sentiment data with realistic patterns
            np.random.seed(42)
            sentiment_data = []
            
            for date in date_range:
                # Base sentiment with some variation
                base_sentiment = np.random.normal(0.1, 0.3)  # Slightly positive on average
                
                # Add game-related sentiment spikes
                if date.weekday() in [0, 2, 4, 6]:  # Game days
                    base_sentiment += np.random.normal(0.2, 0.2)
                
                # Add some realistic patterns
                if 'lebron' in player_name.lower():
                    base_sentiment += 0.1  # LeBron generally more positive
                elif 'davis' in player_name.lower():
                    base_sentiment += 0.05  # AD slightly positive
                
                sentiment_data.append({
                    'date': date,
                    'player_name': player_name,
                    'vader_compound': np.clip(base_sentiment, -1, 1),
                    'vader_positive': max(0, base_sentiment + 0.2),
                    'vader_negative': max(0, -base_sentiment + 0.2),
                    'vader_neutral': 1 - abs(base_sentiment),
                    'post_count': np.random.poisson(5)
                })
            
            df = pd.DataFrame(sentiment_data)
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get sentiment data for {player_name}: {e}")
            return pd.DataFrame()
    
    def _get_player_performance_data(self, player_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get performance data for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with performance data
        """
        try:
            # This would query the actual database
            # For now, generate mock performance data
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Generate mock performance data
            np.random.seed(42)
            performance_data = []
            
            for date in date_range:
                # Only include game days (simplified)
                if date.weekday() in [0, 2, 4, 6]:  # Game days
                    # Generate realistic performance stats
                    if 'lebron' in player_name.lower():
                        points = np.random.normal(25, 8)
                        rebounds = np.random.normal(7, 3)
                        assists = np.random.normal(8, 3)
                    elif 'davis' in player_name.lower():
                        points = np.random.normal(22, 6)
                        rebounds = np.random.normal(12, 4)
                        assists = np.random.normal(3, 2)
                    else:
                        points = np.random.normal(15, 5)
                        rebounds = np.random.normal(5, 3)
                        assists = np.random.normal(4, 2)
                    
                    performance_data.append({
                        'game_date': date,
                        'player_name': player_name,
                        'points': max(0, int(points)),
                        'rebounds': max(0, int(rebounds)),
                        'assists': max(0, int(assists)),
                        'steals': np.random.poisson(1),
                        'blocks': np.random.poisson(1),
                        'turnovers': np.random.poisson(2),
                        'plus_minus': np.random.normal(0, 10),
                        'game_result': np.random.choice(['W', 'L'], p=[0.6, 0.4])
                    })
            
            df = pd.DataFrame(performance_data)
            df['game_date'] = pd.to_datetime(df['game_date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get performance data for {player_name}: {e}")
            return pd.DataFrame()
    
    def _aggregate_daily_sentiment(self, sentiment_data: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate sentiment data by day
        
        Args:
            sentiment_data: Raw sentiment data
            
        Returns:
            Daily aggregated sentiment data
        """
        try:
            daily_agg = sentiment_data.groupby('date').agg({
                'vader_compound': ['mean', 'std', 'count'],
                'vader_positive': 'mean',
                'vader_negative': 'mean',
                'vader_neutral': 'mean',
                'post_count': 'sum'
            }).reset_index()
            
            # Flatten column names
            daily_agg.columns = ['date', 'sentiment_mean', 'sentiment_std', 'sentiment_count', 
                               'positive_mean', 'negative_mean', 'neutral_mean', 'total_posts']
            
            # Fill NaN values
            daily_agg['sentiment_std'] = daily_agg['sentiment_std'].fillna(0)
            
            return daily_agg
            
        except Exception as e:
            logger.error(f"Failed to aggregate daily sentiment: {e}")
            return pd.DataFrame()
    
    def _calculate_correlations(self, sentiment_data: pd.DataFrame, 
                              performance_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate correlations between sentiment and performance
        
        Args:
            sentiment_data: Daily sentiment data
            performance_data: Game performance data
            
        Returns:
            Dictionary with correlation results
        """
        try:
            # Merge sentiment and performance data
            # Use sentiment from day before game (predictive correlation)
            performance_data['prev_day'] = performance_data['game_date'] - timedelta(days=1)
            
            merged_data = performance_data.merge(
                sentiment_data, 
                left_on='prev_day', 
                right_on='date', 
                how='inner'
            )
            
            if merged_data.empty:
                return {'error': 'No overlapping data for correlation analysis'}
            
            correlations = {}
            
            # Calculate correlations for different performance metrics
            performance_metrics = ['points', 'rebounds', 'assists', 'plus_minus']
            
            for metric in performance_metrics:
                if metric in merged_data.columns:
                    # Pearson correlation
                    pearson_r, pearson_p = pearsonr(
                        merged_data['sentiment_mean'], 
                        merged_data[metric]
                    )
                    
                    # Spearman correlation (rank-based)
                    spearman_r, spearman_p = spearmanr(
                        merged_data['sentiment_mean'], 
                        merged_data[metric]
                    )
                    
                    correlations[metric] = {
                        'pearson_r': round(pearson_r, 4),
                        'pearson_p': round(pearson_p, 4),
                        'spearman_r': round(spearman_r, 4),
                        'spearman_p': round(spearman_p, 4),
                        'sample_size': len(merged_data),
                        'is_significant': pearson_p < 0.05
                    }
            
            # Overall correlation strength
            avg_pearson = np.mean([corr['pearson_r'] for corr in correlations.values()])
            avg_spearman = np.mean([corr['spearman_r'] for corr in correlations.values()])
            
            correlations['overall'] = {
                'avg_pearson_r': round(avg_pearson, 4),
                'avg_spearman_r': round(avg_spearman, 4),
                'correlation_strength': self._classify_correlation_strength(avg_pearson)
            }
            
            return correlations
            
        except Exception as e:
            logger.error(f"Failed to calculate correlations: {e}")
            return {'error': str(e)}
    
    def _classify_correlation_strength(self, correlation: float) -> str:
        """
        Classify correlation strength
        
        Args:
            correlation: Correlation coefficient
            
        Returns:
            String classification
        """
        abs_corr = abs(correlation)
        
        if abs_corr >= 0.7:
            return 'strong'
        elif abs_corr >= 0.5:
            return 'moderate'
        elif abs_corr >= 0.3:
            return 'weak'
        else:
            return 'very_weak'
    
    def _generate_correlation_insights(self, correlations: Dict[str, Any], 
                                     player_name: str) -> List[str]:
        """
        Generate insights from correlation analysis
        
        Args:
            correlations: Correlation results
            player_name: Name of the player
            
        Returns:
            List of insights
        """
        insights = []
        
        try:
            if 'error' in correlations:
                return [f"Analysis error: {correlations['error']}"]
            
            overall = correlations.get('overall', {})
            avg_correlation = overall.get('avg_pearson_r', 0)
            strength = overall.get('correlation_strength', 'unknown')
            
            # Main insight
            if avg_correlation > 0.3:
                insights.append(f"{player_name.title()}'s Reddit sentiment shows {strength} positive correlation with performance")
            elif avg_correlation < -0.3:
                insights.append(f"{player_name.title()}'s Reddit sentiment shows {strength} negative correlation with performance")
            else:
                insights.append(f"{player_name.title()}'s Reddit sentiment shows weak correlation with performance")
            
            # Specific metric insights
            for metric, corr_data in correlations.items():
                if metric != 'overall' and isinstance(corr_data, dict):
                    r_value = corr_data.get('pearson_r', 0)
                    is_significant = corr_data.get('is_significant', False)
                    
                    if is_significant and abs(r_value) > 0.3:
                        direction = "positive" if r_value > 0 else "negative"
                        insights.append(f"Significant {direction} correlation between sentiment and {metric} (r={r_value:.3f})")
            
            # Sample size insight
            sample_size = correlations.get('points', {}).get('sample_size', 0)
            if sample_size > 0:
                insights.append(f"Analysis based on {sample_size} games with sufficient sentiment data")
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return [f"Error generating insights: {str(e)}"]
    
    def _get_sentiment_summary(self, sentiment_data: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for sentiment data"""
        if sentiment_data.empty:
            return {}
        
        return {
            'avg_sentiment': round(sentiment_data['sentiment_mean'].mean(), 3),
            'sentiment_std': round(sentiment_data['sentiment_mean'].std(), 3),
            'total_days': len(sentiment_data),
            'positive_days': len(sentiment_data[sentiment_data['sentiment_mean'] > 0.1]),
            'negative_days': len(sentiment_data[sentiment_data['sentiment_mean'] < -0.1])
        }
    
    def _get_performance_summary(self, performance_data: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for performance data"""
        if performance_data.empty:
            return {}
        
        return {
            'total_games': len(performance_data),
            'avg_points': round(performance_data['points'].mean(), 1),
            'avg_rebounds': round(performance_data['rebounds'].mean(), 1),
            'avg_assists': round(performance_data['assists'].mean(), 1),
            'win_percentage': round(len(performance_data[performance_data['game_result'] == 'W']) / len(performance_data), 3)
        }
    
    def analyze_all_qualifying_players(self, start_date: str = "2023-10-01",
                                     end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Analyze correlations for all qualifying players
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with analysis results for all players
        """
        try:
            logger.info("Analyzing correlations for all qualifying players")
            
            # Get qualifying players
            filter_result = self.mention_filter.get_qualifying_players(start_date, end_date)
            
            if not filter_result['success']:
                return {'success': False, 'error': 'Failed to get qualifying players'}
            
            qualifying_players = filter_result['qualifying_players']
            all_results = {}
            
            for player in qualifying_players:
                player_name = player['player_name']
                logger.info(f"Analyzing {player_name}")
                
                result = self.calculate_player_correlations(player_name, start_date, end_date)
                all_results[player_name] = result
            
            # Generate overall insights
            overall_insights = self._generate_overall_insights(all_results)
            
            return {
                'success': True,
                'players_analyzed': len(qualifying_players),
                'results': all_results,
                'overall_insights': overall_insights,
                'analysis_period': f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze all players: {e}")
            return {'success': False, 'error': str(e)}
    
    def _generate_overall_insights(self, all_results: Dict[str, Any]) -> List[str]:
        """Generate overall insights from all player analyses"""
        insights = []
        
        try:
            successful_analyses = [r for r in all_results.values() if r.get('success', False)]
            
            if not successful_analyses:
                return ["No successful analyses to generate insights"]
            
            # Count players by correlation strength
            strong_correlations = 0
            moderate_correlations = 0
            weak_correlations = 0
            
            for result in successful_analyses:
                overall_corr = result.get('correlations', {}).get('overall', {})
                strength = overall_corr.get('correlation_strength', 'unknown')
                
                if strength == 'strong':
                    strong_correlations += 1
                elif strength == 'moderate':
                    moderate_correlations += 1
                else:
                    weak_correlations += 1
            
            insights.append(f"Analyzed {len(successful_analyses)} players with sufficient data")
            insights.append(f"Strong correlations: {strong_correlations} players")
            insights.append(f"Moderate correlations: {moderate_correlations} players")
            insights.append(f"Weak correlations: {weak_correlations} players")
            
            # Find best performing player
            best_player = None
            best_correlation = -1
            
            for player_name, result in all_results.items():
                if result.get('success', False):
                    overall_corr = result.get('correlations', {}).get('overall', {})
                    avg_corr = abs(overall_corr.get('avg_pearson_r', 0))
                    
                    if avg_corr > best_correlation:
                        best_correlation = avg_corr
                        best_player = player_name
            
            if best_player:
                insights.append(f"Strongest sentiment-performance correlation: {best_player.title()} (r={best_correlation:.3f})")
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate overall insights: {e}")
            return [f"Error generating overall insights: {str(e)}"]


def analyze_player_correlations(player_name: str) -> Dict[str, Any]:
    """
    Main function to analyze correlations for a specific player
    
    Args:
        player_name: Name of the player
        
    Returns:
        Analysis results
    """
    engine = SentimentPerformanceCorrelationEngine()
    return engine.calculate_player_correlations(player_name)


if __name__ == "__main__":
    # Test the correlation engine
    print("Testing Correlation Engine...")
    result = analyze_player_correlations("lebron_james")
    
    if result['success']:
        print("✅ Correlation analysis completed successfully!")
        print(f"📊 Data points: {result['data_points']}")
        print(f"🔗 Overall correlation: {result['correlations']['overall']['avg_pearson_r']}")
        print(f"💡 Insights: {len(result['insights'])} generated")
    else:
        print(f"❌ Analysis failed: {result['error']}")
