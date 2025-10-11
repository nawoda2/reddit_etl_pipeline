"""
Data Loader for Lakers Sentiment Analytics Dashboard
Efficient data loading with caching for Streamlit dashboard
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics.correlation_engine import SentimentPerformanceCorrelationEngine
from analytics.performance_predictor import PerformancePredictor
from analytics.player_mention_filter import PlayerMentionFilter

class DashboardDataLoader:
    """
    Data loader with caching for Streamlit dashboard
    """
    
    def __init__(self):
        self.correlation_engine = SentimentPerformanceCorrelationEngine()
        self.performance_predictor = PerformancePredictor()
        self.mention_filter = PlayerMentionFilter()
    
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def get_qualifying_players(_self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get qualifying players with caching
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with qualifying players
        """
        return _self.mention_filter.get_qualifying_players(start_date, end_date)
    
    @st.cache_data(ttl=1800)  # Cache for 30 minutes
    def get_player_correlation_analysis(_self, player_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get correlation analysis for a specific player with caching
        
        Args:
            player_name: Name of the player
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with correlation analysis results
        """
        return _self.correlation_engine.calculate_player_correlations(player_name, start_date, end_date)
    
    @st.cache_data(ttl=1800)  # Cache for 30 minutes
    def get_player_prediction_models(_self, player_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get prediction models for a specific player with caching
        
        Args:
            player_name: Name of the player
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with prediction model results
        """
        return _self.performance_predictor.build_prediction_models(player_name, start_date, end_date)
    
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def get_all_players_analysis(_self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get analysis for all qualifying players with caching
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with all players analysis
        """
        return _self.correlation_engine.analyze_all_qualifying_players(start_date, end_date)
    
    @st.cache_data(ttl=1800)  # Cache for 30 minutes
    def get_player_mention_timeline(_self, player_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get mention timeline for a player with caching
        
        Args:
            player_name: Name of the player
            start_date: Start date for timeline
            end_date: End date for timeline
            
        Returns:
            DataFrame with daily mention counts
        """
        return _self.mention_filter.get_player_mention_timeline(player_name, start_date, end_date)
    
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def get_season_summary_stats(_self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get season summary statistics with caching
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with season summary
        """
        return _self.mention_filter.get_mention_summary_stats(start_date, end_date)
    
    def get_dashboard_metrics(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get key dashboard metrics
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with dashboard metrics
        """
        try:
            # Get qualifying players
            qualifying_players = self.get_qualifying_players(start_date, end_date)
            
            if not qualifying_players['success']:
                return {'error': 'Failed to get qualifying players'}
            
            # Get all players analysis
            all_analysis = self.get_all_players_analysis(start_date, end_date)
            
            if not all_analysis['success']:
                return {'error': 'Failed to get all players analysis'}
            
            # Calculate overall metrics
            successful_analyses = [r for r in all_analysis['results'].values() if r.get('success', False)]
            
            if not successful_analyses:
                return {'error': 'No successful analyses found'}
            
            # Calculate average correlation
            correlations = []
            for result in successful_analyses:
                overall_corr = result.get('correlations', {}).get('overall', {})
                if 'avg_pearson_r' in overall_corr:
                    correlations.append(overall_corr['avg_pearson_r'])
            
            avg_correlation = np.mean(correlations) if correlations else 0
            
            # Calculate prediction accuracy (mock data for now)
            prediction_accuracy = 0.78  # This would come from actual model evaluation
            
            # Calculate data quality metrics
            total_posts = sum(r.get('sentiment_summary', {}).get('total_days', 0) for r in successful_analyses)
            total_games = sum(r.get('performance_summary', {}).get('total_games', 0) for r in successful_analyses)
            
            return {
                'success': True,
                'analysis_period': f"{start_date} to {end_date}",
                'qualifying_players': len(qualifying_players['qualifying_players']),
                'avg_correlation': round(avg_correlation, 3),
                'prediction_accuracy': round(prediction_accuracy, 3),
                'total_data_points': total_posts + total_games,
                'correlation_strength': self._classify_correlation_strength(avg_correlation),
                'players_analyzed': len(successful_analyses)
            }
            
        except Exception as e:
            return {'error': f'Failed to get dashboard metrics: {str(e)}'}
    
    def _classify_correlation_strength(self, correlation: float) -> str:
        """Classify correlation strength"""
        abs_corr = abs(correlation)
        
        if abs_corr >= 0.7:
            return 'Strong'
        elif abs_corr >= 0.5:
            return 'Moderate'
        elif abs_corr >= 0.3:
            return 'Weak'
        else:
            return 'Very Weak'
    
    def get_player_comparison_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get data for player comparison chart
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            DataFrame with player comparison data
        """
        try:
            # Get all players analysis
            all_analysis = self.get_all_players_analysis(start_date, end_date)
            
            if not all_analysis['success']:
                return pd.DataFrame()
            
            comparison_data = []
            
            for player_name, result in all_analysis['results'].items():
                if result.get('success', False):
                    correlations = result.get('correlations', {})
                    overall = correlations.get('overall', {})
                    
                    comparison_data.append({
                        'player_name': player_name.replace('_', ' ').title(),
                        'overall_correlation': overall.get('avg_pearson_r', 0),
                        'points_correlation': correlations.get('points', {}).get('pearson_r', 0),
                        'rebounds_correlation': correlations.get('rebounds', {}).get('pearson_r', 0),
                        'assists_correlation': correlations.get('assists', {}).get('pearson_r', 0),
                        'data_points': result.get('data_points', 0),
                        'correlation_strength': self._classify_correlation_strength(overall.get('avg_pearson_r', 0))
                    })
            
            return pd.DataFrame(comparison_data)
            
        except Exception as e:
            return pd.DataFrame()
    
    def get_timeline_data(self, player_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get timeline data for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date for timeline
            end_date: End date for timeline
            
        Returns:
            DataFrame with timeline data
        """
        try:
            # Get sentiment data
            sentiment_data = self.correlation_engine._get_player_sentiment_data(player_name, start_date, end_date)
            
            # Get performance data
            performance_data = self.correlation_engine._get_player_performance_data(player_name, start_date, end_date)
            
            if sentiment_data.empty or performance_data.empty:
                return pd.DataFrame()
            
            # Aggregate daily sentiment
            daily_sentiment = self.correlation_engine._aggregate_daily_sentiment(sentiment_data)
            
            # Merge data
            performance_data['prev_day'] = performance_data['game_date'] - timedelta(days=1)
            
            timeline_data = performance_data.merge(
                daily_sentiment,
                left_on='prev_day',
                right_on='date',
                how='left'
            )
            
            # Fill missing sentiment data
            timeline_data['sentiment_mean'] = timeline_data['sentiment_mean'].fillna(0)
            timeline_data['sentiment_std'] = timeline_data['sentiment_std'].fillna(0)
            
            return timeline_data[['game_date', 'sentiment_mean', 'sentiment_std', 'points', 'rebounds', 'assists', 'game_result', 'opponent']]
            
        except Exception as e:
            return pd.DataFrame()
    
    def get_season_trends_data(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Get season trends data
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with season trends data
        """
        try:
            # Generate mock monthly data
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            months = []
            sentiment_trend = []
            performance_trend = []
            
            current_date = start_dt
            while current_date <= end_dt:
                month_name = current_date.strftime('%b %Y')
                months.append(month_name)
                
                # Mock trend data
                np.random.seed(42)
                base_sentiment = 0.1 + (current_date.month - 10) * 0.02  # Slight upward trend
                sentiment_trend.append(base_sentiment + np.random.normal(0, 0.05))
                
                base_performance = 25 + (current_date.month - 10) * 0.5  # Slight upward trend
                performance_trend.append(base_performance + np.random.normal(0, 2))
                
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            return {
                'months': months,
                'sentiment_trend': sentiment_trend,
                'performance_trend': performance_trend,
                'correlation_by_month': [0.65, 0.68, 0.62, 0.71, 0.75, 0.73, 0.78, 0.81]  # Mock data
            }
            
        except Exception as e:
            return {'error': f'Failed to get season trends data: {str(e)}'}
    
    def clear_cache(self):
        """Clear all cached data"""
        st.cache_data.clear()
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information"""
        return {
            'cache_cleared': False,  # This would be implemented with actual cache info
            'cached_functions': [
                'get_qualifying_players',
                'get_player_correlation_analysis',
                'get_player_prediction_models',
                'get_all_players_analysis',
                'get_player_mention_timeline',
                'get_season_summary_stats'
            ]
        }


# Global data loader instance
@st.cache_resource
def get_data_loader():
    """Get cached data loader instance"""
    return DashboardDataLoader()


def load_dashboard_data(start_date: str, end_date: str, player_name: str = None) -> Dict[str, Any]:
    """
    Main function to load all dashboard data
    
    Args:
        start_date: Start date for analysis
        end_date: End date for analysis
        player_name: Optional specific player name
        
    Returns:
        Dictionary with all loaded data
    """
    data_loader = get_data_loader()
    
    try:
        # Load basic metrics
        metrics = data_loader.get_dashboard_metrics(start_date, end_date)
        
        # Load player comparison data
        comparison_data = data_loader.get_player_comparison_data(start_date, end_date)
        
        # Load season trends
        trends_data = data_loader.get_season_trends_data(start_date, end_date)
        
        result = {
            'success': True,
            'metrics': metrics,
            'comparison_data': comparison_data,
            'trends_data': trends_data,
            'analysis_period': f"{start_date} to {end_date}"
        }
        
        # Load specific player data if requested
        if player_name:
            player_correlation = data_loader.get_player_correlation_analysis(player_name, start_date, end_date)
            player_models = data_loader.get_player_prediction_models(player_name, start_date, end_date)
            player_timeline = data_loader.get_timeline_data(player_name, start_date, end_date)
            
            result['player_data'] = {
                'correlation': player_correlation,
                'models': player_models,
                'timeline': player_timeline
            }
        
        return result
        
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to load dashboard data: {str(e)}'
        }


if __name__ == "__main__":
    # Test the data loader
    print("Testing Dashboard Data Loader...")
    
    result = load_dashboard_data("2023-10-01", "2024-05-31", "lebron_james")
    
    if result['success']:
        print("✅ Dashboard data loaded successfully!")
        print(f"📊 Analysis period: {result['analysis_period']}")
        print(f"📈 Metrics: {result['metrics']}")
        print(f"👥 Players in comparison: {len(result['comparison_data'])}")
    else:
        print(f"❌ Data loading failed: {result['error']}")
