"""
Player Mention Filter for Lakers Sentiment Analysis
Filters players based on mention threshold requirements for correlation analysis
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processors.database_manager import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PlayerMentionFilter:
    """
    Filters players based on mention frequency thresholds
    Ensures sufficient data for meaningful correlation analysis
    """
    
    def __init__(self, min_mentions_per_day: int = 10, min_days: int = 7):
        self.db_manager = DatabaseManager()
        self.min_mentions_per_day = min_mentions_per_day
        self.min_days = min_days
        
        logger.info(f"PlayerMentionFilter initialized: min {min_mentions_per_day} mentions/day for {min_days} days")
    
    def get_qualifying_players(self, start_date: str = "2023-10-01", 
                             end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Get players who meet the mention threshold requirements
        
        Args:
            start_date: Start date for analysis period
            end_date: End date for analysis period
            
        Returns:
            Dictionary with qualifying players and their statistics
        """
        try:
            logger.info(f"Filtering players for period {start_date} to {end_date}")
            
            # Query player mentions from database
            query = """
            SELECT 
                player_name,
                DATE(created_at) as mention_date,
                COUNT(*) as daily_mentions
            FROM player_mentions 
            WHERE created_at BETWEEN %s AND %s
            GROUP BY player_name, DATE(created_at)
            ORDER BY player_name, mention_date
            """
            
            # Execute query (this would need to be implemented in DatabaseManager)
            # For now, we'll simulate the data structure
            qualifying_players = self._analyze_player_mentions(start_date, end_date)
            
            logger.info(f"Found {len(qualifying_players)} qualifying players")
            
            return {
                'success': True,
                'qualifying_players': qualifying_players,
                'total_players_analyzed': len(qualifying_players),
                'analysis_period': {
                    'start_date': start_date,
                    'end_date': end_date
                },
                'thresholds': {
                    'min_mentions_per_day': self.min_mentions_per_day,
                    'min_days': self.min_days
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get qualifying players: {e}")
            return {'success': False, 'error': str(e)}
    
    def _analyze_player_mentions(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Analyze player mentions to find qualifying players
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            List of qualifying players with statistics
        """
        try:
            # This would query the actual database
            # For now, return mock data based on expected Lakers players
            qualifying_players = []
            
            # Expected high-activity Lakers players
            expected_players = [
                'lebron_james', 'anthony_davis', 'dangelo_russell', 
                'austin_reaves', 'rui_hachimura', 'jarred_vanderbilt'
            ]
            
            for player in expected_players:
                # Mock analysis - in real implementation, this would query the database
                player_stats = {
                    'player_name': player,
                    'total_mentions': 500,  # Mock data
                    'days_with_mentions': 45,  # Mock data
                    'avg_mentions_per_day': 11.1,  # Mock data
                    'max_daily_mentions': 25,  # Mock data
                    'meets_threshold': True,
                    'analysis_period': f"{start_date} to {end_date}",
                    'mention_frequency': 'high' if 500 > 300 else 'medium'
                }
                
                qualifying_players.append(player_stats)
            
            return qualifying_players
            
        except Exception as e:
            logger.error(f"Failed to analyze player mentions: {e}")
            return []
    
    def get_player_mention_timeline(self, player_name: str, 
                                  start_date: str = "2023-10-01",
                                  end_date: str = "2024-05-31") -> pd.DataFrame:
        """
        Get daily mention timeline for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date for timeline
            end_date: End date for timeline
            
        Returns:
            DataFrame with daily mention counts
        """
        try:
            logger.info(f"Getting mention timeline for {player_name}")
            
            # This would query the actual database
            # For now, return mock data
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Generate mock daily mention data
            import numpy as np
            np.random.seed(42)  # For reproducible results
            
            daily_mentions = []
            for date in date_range:
                # Mock data with some realistic patterns
                base_mentions = np.random.poisson(8)  # Base mention rate
                
                # Add some game-related spikes
                if date.weekday() in [0, 2, 4, 6]:  # Game days
                    base_mentions += np.random.poisson(5)
                
                daily_mentions.append({
                    'date': date,
                    'player_name': player_name,
                    'mention_count': max(0, base_mentions),
                    'is_game_day': date.weekday() in [0, 2, 4, 6]
                })
            
            df = pd.DataFrame(daily_mentions)
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"Generated timeline with {len(df)} days for {player_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get mention timeline for {player_name}: {e}")
            return pd.DataFrame()
    
    def get_mention_summary_stats(self, start_date: str = "2023-10-01",
                                end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Get summary statistics for all player mentions
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            qualifying_players = self.get_qualifying_players(start_date, end_date)
            
            if not qualifying_players['success']:
                return {'error': 'Failed to get qualifying players'}
            
            players = qualifying_players['qualifying_players']
            
            if not players:
                return {'error': 'No qualifying players found'}
            
            # Calculate summary statistics
            total_mentions = sum(p['total_mentions'] for p in players)
            avg_mentions_per_player = total_mentions / len(players)
            
            high_frequency_players = [p for p in players if p['mention_frequency'] == 'high']
            
            summary = {
                'total_qualifying_players': len(players),
                'total_mentions': total_mentions,
                'avg_mentions_per_player': round(avg_mentions_per_player, 2),
                'high_frequency_players': len(high_frequency_players),
                'analysis_period_days': (datetime.strptime(end_date, '%Y-%m-%d') - 
                                       datetime.strptime(start_date, '%Y-%m-%d')).days,
                'top_players': sorted(players, key=lambda x: x['total_mentions'], reverse=True)[:5]
            }
            
            logger.info(f"Summary stats: {summary['total_qualifying_players']} players, {summary['total_mentions']} total mentions")
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get mention summary stats: {e}")
            return {'error': str(e)}


def filter_players_by_mentions(min_mentions: int = 10, min_days: int = 7) -> Dict[str, Any]:
    """
    Main function to filter players by mention requirements
    
    Args:
        min_mentions: Minimum mentions per day
        min_days: Minimum days with mentions
        
    Returns:
        Filtering results
    """
    filter_obj = PlayerMentionFilter(min_mentions, min_days)
    return filter_obj.get_qualifying_players()


if __name__ == "__main__":
    # Test the player mention filter
    print("Testing Player Mention Filter...")
    result = filter_players_by_mentions()
    
    if result['success']:
        print("✅ Player filtering completed successfully!")
        print(f"📊 Qualifying players: {result['total_players_analyzed']}")
        for player in result['qualifying_players'][:3]:
            print(f"  - {player['player_name']}: {player['total_mentions']} mentions")
    else:
        print(f"❌ Filtering failed: {result['error']}")
