"""
Historical NBA Data Collection for Lakers Sentiment Analysis
Collects 2023-24 Lakers season player statistics for correlation analysis
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collectors.nba_collector import NBADataCollector
from data_processors.database_manager import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalNBACollector:
    """
    Historical NBA data collector for 2023-24 Lakers season
    Collects comprehensive player statistics for all Lakers players
    """
    
    def __init__(self):
        self.nba_collector = NBADataCollector()
        self.db_manager = DatabaseManager()
        
        # 2023-24 Lakers roster (all players who played during the season)
        self.lakers_roster_2023_24 = {
            'lebron_james': {
                'full_name': 'LeBron James',
                'player_id': 2544,
                'jersey_number': 23,
                'position': 'SF'
            },
            'anthony_davis': {
                'full_name': 'Anthony Davis',
                'player_id': 203076,
                'jersey_number': 3,
                'position': 'PF'
            },
            'dangelo_russell': {
                'full_name': 'D\'Angelo Russell',
                'player_id': 1626156,
                'jersey_number': 1,
                'position': 'PG'
            },
            'austin_reaves': {
                'full_name': 'Austin Reaves',
                'player_id': 1630559,
                'jersey_number': 15,
                'position': 'SG'
            },
            'rui_hachimura': {
                'full_name': 'Rui Hachimura',
                'player_id': 1629067,
                'jersey_number': 28,
                'position': 'PF'
            },
            'jarred_vanderbilt': {
                'full_name': 'Jarred Vanderbilt',
                'player_id': 1629020,
                'jersey_number': 2,
                'position': 'PF'
            },
            'cam_reddish': {
                'full_name': 'Cam Reddish',
                'player_id': 1629629,
                'jersey_number': 5,
                'position': 'SF'
            },
            'jaxson_hayes': {
                'full_name': 'Jaxson Hayes',
                'player_id': 1629637,
                'jersey_number': 11,
                'position': 'C'
            },
            'taurean_prince': {
                'full_name': 'Taurean Prince',
                'player_id': 1627752,
                'jersey_number': 12,
                'position': 'SF'
            },
            'christian_wood': {
                'full_name': 'Christian Wood',
                'player_id': 1626174,
                'jersey_number': 35,
                'position': 'C'
            },
            'gabe_vincent': {
                'full_name': 'Gabe Vincent',
                'player_id': 1629216,
                'jersey_number': 7,
                'position': 'PG'
            },
            'max_christie': {
                'full_name': 'Max Christie',
                'player_id': 1631108,
                'jersey_number': 10,
                'position': 'SG'
            },
            'jalen_hood_schifino': {
                'full_name': 'Jalen Hood-Schifino',
                'player_id': 1641717,
                'jersey_number': 0,
                'position': 'PG'
            },
            'dangelo_russell_2': {  # D'Lo was traded back mid-season
                'full_name': 'D\'Angelo Russell',
                'player_id': 1626156,
                'jersey_number': 1,
                'position': 'PG'
            },
            'spencer_dinwiddie': {
                'full_name': 'Spencer Dinwiddie',
                'player_id': 203915,
                'jersey_number': 26,
                'position': 'PG'
            },
            'skylar_mays': {
                'full_name': 'Skylar Mays',
                'player_id': 1630219,
                'jersey_number': 4,
                'position': 'PG'
            },
            'harry_giles': {
                'full_name': 'Harry Giles',
                'player_id': 1628385,
                'jersey_number': 20,
                'position': 'C'
            },
            'dylan_windler': {
                'full_name': 'Dylan Windler',
                'player_id': 1629685,
                'jersey_number': 9,
                'position': 'SF'
            }
        }
        
        logger.info("Historical NBA collector initialized with 2023-24 Lakers roster")
    
    def collect_season_data(self, season: str = "2023-24") -> Dict[str, Any]:
        """
        Collect NBA data for entire 2023-24 Lakers season
        
        Args:
            season: NBA season (e.g., "2023-24")
            
        Returns:
            Dictionary with collection results
        """
        try:
            logger.info(f"Starting historical NBA data collection for {season} season")
            
            # Define season date range
            season_start = "2023-10-01"
            season_end = "2024-05-31"
            
            all_player_stats = []
            collection_stats = {
                'total_games': 0,
                'players_processed': 0,
                'players_failed': 0,
                'season_start': season_start,
                'season_end': season_end
            }
            
            # Collect data for each player
            for player_key, player_info in self.lakers_roster_2023_24.items():
                try:
                    logger.info(f"Collecting stats for {player_info['full_name']}")
                    
                    # Get player game logs for the season
                    game_logs = self.nba_collector.get_player_game_logs(
                        player_info['player_id'],
                        season=season,
                        start_date=season_start,
                        end_date=season_end
                    )
                    
                    if game_logs.empty:
                        logger.warning(f"No game logs found for {player_info['full_name']}")
                        collection_stats['players_failed'] += 1
                        continue
                    
                    # Process each game
                    for _, game in game_logs.iterrows():
                        player_stat = self._process_game_stat(player_info, game, season)
                        all_player_stats.append(player_stat)
                    
                    collection_stats['players_processed'] += 1
                    collection_stats['total_games'] += len(game_logs)
                    
                    logger.info(f"  Collected {len(game_logs)} games for {player_info['full_name']}")
                    
                    # Add delay to respect API rate limits
                    import time
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Failed to collect data for {player_info['full_name']}: {e}")
                    collection_stats['players_failed'] += 1
                    continue
            
            # Convert to DataFrame and store in database
            if all_player_stats:
                df = pd.DataFrame(all_player_stats)
                
                # Convert date column
                if 'game_date' in df.columns:
                    df['game_date'] = pd.to_datetime(df['game_date'])
                
                # Convert numeric columns
                numeric_columns = ['points', 'rebounds', 'assists', 'steals', 'blocks', 
                                 'turnovers', 'minutes_played', 'plus_minus']
                
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Store in database
                success = self.db_manager.insert_player_performance(df)
                
                if success:
                    logger.info(f"Successfully stored {len(df)} player performance records")
                    collection_stats['records_stored'] = len(df)
                else:
                    logger.error("Failed to store player performance data")
                    collection_stats['records_stored'] = 0
                
                # Save to CSV for backup
                csv_file = f"data/output/lakers_player_stats_{season}.csv"
                df.to_csv(csv_file, index=False)
                collection_stats['csv_file'] = csv_file
                
                logger.info(f"Saved backup CSV to {csv_file}")
            
            logger.info(f"Historical NBA data collection completed: {collection_stats['total_games']} games, {collection_stats['players_processed']} players")
            
            return {
                'success': True,
                'stats': collection_stats,
                'dataframe': df if all_player_stats else pd.DataFrame()
            }
            
        except Exception as e:
            logger.error(f"Failed to collect historical NBA data: {e}")
            return {'success': False, 'error': str(e)}
    
    def _process_game_stat(self, player_info: Dict, game: pd.Series, season: str) -> Dict[str, Any]:
        """
        Process a single game stat into standardized format
        
        Args:
            player_info: Player information dictionary
            game: Game data from NBA API
            season: NBA season
            
        Returns:
            Processed game stat dictionary
        """
        return {
            'player_name': player_info['full_name'].lower().replace(' ', '_').replace('\'', ''),
            'player_full_name': player_info['full_name'],
            'player_id': player_info['player_id'],
            'jersey_number': player_info['jersey_number'],
            'position': player_info['position'],
            'game_date': game.get('GAME_DATE'),
            'game_id': game.get('Game_ID'),
            'matchup': game.get('MATCHUP', ''),
            'points': game.get('PTS', 0),
            'rebounds': game.get('REB', 0),
            'assists': game.get('AST', 0),
            'steals': game.get('STL', 0),
            'blocks': game.get('BLK', 0),
            'turnovers': game.get('TOV', 0),
            'field_goals_made': game.get('FGM', 0),
            'field_goals_attempted': game.get('FGA', 0),
            'three_pointers_made': game.get('FG3M', 0),
            'three_pointers_attempted': game.get('FG3A', 0),
            'free_throws_made': game.get('FTM', 0),
            'free_throws_attempted': game.get('FTA', 0),
            'minutes_played': game.get('MIN', 0),
            'plus_minus': game.get('PLUS_MINUS', 0),
            'game_result': 'W' if game.get('WL') == 'W' else 'L',
            'opponent': self._extract_opponent(game.get('MATCHUP', '')),
            'home_away': 'HOME' if 'vs.' in game.get('MATCHUP', '') else 'AWAY',
            'season': season
        }
    
    def _extract_opponent(self, matchup: str) -> str:
        """
        Extract opponent team name from matchup string
        
        Args:
            matchup: Matchup string (e.g., "LAL vs. GSW" or "LAL @ GSW")
            
        Returns:
            Opponent team name
        """
        if 'vs.' in matchup:
            return matchup.split('vs. ')[1]
        elif '@' in matchup:
            return matchup.split('@ ')[1]
        else:
            return ''
    
    def get_season_summary(self, season: str = "2023-24") -> Dict[str, Any]:
        """
        Get summary of collected season data
        
        Args:
            season: NBA season
            
        Returns:
            Dictionary with season summary
        """
        try:
            # Query database for season data
            query = """
            SELECT 
                player_name,
                COUNT(*) as games_played,
                AVG(points) as avg_points,
                AVG(rebounds) as avg_rebounds,
                AVG(assists) as avg_assists,
                SUM(CASE WHEN game_result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN game_result = 'L' THEN 1 ELSE 0 END) as losses
            FROM player_performance 
            WHERE season = %s
            GROUP BY player_name
            ORDER BY avg_points DESC
            """
            
            # This would need to be implemented in DatabaseManager
            # For now, return basic info
            return {
                'season': season,
                'total_players': len(self.lakers_roster_2023_24),
                'roster': list(self.lakers_roster_2023_24.keys())
            }
            
        except Exception as e:
            logger.error(f"Failed to get season summary: {e}")
            return {'error': str(e)}


def collect_historical_nba_data(season: str = "2023-24") -> Dict[str, Any]:
    """
    Main function to collect historical NBA data
    
    Args:
        season: NBA season to collect data for
        
    Returns:
        Collection results
    """
    collector = HistoricalNBACollector()
    return collector.collect_season_data(season)


if __name__ == "__main__":
    # Test the historical NBA data collection
    print("Starting historical NBA data collection...")
    result = collect_historical_nba_data("2023-24")
    
    if result['success']:
        print("✅ Historical NBA data collection completed successfully!")
        print(f"📊 Total games: {result['stats']['total_games']}")
        print(f"👥 Players processed: {result['stats']['players_processed']}")
        print(f"💾 Records stored: {result['stats'].get('records_stored', 0)}")
    else:
        print(f"❌ Collection failed: {result['error']}")
