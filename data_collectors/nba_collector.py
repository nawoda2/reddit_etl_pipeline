"""
NBA Data ETL Module for Lakers Sentiment Analysis Project
Handles NBA player statistics and performance data collection
"""

import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBADataCollector:
    """
    NBA data collector for 2025-26 Lakers player statistics
    Uses multiple data sources for comprehensive player performance data
    """
    
    def __init__(self):
        self.base_url = "https://stats.nba.com/stats"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nba.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site'
        }
        
        # Lakers team ID
        self.lakers_team_id = 1610612747
        
        # Current Lakers roster (as of 2025-26 season)
        self.lakers_roster = {
            # Starting Lineup
            'luka_doncic': {
                'full_name': 'Luka Doncic',
                'player_id': 1629029,
                'jersey_number': 77,
                'position': 'PG'
            },
            'austin_reaves': {
                'full_name': 'Austin Reaves',
                'player_id': 1630559,
                'jersey_number': 15,
                'position': 'SG'
            },
            'lebron_james': {
                'full_name': 'LeBron James',
                'player_id': 2544,
                'jersey_number': 23,
                'position': 'SF'
            },
            'rui_hachimura': {
                'full_name': 'Rui Hachimura',
                'player_id': 1629067,
                'jersey_number': 28,
                'position': 'PF'
            },
            'deandre_ayton': {
                'full_name': 'Deandre Ayton',
                'player_id': 203112,
                'jersey_number': 5,
                'position': 'C'
            },
            
            # Bench Players
            'marcus_smart': {
                'full_name': 'Marcus Smart',
                'player_id': 203935,
                'jersey_number': 36,
                'position': 'PG'
            },
            'gabe_vincent': {
                'full_name': 'Gabe Vincent',
                'player_id': 1629216,
                'jersey_number': 7,
                'position': 'PG'
            },
            'bronny_james': {
                'full_name': 'Bronny James',
                'player_id': 1631119,
                'jersey_number': 9,
                'position': 'PG'
            },
            'jarred_vanderbilt': {
                'full_name': 'Jarred Vanderbilt',
                'player_id': 1629020,
                'jersey_number': 2,
                'position': 'PF'
            },
            'dalton_knecht': {
                'full_name': 'Dalton Knecht',
                'player_id': 1631118,
                'jersey_number': 4,
                'position': 'SF'
            },
            'jake_laravia': {
                'full_name': 'Jake LaRavia',
                'player_id': 1631117,
                'jersey_number': 12,
                'position': 'SF'
            },
            'adou_thiero': {
                'full_name': 'Adou Thiero',
                'player_id': 1631116,
                'jersey_number': 1,
                'position': 'SF'
            },
            'jaxson_hayes': {
                'full_name': 'Jaxson Hayes',
                'player_id': 1629637,
                'jersey_number': 11,
                'position': 'C'
            },
            'maxi_kleber': {
                'full_name': 'Maxi Kleber',
                'player_id': 1628467,
                'jersey_number': 14,
                'position': 'C'
            },
            
            # Two-Way Players
            'christian_koloko': {
                'full_name': 'Christian Koloko',
                'player_id': 1631115,
                'jersey_number': 10,
                'position': 'C'
            },
            'chris_manon': {
                'full_name': 'Chris Manon',
                'player_id': 1631114,
                'jersey_number': 30,
                'position': 'SG'
            },
            
            # Additional Players
            'nick_smith_jr': {
                'full_name': 'Nick Smith Jr.',
                'player_id': 1631113,
                'jersey_number': 8,
                'position': 'PG'
            },
            'nate_williams': {
                'full_name': 'Nate Williams',
                'player_id': 1631112,
                'jersey_number': 19,
                'position': 'SG'
            },
            'rj_davis': {
                'full_name': 'RJ Davis',
                'player_id': 1631111,
                'jersey_number': 99,
                'position': 'PG'
            },
            'augustas_marciulionis': {
                'full_name': 'Augustas Marciulionis',
                'player_id': 1631110,
                'jersey_number': 96,
                'position': 'PG'
            }
        }
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """
        Make API request to NBA stats endpoint
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            JSON response or None if failed
        """
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            # Add delay to respect rate limits
            time.sleep(0.5)
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in API request: {e}")
            return None
    
    def get_player_game_logs(self, player_id: int, season: str = "2025-26", 
                           start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get game logs for a specific player
        
        Args:
            player_id: NBA player ID
            season: NBA season (e.g., "2025-26")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with player game logs
        """
        try:
            params = {
                'PlayerID': player_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00'
            }
            
            if start_date:
                params['DateFrom'] = start_date
            if end_date:
                params['DateTo'] = end_date
            
            response = self._make_request('playergamelog', params)
            
            if not response or 'resultSets' not in response:
                logger.error(f"No data returned for player {player_id}")
                return pd.DataFrame()
            
            # Extract game log data
            game_logs = response['resultSets'][0]
            headers = game_logs['headers']
            rows = game_logs['rowSet']
            
            if not rows:
                logger.info(f"No game logs found for player {player_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=headers)
            
            # Convert date column
            if 'GAME_DATE' in df.columns:
                df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
            
            logger.info(f"Retrieved {len(df)} game logs for player {player_id}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get game logs for player {player_id}: {e}")
            return pd.DataFrame()
    
    def get_team_game_logs(self, team_id: int = None, season: str = "2025-26",
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get game logs for Lakers team
        
        Args:
            team_id: NBA team ID (defaults to Lakers)
            season: NBA season
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with team game logs
        """
        if team_id is None:
            team_id = self.lakers_team_id
        
        try:
            params = {
                'TeamID': team_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00'
            }
            
            if start_date:
                params['DateFrom'] = start_date
            if end_date:
                params['DateTo'] = end_date
            
            response = self._make_request('teamgamelog', params)
            
            if not response or 'resultSets' not in response:
                logger.error(f"No data returned for team {team_id}")
                return pd.DataFrame()
            
            # Extract team game log data
            game_logs = response['resultSets'][0]
            headers = game_logs['headers']
            rows = game_logs['rowSet']
            
            if not rows:
                logger.info(f"No game logs found for team {team_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=headers)
            
            # Convert date column
            if 'GAME_DATE' in df.columns:
                df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
            
            logger.info(f"Retrieved {len(df)} team game logs for team {team_id}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to get team game logs for team {team_id}: {e}")
            return pd.DataFrame()
    
    def get_player_season_stats(self, player_id: int, season: str = "2025-26") -> Dict[str, Any]:
        """
        Get season statistics for a specific player
        
        Args:
            player_id: NBA player ID
            season: NBA season
            
        Returns:
            Dictionary with player season stats
        """
        try:
            params = {
                'PlayerID': player_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00',
                'PerMode': 'PerGame'
            }
            
            response = self._make_request('playergamelog', params)
            
            if not response or 'resultSets' not in response:
                logger.error(f"No season stats returned for player {player_id}")
                return {}
            
            # Extract season stats
            season_stats = response['resultSets'][0]
            headers = season_stats['headers']
            rows = season_stats['rowSet']
            
            if not rows:
                logger.info(f"No season stats found for player {player_id}")
                return {}
            
            # Calculate season averages
            df = pd.DataFrame(rows, columns=headers)
            
            # Convert numeric columns
            numeric_columns = ['PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'FGM', 'FGA', 
                             'FG3M', 'FG3A', 'FTM', 'FTA', 'MIN', 'PLUS_MINUS']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate averages
            season_averages = {}
            for col in numeric_columns:
                if col in df.columns:
                    season_averages[col.lower()] = df[col].mean()
            
            # Add game count
            season_averages['games_played'] = len(df)
            
            # Add win/loss record
            if 'WL' in df.columns:
                wins = len(df[df['WL'] == 'W'])
                losses = len(df[df['WL'] == 'L'])
                season_averages['wins'] = wins
                season_averages['losses'] = losses
                season_averages['win_percentage'] = wins / (wins + losses) if (wins + losses) > 0 else 0
            
            logger.info(f"Retrieved season stats for player {player_id}")
            return season_averages
            
        except Exception as e:
            logger.error(f"Failed to get season stats for player {player_id}: {e}")
            return {}
    
    def get_all_lakers_player_stats(self, season: str = "2025-26",
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get statistics for all Lakers players
        
        Args:
            season: NBA season
            start_date: Start date for data collection
            end_date: End date for data collection
            
        Returns:
            DataFrame with all Lakers player statistics
        """
        all_player_stats = []
        
        for player_key, player_info in self.lakers_roster.items():
            if player_info['player_id'] == 1503:  # Skip placeholder entries
                continue
                
            logger.info(f"Collecting stats for {player_info['full_name']}")
            
            # Get game logs
            game_logs = self.get_player_game_logs(
                player_info['player_id'], 
                season, 
                start_date, 
                end_date
            )
            
            if game_logs.empty:
                logger.warning(f"No game logs found for {player_info['full_name']}")
                continue
            
            # Process each game
            for _, game in game_logs.iterrows():
                player_stat = {
                    'player_name': player_key,
                    'player_full_name': player_info['full_name'],
                    'player_id': player_info['player_id'],
                    'jersey_number': player_info['jersey_number'],
                    'position': player_info['position'],
                    'game_date': game.get('GAME_DATE'),
                    'game_id': game.get('Game_ID'),
                    'matchup': game.get('MATCHUP', ''),
                    'wl': game.get('WL', ''),
                    'min': game.get('MIN', 0),
                    'fgm': game.get('FGM', 0),
                    'fga': game.get('FGA', 0),
                    'fg_pct': game.get('FG_PCT', 0),
                    'fg3m': game.get('FG3M', 0),
                    'fg3a': game.get('FG3A', 0),
                    'fg3_pct': game.get('FG3_PCT', 0),
                    'ftm': game.get('FTM', 0),
                    'fta': game.get('FTA', 0),
                    'ft_pct': game.get('FT_PCT', 0),
                    'oreb': game.get('OREB', 0),
                    'dreb': game.get('DREB', 0),
                    'reb': game.get('REB', 0),
                    'ast': game.get('AST', 0),
                    'stl': game.get('STL', 0),
                    'blk': game.get('BLK', 0),
                    'tov': game.get('TOV', 0),
                    'pf': game.get('PF', 0),
                    'pts': game.get('PTS', 0),
                    'plus_minus': game.get('PLUS_MINUS', 0),
                    'season': season
                }
                
                # Extract opponent from matchup
                if 'vs.' in game.get('MATCHUP', ''):
                    player_stat['opponent'] = game.get('MATCHUP', '').split('vs. ')[1]
                    player_stat['home_away'] = 'HOME'
                elif '@' in game.get('MATCHUP', ''):
                    player_stat['opponent'] = game.get('MATCHUP', '').split('@ ')[1]
                    player_stat['home_away'] = 'AWAY'
                else:
                    player_stat['opponent'] = ''
                    player_stat['home_away'] = ''
                
                # Convert game result
                player_stat['game_result'] = 'W' if game.get('WL') == 'W' else 'L'
                
                all_player_stats.append(player_stat)
            
            # Add delay between players to respect rate limits
            time.sleep(1)
        
        if not all_player_stats:
            logger.warning("No player statistics collected")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_player_stats)
        
        # Convert date column
        if 'game_date' in df.columns:
            df['game_date'] = pd.to_datetime(df['game_date'])
        
        # Convert numeric columns
        numeric_columns = ['min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct', 
                          'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb', 'ast', 
                          'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"Collected statistics for {len(df)} player games")
        return df
    
    def get_recent_lakers_games(self, days: int = 30) -> pd.DataFrame:
        """
        Get recent Lakers games within specified days
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame with recent Lakers games
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        return self.get_team_game_logs(
            start_date=start_date_str,
            end_date=end_date_str
        )
    
    def get_player_performance_summary(self, player_name: str, days: int = 30) -> Dict[str, Any]:
        """
        Get performance summary for a specific player
        
        Args:
            player_name: Player name key from roster
            days: Number of days to look back
            
        Returns:
            Dictionary with performance summary
        """
        if player_name not in self.lakers_roster:
            logger.error(f"Player {player_name} not found in Lakers roster")
            return {}
        
        player_info = self.lakers_roster[player_name]
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Get game logs
        game_logs = self.get_player_game_logs(
            player_info['player_id'],
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        if game_logs.empty:
            return {
                'player_name': player_name,
                'player_full_name': player_info['full_name'],
                'games_played': 0,
                'error': 'No games found in specified period'
            }
        
        # Calculate summary statistics
        summary = {
            'player_name': player_name,
            'player_full_name': player_info['full_name'],
            'games_played': len(game_logs),
            'avg_points': game_logs['PTS'].mean() if 'PTS' in game_logs.columns else 0,
            'avg_rebounds': game_logs['REB'].mean() if 'REB' in game_logs.columns else 0,
            'avg_assists': game_logs['AST'].mean() if 'AST' in game_logs.columns else 0,
            'avg_steals': game_logs['STL'].mean() if 'STL' in game_logs.columns else 0,
            'avg_blocks': game_logs['BLK'].mean() if 'BLK' in game_logs.columns else 0,
            'avg_turnovers': game_logs['TOV'].mean() if 'TOV' in game_logs.columns else 0,
            'avg_minutes': game_logs['MIN'].mean() if 'MIN' in game_logs.columns else 0,
            'avg_plus_minus': game_logs['PLUS_MINUS'].mean() if 'PLUS_MINUS' in game_logs.columns else 0,
            'wins': len(game_logs[game_logs['WL'] == 'W']) if 'WL' in game_logs.columns else 0,
            'losses': len(game_logs[game_logs['WL'] == 'L']) if 'WL' in game_logs.columns else 0,
            'win_percentage': len(game_logs[game_logs['WL'] == 'W']) / len(game_logs) if 'WL' in game_logs.columns and len(game_logs) > 0 else 0
        }
        
        return summary


def test_nba_data_collector():
    """Test function for the NBA data collector"""
    try:
        collector = NBADataCollector()
        
        # Test getting LeBron's recent game logs
        print("Testing NBA Data Collector:")
        print("=" * 50)
        
        lebron_id = collector.lakers_roster['lebron']['player_id']
        print(f"LeBron James Player ID: {lebron_id}")
        
        # Get recent game logs (last 30 days)
        recent_games = collector.get_recent_lakers_games(30)
        print(f"Recent Lakers games: {len(recent_games)} games found")
        
        if not recent_games.empty:
            print(f"Latest game: {recent_games.iloc[0]['GAME_DATE']}")
            print(f"Latest result: {recent_games.iloc[0]['WL']}")
        
        # Test player performance summary
        lebron_summary = collector.get_player_performance_summary('lebron', 30)
        print(f"LeBron performance summary: {lebron_summary}")
        
        print("NBA Data Collector test completed successfully!")
        
    except Exception as e:
        print(f"NBA Data Collector test failed: {e}")


if __name__ == "__main__":
    test_nba_data_collector()
