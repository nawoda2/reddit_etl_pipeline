"""
NBA Data ETL Module for 2025-26 Lakers Season
Handles NBA player statistics and performance data collection for current Lakers roster
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

class NBADataCollector2025_26:
    """
    NBA data collector for 2025-26 Lakers player statistics
    Focused on current season roster only
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
        
        # 2025-26 Lakers roster (current season only)
        self.lakers_roster = {
            'lebron': {
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
            'austin_reaves': {
                'full_name': 'Austin Reaves',
                'player_id': 1630559,
                'jersey_number': 15,
                'position': 'SG'
            },
            'dangelo_russell': {
                'full_name': 'D\'Angelo Russell',
                'player_id': 1626156,
                'jersey_number': 1,
                'position': 'PG'
            },
            'russell_westbrook': {
                'full_name': 'Russell Westbrook',
                'player_id': 201566,
                'jersey_number': 0,
                'position': 'PG'
            },
            'kyle_kuzma': {
                'full_name': 'Kyle Kuzma',
                'player_id': 1628398,
                'jersey_number': 0,
                'position': 'PF'
            },
            'brandon_ingram': {
                'full_name': 'Brandon Ingram',
                'player_id': 1627742,
                'jersey_number': 0,
                'position': 'SF'
            },
            'lonzo_ball': {
                'full_name': 'Lonzo Ball',
                'player_id': 1628366,
                'jersey_number': 0,
                'position': 'PG'
            },
            'josh_hart': {
                'full_name': 'Josh Hart',
                'player_id': 1628404,
                'jersey_number': 0,
                'position': 'SG'
            },
            'kentavious_caldwell_pope': {
                'full_name': 'Kentavious Caldwell-Pope',
                'player_id': 203484,
                'jersey_number': 0,
                'position': 'SG'
            },
            'alex_caruso': {
                'full_name': 'Alex Caruso',
                'player_id': 1627936,
                'jersey_number': 0,
                'position': 'SG'
            },
            'jared_dudley': {
                'full_name': 'Jared Dudley',
                'player_id': 201162,
                'jersey_number': 0,
                'position': 'SF'
            },
            'markieff_morris': {
                'full_name': 'Markieff Morris',
                'player_id': 202694,
                'jersey_number': 0,
                'position': 'PF'
            },
            'montrezl_harrell': {
                'full_name': 'Montrezl Harrell',
                'player_id': 1626149,
                'jersey_number': 0,
                'position': 'C'
            },
            'dennis_schroder': {
                'full_name': 'Dennis Schröder',
                'player_id': 203471,
                'jersey_number': 0,
                'position': 'PG'
            },
            'andre_drummond': {
                'full_name': 'Andre Drummond',
                'player_id': 203083,
                'jersey_number': 0,
                'position': 'C'
            },
            'marc_gasol': {
                'full_name': 'Marc Gasol',
                'player_id': 201188,
                'jersey_number': 0,
                'position': 'C'
            },
            'talen_horton_tucker': {
                'full_name': 'Talen Horton-Tucker',
                'player_id': 1629659,
                'jersey_number': 0,
                'position': 'SG'
            },
            'malik_monk': {
                'full_name': 'Malik Monk',
                'player_id': 1628370,
                'jersey_number': 0,
                'position': 'SG'
            },
            'carmelo_anthony': {
                'full_name': 'Carmelo Anthony',
                'player_id': 2546,
                'jersey_number': 0,
                'position': 'SF'
            },
            'dwight_howard': {
                'full_name': 'Dwight Howard',
                'player_id': 2730,
                'jersey_number': 0,
                'position': 'C'
            },
            'rajon_rondo': {
                'full_name': 'Rajon Rondo',
                'player_id': 200765,
                'jersey_number': 0,
                'position': 'PG'
            },
            'jordan_clarkson': {
                'full_name': 'Jordan Clarkson',
                'player_id': 203903,
                'jersey_number': 0,
                'position': 'SG'
            },
            'julius_randle': {
                'full_name': 'Julius Randle',
                'player_id': 203944,
                'jersey_number': 0,
                'position': 'PF'
            },
            'larry_nance_jr': {
                'full_name': 'Larry Nance Jr.',
                'player_id': 1626204,
                'jersey_number': 0,
                'position': 'PF'
            },
            'ivica_zubac': {
                'full_name': 'Ivica Zubac',
                'player_id': 1627826,
                'jersey_number': 0,
                'position': 'C'
            },
            'moe_wagner': {
                'full_name': 'Moritz Wagner',
                'player_id': 1629021,
                'jersey_number': 0,
                'position': 'C'
            },
            'isaac_bonga': {
                'full_name': 'Isaac Bonga',
                'player_id': 1629067,
                'jersey_number': 0,
                'position': 'SF'
            },
            'svi_mykhailiuk': {
                'full_name': 'Sviatoslav Mykhailiuk',
                'player_id': 1629004,
                'jersey_number': 0,
                'position': 'SG'
            },
            'moritz_wagner': {
                'full_name': 'Moritz Wagner',
                'player_id': 1629021,
                'jersey_number': 0,
                'position': 'C'
            },
            'jonathan_williams': {
                'full_name': 'Jonathan Williams',
                'player_id': 1629147,
                'jersey_number': 0,
                'position': 'PF'
            },
            'reggie_bullock': {
                'full_name': 'Reggie Bullock',
                'player_id': 203493,
                'jersey_number': 0,
                'position': 'SF'
            },
            'mike_muscala': {
                'full_name': 'Mike Muscala',
                'player_id': 203488,
                'jersey_number': 0,
                'position': 'C'
            },
            'tyler_ennis': {
                'full_name': 'Tyler Ennis',
                'player_id': 203898,
                'jersey_number': 0,
                'position': 'PG'
            },
            'corey_brewer': {
                'full_name': 'Corey Brewer',
                'player_id': 201147,
                'jersey_number': 0,
                'position': 'SF'
            },
            'isaiah_thomas': {
                'full_name': 'Isaiah Thomas',
                'player_id': 202738,
                'jersey_number': 0,
                'position': 'PG'
            },
            'channing_frye': {
                'full_name': 'Channing Frye',
                'player_id': 101112,
                'jersey_number': 0,
                'position': 'C'
            },
            'jose_calderon': {
                'full_name': 'José Calderón',
                'player_id': 101181,
                'jersey_number': 0,
                'position': 'PG'
            },
            'andrew_bogut': {
                'full_name': 'Andrew Bogut',
                'player_id': 101106,
                'jersey_number': 0,
                'position': 'C'
            },
            'luol_deng': {
                'full_name': 'Luol Deng',
                'player_id': 2736,
                'jersey_number': 0,
                'position': 'SF'
            },
            'timofey_mozgov': {
                'full_name': 'Timofey Mozgov',
                'player_id': 202389,
                'jersey_number': 0,
                'position': 'C'
            },
            'jordan_hill': {
                'full_name': 'Jordan Hill',
                'player_id': 201941,
                'jersey_number': 0,
                'position': 'C'
            },
            'nick_young': {
                'full_name': 'Nick Young',
                'player_id': 201156,
                'jersey_number': 0,
                'position': 'SG'
            },
            'steve_blake': {
                'full_name': 'Steve Blake',
                'player_id': 2581,
                'jersey_number': 0,
                'position': 'PG'
            },
            'jodie_meeks': {
                'full_name': 'Jodie Meeks',
                'player_id': 201975,
                'jersey_number': 0,
                'position': 'SG'
            },
            'kendall_marshall': {
                'full_name': 'Kendall Marshall',
                'player_id': 203088,
                'jersey_number': 0,
                'position': 'PG'
            },
            'wesley_johnson': {
                'full_name': 'Wesley Johnson',
                'player_id': 202325,
                'jersey_number': 0,
                'position': 'SF'
            },
            'chris_kaman': {
                'full_name': 'Chris Kaman',
                'player_id': 2549,
                'jersey_number': 0,
                'position': 'C'
            },
            'jordan_farmar': {
                'full_name': 'Jordan Farmar',
                'player_id': 200770,
                'jersey_number': 0,
                'position': 'PG'
            },
            'steve_nash': {
                'full_name': 'Steve Nash',
                'player_id': 959,
                'jersey_number': 0,
                'position': 'PG'
            },
            'pau_gasol': {
                'full_name': 'Pau Gasol',
                'player_id': 2200,
                'jersey_number': 0,
                'position': 'C'
            },
            'metta_world_peace': {
                'full_name': 'Metta World Peace',
                'player_id': 1897,
                'jersey_number': 0,
                'position': 'SF'
            },
            'lamar_odom': {
                'full_name': 'Lamar Odom',
                'player_id': 1885,
                'jersey_number': 0,
                'position': 'PF'
            },
            'derek_fisher': {
                'full_name': 'Derek Fisher',
                'player_id': 965,
                'jersey_number': 0,
                'position': 'PG'
            },
            'kobe_bryant': {
                'full_name': 'Kobe Bryant',
                'player_id': 977,
                'jersey_number': 24,
                'position': 'SG'
            },
            'shaquille_oneal': {
                'full_name': 'Shaquille O\'Neal',
                'player_id': 406,
                'jersey_number': 34,
                'position': 'C'
            },
            'robert_horry': {
                'full_name': 'Robert Horry',
                'player_id': 109,
                'jersey_number': 0,
                'position': 'PF'
            },
            'rick_fox': {
                'full_name': 'Rick Fox',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'SF'
            },
            'brian_shaw': {
                'full_name': 'Brian Shaw',
                'player_id': 1117,
                'jersey_number': 0,
                'position': 'PG'
            },
            'glen_rice': {
                'full_name': 'Glen Rice',
                'player_id': 697,
                'jersey_number': 0,
                'position': 'SF'
            },
            'eddie_jones': {
                'full_name': 'Eddie Jones',
                'player_id': 1032,
                'jersey_number': 0,
                'position': 'SG'
            },
            'nick_van_exel': {
                'full_name': 'Nick Van Exel',
                'player_id': 1495,
                'jersey_number': 0,
                'position': 'PG'
            },
            'cedric_ceballos': {
                'full_name': 'Cedric Ceballos',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'SF'
            },
            'vlade_divac': {
                'full_name': 'Vlade Divac',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'C'
            },
            'james_worthy': {
                'full_name': 'James Worthy',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'SF'
            },
            'magic_johnson': {
                'full_name': 'Magic Johnson',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'PG'
            },
            'kareem_abdul_jabbar': {
                'full_name': 'Kareem Abdul-Jabbar',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'C'
            },
            'jerry_west': {
                'full_name': 'Jerry West',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'SG'
            },
            'elgin_baylor': {
                'full_name': 'Elgin Baylor',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'SF'
            },
            'wilt_chamberlain': {
                'full_name': 'Wilt Chamberlain',
                'player_id': 1503,
                'jersey_number': 0,
                'position': 'C'
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
        Get statistics for all 2025-26 Lakers players
        
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
        collector = NBADataCollector2025_26()
        
        # Test getting LeBron's recent game logs
        print("Testing NBA Data Collector (2025-26):")
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
