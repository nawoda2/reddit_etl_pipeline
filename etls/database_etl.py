"""
Database ETL Module for Lakers Sentiment Analysis Project
Handles PostgreSQL database operations for sentiment data and NBA player statistics
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, Boolean
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import json

from utils.constants import (
    DATABASE_HOST, DATABASE_NAME, DATABASE_PORT, 
    DATABASE_USER, DATABASE_PASSWORD
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class RedditPost(Base):
    """Reddit post data model"""
    __tablename__ = 'reddit_posts'
    
    id = Column(String, primary_key=True)
    title = Column(Text)
    selftext = Column(Text)
    author = Column(String)
    subreddit = Column(String)
    score = Column(Integer)
    num_comments = Column(Integer)
    created_utc = Column(DateTime)
    url = Column(Text)
    upvote_ratio = Column(Float)
    over_18 = Column(Boolean)
    edited = Column(Boolean)
    spoiler = Column(Boolean)
    stickied = Column(Boolean)
    created_at = Column(DateTime, default=datetime.utcnow)

class PlayerMention(Base):
    """Player mention data model"""
    __tablename__ = 'player_mentions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String)
    player_name = Column(String)
    mention_context = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class SentimentScore(Base):
    """Sentiment analysis results data model"""
    __tablename__ = 'sentiment_scores'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(String)
    player_name = Column(String)
    vader_positive = Column(Float)
    vader_negative = Column(Float)
    vader_neutral = Column(Float)
    vader_compound = Column(Float)
    textblob_polarity = Column(Float)
    textblob_subjectivity = Column(Float)
    textblob_sentiment = Column(String)
    transformer_positive = Column(Float)
    transformer_negative = Column(Float)
    transformer_neutral = Column(Float)
    transformer_sentiment = Column(String)
    analysis_timestamp = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

class PlayerPerformance(Base):
    """NBA player performance data model"""
    __tablename__ = 'player_performance'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(String)
    game_date = Column(DateTime)
    points = Column(Integer)
    rebounds = Column(Integer)
    assists = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    field_goals_made = Column(Integer)
    field_goals_attempted = Column(Integer)
    three_pointers_made = Column(Integer)
    three_pointers_attempted = Column(Integer)
    free_throws_made = Column(Integer)
    free_throws_attempted = Column(Integer)
    minutes_played = Column(Float)
    plus_minus = Column(Integer)
    game_result = Column(String)  # 'W' or 'L'
    opponent = Column(String)
    season = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class CorrelationAnalysis(Base):
    """Sentiment vs performance correlation analysis results"""
    __tablename__ = 'correlation_analysis'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(String)
    analysis_date = Column(DateTime)
    correlation_type = Column(String)  # 'sentiment_vs_points', 'sentiment_vs_win', etc.
    correlation_coefficient = Column(Float)
    p_value = Column(Float)
    sample_size = Column(Integer)
    analysis_period_days = Column(Integer)
    additional_metrics = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)

class DatabaseManager:
    """Database manager for Lakers sentiment analysis project"""
    
    def __init__(self):
        self.engine = None
        self.Session = None
        self._connect()
    
    def _connect(self):
        """Establish database connection"""
        try:
            connection_string = (
                f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@"
                f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
            )
            
            self.engine = create_engine(connection_string, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def drop_tables(self):
        """Drop all database tables"""
        try:
            Base.metadata.drop_all(self.engine)
            logger.info("Database tables dropped successfully")
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")
            raise
    
    def insert_reddit_posts(self, df: pd.DataFrame) -> bool:
        """
        Insert Reddit posts into database
        
        Args:
            df: DataFrame containing Reddit post data
            
        Returns:
            Boolean indicating success
        """
        try:
            session = self.Session()
            
            for _, row in df.iterrows():
                post = RedditPost(
                    id=row['id'],
                    title=row.get('title', ''),
                    selftext=row.get('selftext', ''),
                    author=str(row.get('author', '')),
                    subreddit=row.get('subreddit', ''),
                    score=int(row.get('score', 0)),
                    num_comments=int(row.get('num_comments', 0)),
                    created_utc=row.get('created_utc'),
                    url=row.get('url', ''),
                    upvote_ratio=float(row.get('upvote_ratio', 0.0)),
                    over_18=bool(row.get('over_18', False)),
                    edited=bool(row.get('edited', False)),
                    spoiler=bool(row.get('spoiler', False)),
                    stickied=bool(row.get('stickied', False))
                )
                session.add(post)
            
            session.commit()
            session.close()
            
            logger.info(f"Successfully inserted {len(df)} Reddit posts")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert Reddit posts: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def insert_sentiment_scores(self, df: pd.DataFrame) -> bool:
        """
        Insert sentiment analysis results into database
        
        Args:
            df: DataFrame containing sentiment analysis results
            
        Returns:
            Boolean indicating success
        """
        try:
            session = self.Session()
            
            for _, row in df.iterrows():
                # Handle multiple player mentions per post
                mentioned_players = row.get('mentioned_players', [])
                if not isinstance(mentioned_players, list):
                    mentioned_players = []
                
                for player in mentioned_players:
                    sentiment = SentimentScore(
                        post_id=row['id'],
                        player_name=player,
                        vader_positive=float(row.get('vader_positive', 0.0)),
                        vader_negative=float(row.get('vader_negative', 0.0)),
                        vader_neutral=float(row.get('vader_neutral', 1.0)),
                        vader_compound=float(row.get('vader_compound', 0.0)),
                        textblob_polarity=float(row.get('textblob_polarity', 0.0)),
                        textblob_subjectivity=float(row.get('textblob_subjectivity', 0.0)),
                        textblob_sentiment=str(row.get('textblob_sentiment', 'neutral')),
                        transformer_positive=float(row.get('transformer_positive', 0.0)),
                        transformer_negative=float(row.get('transformer_negative', 0.0)),
                        transformer_neutral=float(row.get('transformer_neutral', 1.0)),
                        transformer_sentiment=str(row.get('transformer_sentiment', 'neutral')),
                        analysis_timestamp=datetime.fromisoformat(
                            row.get('analysis_timestamp', datetime.now().isoformat())
                        )
                    )
                    session.add(sentiment)
            
            session.commit()
            session.close()
            
            logger.info(f"Successfully inserted sentiment scores for {len(df)} posts")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert sentiment scores: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def insert_player_performance(self, df: pd.DataFrame) -> bool:
        """
        Insert NBA player performance data into database
        
        Args:
            df: DataFrame containing player performance data
            
        Returns:
            Boolean indicating success
        """
        try:
            session = self.Session()
            
            for _, row in df.iterrows():
                performance = PlayerPerformance(
                    player_name=row['player_name'],
                    game_date=row['game_date'],
                    points=int(row.get('points', 0)),
                    rebounds=int(row.get('rebounds', 0)),
                    assists=int(row.get('assists', 0)),
                    steals=int(row.get('steals', 0)),
                    blocks=int(row.get('blocks', 0)),
                    turnovers=int(row.get('turnovers', 0)),
                    field_goals_made=int(row.get('field_goals_made', 0)),
                    field_goals_attempted=int(row.get('field_goals_attempted', 0)),
                    three_pointers_made=int(row.get('three_pointers_made', 0)),
                    three_pointers_attempted=int(row.get('three_pointers_attempted', 0)),
                    free_throws_made=int(row.get('free_throws_made', 0)),
                    free_throws_attempted=int(row.get('free_throws_attempted', 0)),
                    minutes_played=float(row.get('minutes_played', 0.0)),
                    plus_minus=int(row.get('plus_minus', 0)),
                    game_result=str(row.get('game_result', '')),
                    opponent=str(row.get('opponent', '')),
                    season=str(row.get('season', ''))
                )
                session.add(performance)
            
            session.commit()
            session.close()
            
            logger.info(f"Successfully inserted {len(df)} player performance records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert player performance data: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def get_player_sentiment_summary(self, player_name: str, days: int = 30) -> pd.DataFrame:
        """
        Get sentiment summary for a specific player
        
        Args:
            player_name: Name of the player
            days: Number of days to look back
            
        Returns:
            DataFrame with sentiment summary
        """
        try:
            query = text("""
                SELECT 
                    DATE(analysis_timestamp) as date,
                    COUNT(*) as mention_count,
                    AVG(vader_compound) as avg_vader_compound,
                    AVG(textblob_polarity) as avg_textblob_polarity,
                    AVG(textblob_subjectivity) as avg_textblob_subjectivity,
                    COUNT(CASE WHEN vader_compound > 0.05 THEN 1 END) as positive_mentions,
                    COUNT(CASE WHEN vader_compound < -0.05 THEN 1 END) as negative_mentions,
                    COUNT(CASE WHEN vader_compound BETWEEN -0.05 AND 0.05 THEN 1 END) as neutral_mentions
                FROM sentiment_scores 
                WHERE player_name = :player_name 
                AND analysis_timestamp >= NOW() - INTERVAL ':days days'
                GROUP BY DATE(analysis_timestamp)
                ORDER BY date DESC
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"player_name": player_name, "days": days})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get player sentiment summary: {e}")
            return pd.DataFrame()
    
    def get_player_performance_summary(self, player_name: str, days: int = 30) -> pd.DataFrame:
        """
        Get performance summary for a specific player
        
        Args:
            player_name: Name of the player
            days: Number of days to look back
            
        Returns:
            DataFrame with performance summary
        """
        try:
            query = text("""
                SELECT 
                    game_date,
                    points,
                    rebounds,
                    assists,
                    steals,
                    blocks,
                    turnovers,
                    field_goals_made,
                    field_goals_attempted,
                    three_pointers_made,
                    three_pointers_attempted,
                    free_throws_made,
                    free_throws_attempted,
                    minutes_played,
                    plus_minus,
                    game_result,
                    opponent
                FROM player_performance 
                WHERE player_name = :player_name 
                AND game_date >= NOW() - INTERVAL ':days days'
                ORDER BY game_date DESC
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"player_name": player_name, "days": days})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get player performance summary: {e}")
            return pd.DataFrame()
    
    def calculate_sentiment_performance_correlation(self, player_name: str, days: int = 30) -> Dict[str, Any]:
        """
        Calculate correlation between sentiment and performance for a player
        
        Args:
            player_name: Name of the player
            days: Number of days to analyze
            
        Returns:
            Dictionary with correlation results
        """
        try:
            # Get sentiment data
            sentiment_df = self.get_player_sentiment_summary(player_name, days)
            
            # Get performance data
            performance_df = self.get_player_performance_summary(player_name, days)
            
            if sentiment_df.empty or performance_df.empty:
                return {"error": "Insufficient data for correlation analysis"}
            
            # Merge data by date
            sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
            performance_df['game_date'] = pd.to_datetime(performance_df['game_date'])
            
            merged_df = pd.merge(
                sentiment_df, 
                performance_df, 
                left_on='date', 
                right_on='game_date', 
                how='inner'
            )
            
            if merged_df.empty:
                return {"error": "No matching dates between sentiment and performance data"}
            
            # Calculate correlations
            correlations = {}
            
            # Sentiment vs Points
            if 'points' in merged_df.columns and 'avg_vader_compound' in merged_df.columns:
                corr_coef = merged_df['avg_vader_compound'].corr(merged_df['points'])
                correlations['sentiment_vs_points'] = {
                    'correlation_coefficient': corr_coef,
                    'sample_size': len(merged_df)
                }
            
            # Sentiment vs Win/Loss
            if 'game_result' in merged_df.columns and 'avg_vader_compound' in merged_df.columns:
                # Convert W/L to numeric (1 for win, 0 for loss)
                merged_df['game_result_numeric'] = merged_df['game_result'].map({'W': 1, 'L': 0})
                corr_coef = merged_df['avg_vader_compound'].corr(merged_df['game_result_numeric'])
                correlations['sentiment_vs_win'] = {
                    'correlation_coefficient': corr_coef,
                    'sample_size': len(merged_df)
                }
            
            # Sentiment vs Plus/Minus
            if 'plus_minus' in merged_df.columns and 'avg_vader_compound' in merged_df.columns:
                corr_coef = merged_df['avg_vader_compound'].corr(merged_df['plus_minus'])
                correlations['sentiment_vs_plus_minus'] = {
                    'correlation_coefficient': corr_coef,
                    'sample_size': len(merged_df)
                }
            
            return {
                'player_name': player_name,
                'analysis_period_days': days,
                'correlations': correlations,
                'total_data_points': len(merged_df)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate correlation: {e}")
            return {"error": str(e)}
    
    def save_correlation_analysis(self, correlation_results: Dict[str, Any]) -> bool:
        """
        Save correlation analysis results to database
        
        Args:
            correlation_results: Results from correlation analysis
            
        Returns:
            Boolean indicating success
        """
        try:
            session = self.Session()
            
            for correlation_type, result in correlation_results.get('correlations', {}).items():
                analysis = CorrelationAnalysis(
                    player_name=correlation_results['player_name'],
                    analysis_date=datetime.now(),
                    correlation_type=correlation_type,
                    correlation_coefficient=result['correlation_coefficient'],
                    p_value=None,  # Could be calculated with scipy.stats
                    sample_size=result['sample_size'],
                    analysis_period_days=correlation_results['analysis_period_days'],
                    additional_metrics=json.dumps({
                        'total_data_points': correlation_results.get('total_data_points', 0)
                    })
                )
                session.add(analysis)
            
            session.commit()
            session.close()
            
            logger.info(f"Successfully saved correlation analysis for {correlation_results['player_name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save correlation analysis: {e}")
            if 'session' in locals():
                session.rollback()
                session.close()
            return False
    
    def get_all_players_sentiment_summary(self, days: int = 30) -> pd.DataFrame:
        """
        Get sentiment summary for all players
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame with sentiment summary for all players
        """
        try:
            query = text("""
                SELECT 
                    player_name,
                    COUNT(*) as total_mentions,
                    AVG(vader_compound) as avg_vader_compound,
                    AVG(textblob_polarity) as avg_textblob_polarity,
                    AVG(textblob_subjectivity) as avg_textblob_subjectivity,
                    COUNT(CASE WHEN vader_compound > 0.05 THEN 1 END) as positive_mentions,
                    COUNT(CASE WHEN vader_compound < -0.05 THEN 1 END) as negative_mentions,
                    COUNT(CASE WHEN vader_compound BETWEEN -0.05 AND 0.05 THEN 1 END) as neutral_mentions,
                    COUNT(CASE WHEN vader_compound > 0.05 THEN 1 END)::FLOAT / 
                    NULLIF(COUNT(CASE WHEN vader_compound < -0.05 THEN 1 END), 0) as sentiment_ratio
                FROM sentiment_scores 
                WHERE analysis_timestamp >= NOW() - INTERVAL ':days days'
                GROUP BY player_name
                HAVING COUNT(*) > 0
                ORDER BY total_mentions DESC
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"days": days})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get all players sentiment summary: {e}")
            return pd.DataFrame()
    
    def get_existing_post_ids(self, post_ids: List[str]) -> List[str]:
        """
        Get list of existing post IDs to avoid duplicates
        
        Args:
            post_ids: List of post IDs to check
            
        Returns:
            List of existing post IDs
        """
        try:
            if not post_ids:
                return []
            
            # Create placeholders for the IN clause
            placeholders = ','.join([f"'{pid}'" for pid in post_ids])
            
            query = text(f"""
                SELECT id FROM reddit_posts 
                WHERE id IN ({placeholders})
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                existing_ids = [row[0] for row in result.fetchall()]
            
            return existing_ids
            
        except Exception as e:
            logger.error(f"Failed to get existing post IDs: {e}")
            return []


def test_database_manager():
    """Test function for the database manager"""
    try:
        db_manager = DatabaseManager()
        
        # Test connection
        print("Database connection test: PASSED")
        
        # Create tables
        db_manager.create_tables()
        print("Table creation test: PASSED")
        
        # Test basic operations
        test_df = pd.DataFrame({
            'id': ['test1', 'test2'],
            'title': ['Test Post 1', 'Test Post 2'],
            'selftext': ['Test content 1', 'Test content 2'],
            'author': ['user1', 'user2'],
            'subreddit': ['lakers', 'lakers'],
            'score': [10, 20],
            'num_comments': [5, 10],
            'created_utc': [datetime.now(), datetime.now()],
            'url': ['http://test1.com', 'http://test2.com'],
            'upvote_ratio': [0.8, 0.9],
            'over_18': [False, False],
            'edited': [False, False],
            'spoiler': [False, False],
            'stickied': [False, False]
        })
        
        # Test insert
        success = db_manager.insert_reddit_posts(test_df)
        print(f"Insert test: {'PASSED' if success else 'FAILED'}")
        
        print("Database manager test completed successfully!")
        
    except Exception as e:
        print(f"Database manager test failed: {e}")


if __name__ == "__main__":
    test_database_manager()
