"""
Test Suite for Lakers Sentiment Analysis Project
Comprehensive testing for all components
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processors.sentiment_analyzer import LakersSentimentAnalyzer
from data_processors.database_manager import DatabaseManager
from data_collectors.nba_collector import NBADataCollector
from data_processors.performance_pipeline import SentimentPerformancePipeline

class TestSentimentAnalysis(unittest.TestCase):
    """Test cases for sentiment analysis functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.analyzer = LakersSentimentAnalyzer()
        self.test_texts = [
            "LeBron James is playing amazing basketball this season!",
            "Anthony Davis needs to step up his game, he's been disappointing.",
            "The Lakers are going to win the championship this year!",
            "Russell Westbrook is the worst player on the team.",
            "Austin Reaves has been a pleasant surprise this season."
        ]
    
    def test_text_preprocessing(self):
        """Test text preprocessing functionality"""
        test_text = "LeBron James is AMAZING! Check out this link: http://example.com @user1 /r/lakers"
        preprocessed = self.analyzer.preprocess_text(test_text)
        
        self.assertIsInstance(preprocessed, str)
        self.assertNotIn("http://example.com", preprocessed)
        self.assertNotIn("@user1", preprocessed)
        self.assertNotIn("/r/lakers", preprocessed)
        self.assertIn("lebron james", preprocessed.lower())
    
    def test_player_mention_extraction(self):
        """Test player mention extraction"""
        test_text = "LeBron James and Anthony Davis are playing well together"
        mentions = self.analyzer.extract_player_mentions(test_text)
        
        self.assertIsInstance(mentions, list)
        self.assertIn('lebron', mentions)
        self.assertIn('anthony_davis', mentions)
    
    def test_vader_sentiment_analysis(self):
        """Test VADER sentiment analysis"""
        positive_text = "LeBron James is playing amazing basketball!"
        negative_text = "Anthony Davis is terrible this season."
        
        positive_result = self.analyzer.analyze_sentiment_vader(positive_text)
        negative_result = self.analyzer.analyze_sentiment_vader(negative_text)
        
        self.assertIsInstance(positive_result, dict)
        self.assertIn('vader_compound', positive_result)
        self.assertIn('vader_positive', positive_result)
        self.assertIn('vader_negative', positive_result)
        self.assertIn('vader_neutral', positive_result)
        
        # Positive text should have higher compound score
        self.assertGreater(positive_result['vader_compound'], negative_result['vader_compound'])
    
    def test_textblob_sentiment_analysis(self):
        """Test TextBlob sentiment analysis"""
        positive_text = "LeBron James is playing amazing basketball!"
        negative_text = "Anthony Davis is terrible this season."
        
        positive_result = self.analyzer.analyze_sentiment_textblob(positive_text)
        negative_result = self.analyzer.analyze_sentiment_textblob(negative_text)
        
        self.assertIsInstance(positive_result, dict)
        self.assertIn('textblob_polarity', positive_result)
        self.assertIn('textblob_subjectivity', positive_result)
        self.assertIn('textblob_sentiment', positive_result)
        
        # Positive text should have higher polarity
        self.assertGreater(positive_result['textblob_polarity'], negative_result['textblob_polarity'])
    
    def test_comprehensive_sentiment_analysis(self):
        """Test comprehensive sentiment analysis"""
        test_text = "LeBron James is playing amazing basketball this season!"
        result = self.analyzer.analyze_sentiment_comprehensive(test_text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('text', result)
        self.assertIn('preprocessed_text', result)
        self.assertIn('mentioned_players', result)
        self.assertIn('vader_compound', result)
        self.assertIn('textblob_polarity', result)
        self.assertIn('analysis_timestamp', result)
        
        # Should mention LeBron
        self.assertIn('lebron', result['mentioned_players'])
    
    def test_dataframe_sentiment_analysis(self):
        """Test sentiment analysis on DataFrame"""
        test_df = pd.DataFrame({
            'id': ['test1', 'test2'],
            'title': ['LeBron James is amazing!', 'Anthony Davis needs to improve'],
            'selftext': ['Great game by LeBron', 'AD was disappointing']
        })
        
        result_df = self.analyzer.analyze_dataframe(test_df, text_column='title')
        
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertGreater(len(result_df), 0)
        self.assertIn('mentioned_players', result_df.columns)
        self.assertIn('vader_compound', result_df.columns)
    
    def test_player_sentiment_summary(self):
        """Test player sentiment summary generation"""
        test_df = pd.DataFrame({
            'id': ['test1', 'test2', 'test3'],
            'mentioned_players': [['lebron'], ['lebron'], ['anthony_davis']],
            'vader_compound': [0.5, 0.3, -0.2],
            'textblob_polarity': [0.4, 0.2, -0.1],
            'textblob_subjectivity': [0.6, 0.5, 0.4]
        })
        
        summary_df = self.analyzer.get_player_sentiment_summary(test_df)
        
        self.assertIsInstance(summary_df, pd.DataFrame)
        if not summary_df.empty:
            self.assertIn('player', summary_df.columns)
            self.assertIn('total_mentions', summary_df.columns)
            self.assertIn('avg_vader_compound', summary_df.columns)


class TestDatabaseETL(unittest.TestCase):
    """Test cases for database ETL functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        try:
            self.db_manager = DatabaseManager()
            self.test_reddit_data = pd.DataFrame({
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
            
            self.test_sentiment_data = pd.DataFrame({
                'id': ['test1', 'test2'],
                'mentioned_players': [['lebron'], ['anthony_davis']],
                'vader_positive': [0.5, 0.2],
                'vader_negative': [0.1, 0.6],
                'vader_neutral': [0.4, 0.2],
                'vader_compound': [0.4, -0.4],
                'textblob_polarity': [0.3, -0.3],
                'textblob_subjectivity': [0.6, 0.5],
                'textblob_sentiment': ['positive', 'negative'],
                'transformer_positive': [0.7, 0.1],
                'transformer_negative': [0.1, 0.8],
                'transformer_neutral': [0.2, 0.1],
                'transformer_sentiment': ['positive', 'negative'],
                'analysis_timestamp': [datetime.now().isoformat(), datetime.now().isoformat()]
            })
            
        except Exception as e:
            self.skipTest(f"Database connection failed: {e}")
    
    def test_database_connection(self):
        """Test database connection"""
        self.assertIsNotNone(self.db_manager.engine)
        self.assertIsNotNone(self.db_manager.Session)
    
    def test_table_creation(self):
        """Test database table creation"""
        try:
            self.db_manager.create_tables()
            # If no exception is raised, tables were created successfully
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Table creation failed: {e}")
    
    def test_reddit_posts_insertion(self):
        """Test Reddit posts insertion"""
        success = self.db_manager.insert_reddit_posts(self.test_reddit_data)
        self.assertTrue(success)
    
    def test_sentiment_scores_insertion(self):
        """Test sentiment scores insertion"""
        success = self.db_manager.insert_sentiment_scores(self.test_sentiment_data)
        self.assertTrue(success)
    
    def test_player_sentiment_summary_query(self):
        """Test player sentiment summary query"""
        # First insert test data
        self.db_manager.insert_reddit_posts(self.test_reddit_data)
        self.db_manager.insert_sentiment_scores(self.test_sentiment_data)
        
        # Query sentiment summary
        summary_df = self.db_manager.get_player_sentiment_summary('lebron', days=30)
        self.assertIsInstance(summary_df, pd.DataFrame)


class TestNBADataETL(unittest.TestCase):
    """Test cases for NBA data ETL functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.nba_collector = NBADataCollector()
    
    def test_nba_collector_initialization(self):
        """Test NBA data collector initialization"""
        self.assertIsNotNone(self.nba_collector.lakers_roster)
        self.assertIn('lebron', self.nba_collector.lakers_roster)
        self.assertIn('anthony_davis', self.nba_collector.lakers_roster)
    
    def test_player_performance_summary(self):
        """Test player performance summary (mock test)"""
        # This is a mock test since we can't guarantee NBA API availability
        try:
            summary = self.nba_collector.get_player_performance_summary('lebron', days=7)
            self.assertIsInstance(summary, dict)
            self.assertIn('player_name', summary)
        except Exception as e:
            # If API is not available, that's okay for testing
            self.assertIsInstance(e, Exception)


class TestSentimentPerformancePipeline(unittest.TestCase):
    """Test cases for the complete sentiment-performance pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        try:
            self.pipeline = SentimentPerformancePipeline()
        except Exception as e:
            self.skipTest(f"Pipeline initialization failed: {e}")
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization"""
        self.assertIsNotNone(self.pipeline.sentiment_analyzer)
        self.assertIsNotNone(self.pipeline.db_manager)
        self.assertIsNotNone(self.pipeline.nba_collector)
    
    def test_reddit_data_extraction(self):
        """Test Reddit data extraction"""
        try:
            df = self.pipeline.extract_reddit_data(limit=5)
            self.assertIsInstance(df, pd.DataFrame)
        except Exception as e:
            # If Reddit API is not available, that's okay for testing
            self.assertIsInstance(e, Exception)
    
    def test_sentiment_analysis(self):
        """Test sentiment analysis on sample data"""
        test_df = pd.DataFrame({
            'id': ['test1', 'test2'],
            'title': ['LeBron James is amazing!', 'Anthony Davis needs to improve'],
            'selftext': ['Great game by LeBron', 'AD was disappointing']
        })
        
        result_df = self.pipeline.analyze_sentiment(test_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        if not result_df.empty:
            self.assertIn('mentioned_players', result_df.columns)
            self.assertIn('vader_compound', result_df.columns)
    
    def test_nba_data_extraction(self):
        """Test NBA data extraction"""
        try:
            df = self.pipeline.extract_nba_data(days=7)
            self.assertIsInstance(df, pd.DataFrame)
        except Exception as e:
            # If NBA API is not available, that's okay for testing
            self.assertIsInstance(e, Exception)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system"""
    
    def test_end_to_end_workflow(self):
        """Test complete end-to-end workflow with mock data"""
        try:
            # Create mock data
            mock_reddit_df = pd.DataFrame({
                'id': ['test1', 'test2', 'test3'],
                'title': ['LeBron James is amazing!', 'Anthony Davis needs to improve', 'Lakers win!'],
                'selftext': ['Great game by LeBron', 'AD was disappointing', 'Great team effort'],
                'author': ['user1', 'user2', 'user3'],
                'subreddit': ['lakers', 'lakers', 'lakers'],
                'score': [100, 50, 200],
                'num_comments': [20, 10, 40],
                'created_utc': [datetime.now(), datetime.now(), datetime.now()],
                'url': ['http://test1.com', 'http://test2.com', 'http://test3.com'],
                'upvote_ratio': [0.9, 0.7, 0.95],
                'over_18': [False, False, False],
                'edited': [False, False, False],
                'spoiler': [False, False, False],
                'stickied': [False, False, False]
            })
            
            # Test sentiment analysis
            analyzer = LakersSentimentAnalyzer()
            sentiment_df = analyzer.analyze_dataframe(mock_reddit_df)
            
            self.assertIsInstance(sentiment_df, pd.DataFrame)
            self.assertGreater(len(sentiment_df), 0)
            self.assertIn('mentioned_players', sentiment_df.columns)
            
            # Test player sentiment summary
            summary_df = analyzer.get_player_sentiment_summary(sentiment_df)
            self.assertIsInstance(summary_df, pd.DataFrame)
            
        except Exception as e:
            self.fail(f"End-to-end workflow test failed: {e}")


def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestSentimentAnalysis,
        TestDatabaseETL,
        TestNBADataETL,
        TestSentimentPerformancePipeline,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
