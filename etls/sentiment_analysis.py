"""
Sentiment Analysis Module for Lakers Reddit Data
Implements multiple sentiment analysis approaches for player-specific sentiment analysis
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime

# Sentiment Analysis Libraries
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch

# NLP Libraries
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LakersSentimentAnalyzer:
    """
    Advanced sentiment analyzer specifically designed for Lakers Reddit data
    Combines multiple sentiment analysis approaches for robust results
    """
    
    def __init__(self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"):
        """
        Initialize the sentiment analyzer with multiple models
        
        Args:
            model_name: Hugging Face model name for transformer-based sentiment analysis
        """
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.model_name = model_name
        self.transformer_pipeline = None
        self.nlp = None
        
        # 2025-26 Lakers player names and common variations
        self.lakers_players = {
            'lebron': ['lebron', 'lebron james', 'lbj', 'king james', 'bron'],
            'anthony_davis': ['anthony davis', 'ad', 'brow', 'anthony', 'davis'],
            'austin_reaves': ['austin reaves', 'ar', 'reaves', 'austin'],
            'dangelo_russell': ['dangelo russell', 'dlo', 'russell', 'dangelo'],
            'russell_westbrook': ['russell westbrook', 'westbrook', 'russ', 'russell'],
            'kyle_kuzma': ['kyle kuzma', 'kuzma', 'kyle'],
            'brandon_ingram': ['brandon ingram', 'ingram', 'brandon', 'bi'],
            'lonzo_ball': ['lonzo ball', 'lonzo', 'ball'],
            'josh_hart': ['josh hart', 'hart', 'josh'],
            'kentavious_caldwell_pope': ['kentavious caldwell pope', 'kcp', 'pope'],
            'alex_caruso': ['alex caruso', 'caruso', 'alex'],
            'jared_dudley': ['jared dudley', 'dudley', 'jared'],
            'markieff_morris': ['markieff morris', 'morris', 'markieff'],
            'montrezl_harrell': ['montrezl harrell', 'harrell', 'trez'],
            'dennis_schroder': ['dennis schroder', 'schroder', 'dennis'],
            'andre_drummond': ['andre drummond', 'drummond', 'andre'],
            'marc_gasol': ['marc gasol', 'gasol', 'marc'],
            'talen_horton_tucker': ['talen horton tucker', 'tht', 'tucker', 'talen'],
            'malik_monk': ['malik monk', 'monk', 'malik'],
            'carmelo_anthony': ['carmelo anthony', 'melo', 'carmelo', 'anthony'],
            'dwight_howard': ['dwight howard', 'howard', 'dwight'],
            'rajon_rondo': ['rajon rondo', 'rondo', 'rajon'],
            'jordan_clarkson': ['jordan clarkson', 'clarkson', 'jordan'],
            'julius_randle': ['julius randle', 'randle', 'julius'],
            'larry_nance_jr': ['larry nance jr', 'nance', 'larry'],
            'ivica_zubac': ['ivica zubac', 'zubac', 'ivica'],
            'moe_wagner': ['moe wagner', 'wagner', 'moe'],
            'isaac_bonga': ['isaac bonga', 'bonga', 'isaac'],
            'svi_mykhailiuk': ['svi mykhailiuk', 'svi', 'mykhailiuk'],
            'moritz_wagner': ['moritz wagner', 'moritz'],
            'jonathan_williams': ['jonathan williams', 'jonathan'],
            'reggie_bullock': ['reggie bullock', 'bullock', 'reggie'],
            'mike_muscala': ['mike muscala', 'muscala', 'mike'],
            'tyler_ennis': ['tyler ennis', 'ennis', 'tyler'],
            'corey_brewer': ['corey brewer', 'brewer', 'corey'],
            'isaiah_thomas': ['isaiah thomas', 'it', 'isaiah'],
            'channing_frye': ['channing frye', 'frye', 'channing'],
            'jose_calderon': ['jose calderon', 'calderon', 'jose'],
            'andrew_bogut': ['andrew bogut', 'bogut', 'andrew'],
            'luol_deng': ['luol deng', 'deng', 'luol'],
            'timofey_mozgov': ['timofey mozgov', 'mozgov', 'timofey'],
            'jordan_hill': ['jordan hill', 'hill', 'jordan'],
            'nick_young': ['nick young', 'swaggy p', 'nick'],
            'steve_blake': ['steve blake', 'blake', 'steve'],
            'jodie_meeks': ['jodie meeks', 'meeks', 'jodie'],
            'kendall_marshall': ['kendall marshall', 'marshall', 'kendall'],
            'wesley_johnson': ['wesley johnson', 'johnson', 'wesley'],
            'chris_kaman': ['chris kaman', 'kaman', 'chris'],
            'jordan_farmar': ['jordan farmar', 'farmar', 'jordan'],
            'steve_nash': ['steve nash', 'nash', 'steve'],
            'pau_gasol': ['pau gasol', 'pau', 'gasol'],
            'metta_world_peace': ['metta world peace', 'artest', 'metta'],
            'lamar_odom': ['lamar odom', 'odom', 'lamar'],
            'derek_fisher': ['derek fisher', 'fisher', 'derek'],
            'kobe_bryant': ['kobe bryant', 'kobe', 'mamba', 'black mamba'],
            'shaquille_oneal': ['shaquille oneal', 'shaq', 'oneal'],
            'robert_horry': ['robert horry', 'horry', 'robert'],
            'rick_fox': ['rick fox', 'fox', 'rick'],
            'brian_shaw': ['brian shaw', 'shaw', 'brian'],
            'glen_rice': ['glen rice', 'rice', 'glen'],
            'eddie_jones': ['eddie jones', 'jones', 'eddie'],
            'nick_van_exel': ['nick van exel', 'van exel', 'nick'],
            'cedric_ceballos': ['cedric ceballos', 'ceballos', 'cedric'],
            'vlade_divac': ['vlade divac', 'divac', 'vlade'],
            'james_worthy': ['james worthy', 'worthy', 'james'],
            'magic_johnson': ['magic johnson', 'magic', 'johnson'],
            'kareem_abdul_jabbar': ['kareem abdul jabbar', 'kareem', 'jabbar'],
            'jerry_west': ['jerry west', 'west', 'jerry'],
            'elgin_baylor': ['elgin baylor', 'baylor', 'elgin'],
            'wilt_chamberlain': ['wilt chamberlain', 'wilt', 'chamberlain']
        }
        
        self._initialize_models()
        self._download_nltk_data()
    
    def _initialize_models(self):
        """Initialize transformer models and spaCy"""
        try:
            # Initialize transformer pipeline for sentiment analysis
            logger.info(f"Loading transformer model: {self.model_name}")
            self.transformer_pipeline = pipeline(
                "sentiment-analysis",
                model=self.model_name,
                tokenizer=self.model_name,
                return_all_scores=True
            )
            logger.info("Transformer model loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load transformer model: {e}")
            self.transformer_pipeline = None
        
        try:
            # Initialize spaCy for NER
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("spaCy model loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load spaCy model: {e}")
            self.nlp = None
    
    def _download_nltk_data(self):
        """Download required NLTK data"""
        try:
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt', quiet=True)
            logger.info("NLTK data downloaded successfully")
        except Exception as e:
            logger.warning(f"Failed to download NLTK data: {e}")
    
    def preprocess_text(self, text: str) -> str:
        """
        Preprocess text for sentiment analysis
        
        Args:
            text: Raw text to preprocess
            
        Returns:
            Cleaned text
        """
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove user mentions and subreddit references
        text = re.sub(r'@\w+|/u/\w+|/r/\w+', '', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\!\?\,\;\:]', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_player_mentions(self, text: str) -> List[str]:
        """
        Extract Lakers player mentions from text
        
        Args:
            text: Text to analyze for player mentions
            
        Returns:
            List of mentioned player names
        """
        if not isinstance(text, str):
            return []
        
        text_lower = text.lower()
        mentioned_players = []
        
        for player_key, variations in self.lakers_players.items():
            for variation in variations:
                if variation in text_lower:
                    mentioned_players.append(player_key)
                    break  # Avoid duplicate mentions of same player
        
        return list(set(mentioned_players))  # Remove duplicates
    
    def analyze_sentiment_vader(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using VADER
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        try:
            scores = self.vader_analyzer.polarity_scores(text)
            return {
                'vader_positive': scores['pos'],
                'vader_negative': scores['neg'],
                'vader_neutral': scores['neu'],
                'vader_compound': scores['compound']
            }
        except Exception as e:
            logger.error(f"VADER analysis failed: {e}")
            return {
                'vader_positive': 0.0,
                'vader_negative': 0.0,
                'vader_neutral': 1.0,
                'vader_compound': 0.0
            }
    
    def analyze_sentiment_textblob(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using TextBlob
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Convert polarity to positive/negative/neutral
            if polarity > 0.1:
                sentiment = 'positive'
            elif polarity < -0.1:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'
            
            return {
                'textblob_polarity': polarity,
                'textblob_subjectivity': subjectivity,
                'textblob_sentiment': sentiment
            }
        except Exception as e:
            logger.error(f"TextBlob analysis failed: {e}")
            return {
                'textblob_polarity': 0.0,
                'textblob_subjectivity': 0.0,
                'textblob_sentiment': 'neutral'
            }
    
    def analyze_sentiment_transformer(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using transformer model
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        if self.transformer_pipeline is None:
            return {
                'transformer_positive': 0.0,
                'transformer_negative': 0.0,
                'transformer_neutral': 1.0,
                'transformer_sentiment': 'neutral'
            }
        
        try:
            # Truncate text if too long
            if len(text) > 512:
                text = text[:512]
            
            results = self.transformer_pipeline(text)
            
            # Extract scores
            scores = {}
            for result in results[0]:
                label = result['label'].lower()
                score = result['score']
                scores[f'transformer_{label}'] = score
            
            # Determine overall sentiment
            max_score = max(results[0], key=lambda x: x['score'])
            sentiment = max_score['label'].lower()
            
            scores['transformer_sentiment'] = sentiment
            
            return scores
        except Exception as e:
            logger.error(f"Transformer analysis failed: {e}")
            return {
                'transformer_positive': 0.0,
                'transformer_negative': 0.0,
                'transformer_neutral': 1.0,
                'transformer_sentiment': 'neutral'
            }
    
    def analyze_sentiment_comprehensive(self, text: str) -> Dict[str, any]:
        """
        Perform comprehensive sentiment analysis using all available methods
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with all sentiment analysis results
        """
        if not isinstance(text, str) or not text.strip():
            return {
                'text': '',
                'preprocessed_text': '',
                'mentioned_players': [],
                'vader_positive': 0.0,
                'vader_negative': 0.0,
                'vader_neutral': 1.0,
                'vader_compound': 0.0,
                'textblob_polarity': 0.0,
                'textblob_subjectivity': 0.0,
                'textblob_sentiment': 'neutral',
                'transformer_positive': 0.0,
                'transformer_negative': 0.0,
                'transformer_neutral': 1.0,
                'transformer_sentiment': 'neutral',
                'analysis_timestamp': datetime.now().isoformat()
            }
        
        # Preprocess text
        preprocessed_text = self.preprocess_text(text)
        
        # Extract player mentions
        mentioned_players = self.extract_player_mentions(text)
        
        # Perform sentiment analysis with all methods
        vader_scores = self.analyze_sentiment_vader(preprocessed_text)
        textblob_scores = self.analyze_sentiment_textblob(preprocessed_text)
        transformer_scores = self.analyze_sentiment_transformer(preprocessed_text)
        
        # Combine all results
        result = {
            'text': text,
            'preprocessed_text': preprocessed_text,
            'mentioned_players': mentioned_players,
            **vader_scores,
            **textblob_scores,
            **transformer_scores,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def analyze_dataframe(self, df: pd.DataFrame, text_column: str = 'selftext') -> pd.DataFrame:
        """
        Analyze sentiment for an entire DataFrame
        
        Args:
            df: DataFrame containing text data
            text_column: Name of the column containing text to analyze
            
        Returns:
            DataFrame with sentiment analysis results
        """
        logger.info(f"Starting sentiment analysis for {len(df)} rows")
        
        results = []
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing row {idx}/{len(df)}")
            
            text = str(row.get(text_column, ''))
            if pd.isna(text) or text == 'nan':
                text = ''
            
            # Also check title column for player mentions
            title = str(row.get('title', ''))
            if pd.isna(title) or title == 'nan':
                title = ''
            
            # Combine title and text for analysis
            combined_text = f"{title} {text}".strip()
            
            sentiment_result = self.analyze_sentiment_comprehensive(combined_text)
            results.append(sentiment_result)
        
        # Convert results to DataFrame
        sentiment_df = pd.DataFrame(results)
        
        # Merge with original DataFrame
        result_df = pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)
        
        logger.info(f"Sentiment analysis completed for {len(result_df)} rows")
        return result_df
    
    def get_player_sentiment_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get sentiment summary for each player
        
        Args:
            df: DataFrame with sentiment analysis results
            
        Returns:
            DataFrame with player sentiment summaries
        """
        player_summaries = []
        
        for player in self.lakers_players.keys():
            # Filter posts mentioning this player
            player_posts = df[df['mentioned_players'].apply(
                lambda x: player in x if isinstance(x, list) else False
            )]
            
            if len(player_posts) == 0:
                continue
            
            # Calculate average sentiment scores
            summary = {
                'player': player,
                'total_mentions': len(player_posts),
                'avg_vader_compound': player_posts['vader_compound'].mean(),
                'avg_textblob_polarity': player_posts['textblob_polarity'].mean(),
                'avg_textblob_subjectivity': player_posts['textblob_subjectivity'].mean(),
                'positive_mentions': len(player_posts[player_posts['vader_compound'] > 0.05]),
                'negative_mentions': len(player_posts[player_posts['vader_compound'] < -0.05]),
                'neutral_mentions': len(player_posts[
                    (player_posts['vader_compound'] >= -0.05) & 
                    (player_posts['vader_compound'] <= 0.05)
                ]),
                'sentiment_ratio': len(player_posts[player_posts['vader_compound'] > 0.05]) / 
                                 len(player_posts[player_posts['vader_compound'] < -0.05]) 
                                 if len(player_posts[player_posts['vader_compound'] < -0.05]) > 0 else float('inf')
            }
            
            player_summaries.append(summary)
        
        return pd.DataFrame(player_summaries)


def test_sentiment_analyzer():
    """Test function for the sentiment analyzer"""
    analyzer = LakersSentimentAnalyzer()
    
    # Test texts
    test_texts = [
        "LeBron James is playing amazing basketball this season!",
        "Anthony Davis needs to step up his game, he's been disappointing.",
        "The Lakers are going to win the championship this year!",
        "Russell Westbrook is the worst player on the team.",
        "Austin Reaves has been a pleasant surprise this season."
    ]
    
    print("Testing Lakers Sentiment Analyzer:")
    print("=" * 50)
    
    for text in test_texts:
        print(f"\nText: {text}")
        result = analyzer.analyze_sentiment_comprehensive(text)
        print(f"Preprocessed: {result['preprocessed_text']}")
        print(f"Mentioned Players: {result['mentioned_players']}")
        print(f"VADER Compound: {result['vader_compound']:.3f}")
        print(f"TextBlob Polarity: {result['textblob_polarity']:.3f}")
        print(f"TextBlob Sentiment: {result['textblob_sentiment']}")
        if 'transformer_sentiment' in result:
            print(f"Transformer Sentiment: {result['transformer_sentiment']}")
        print("-" * 30)


if __name__ == "__main__":
    test_sentiment_analyzer()
