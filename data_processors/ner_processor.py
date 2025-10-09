"""
Named Entity Recognition (NER) Processor for Lakers Player Mentions
Detects and extracts player mentions from Reddit posts and comments
"""

import re
import pandas as pd
import logging
from typing import List, Dict, Tuple, Set
from datetime import datetime
import spacy
from spacy.matcher import Matcher
from spacy.tokens import Span

from utils.lakers_roster import LAKERS_ROSTER, normalize_player_name, get_all_player_aliases

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LakersNERProcessor:
    """Named Entity Recognition processor for Lakers players"""
    
    def __init__(self):
        """Initialize the NER processor"""
        try:
            # Load spaCy model
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("spaCy model loaded successfully")
        except OSError:
            logger.error("spaCy model 'en_core_web_sm' not found. Please install it with: python -m spacy download en_core_web_sm")
            raise
        
        # Initialize matcher for custom patterns
        self.matcher = Matcher(self.nlp.vocab)
        
        # Create patterns for Lakers players
        self._create_player_patterns()
        
        # Add custom entity ruler for Lakers players
        self._setup_entity_ruler()
        
        # Get all player aliases for exact matching
        self.player_aliases = get_all_player_aliases()
        
        logger.info(f"NER Processor initialized with {len(self.player_aliases)} player aliases")
    
    def _create_player_patterns(self):
        """Create spaCy matcher patterns for Lakers players"""
        patterns = []
        
        for player_name, player_info in LAKERS_ROSTER.items():
            # Pattern for full name
            patterns.append({
                "label": "LAKERS_PLAYER",
                "pattern": [{"LOWER": {"IN": [name.lower() for name in player_info["aliases"]]}}]
            })
            
            # Pattern for first name only (be careful with common names)
            first_name = player_name.split()[0].lower()
            if first_name not in ["james", "jordan", "jake", "jaxson", "christian"]:  # Avoid common names
                patterns.append({
                    "label": "LAKERS_PLAYER", 
                    "pattern": [{"LOWER": first_name}]
                })
        
        # Add patterns to matcher
        for pattern in patterns:
            self.matcher.add("LAKERS_PLAYER", [pattern["pattern"]])
    
    def _setup_entity_ruler(self):
        """Setup entity ruler for exact player name matching"""
        ruler = self.nlp.add_pipe("entity_ruler", before="ner")
        
        # Create patterns for exact matching
        patterns = []
        for player_name, player_info in LAKERS_ROSTER.items():
            for alias in [player_name] + player_info["aliases"]:
                patterns.append({
                    "label": "LAKERS_PLAYER",
                    "pattern": alias
                })
        
        ruler.add_patterns(patterns)
    
    def extract_player_mentions(self, text: str) -> List[Dict]:
        """
        Extract Lakers player mentions from text
        
        Args:
            text: Input text to analyze
            
        Returns:
            List of dictionaries containing player mention information
        """
        if not text or pd.isna(text):
            return []
        
        # Process text with spaCy
        doc = self.nlp(str(text))
        
        mentions = []
        processed_text = text.lower()
        
        # Find exact matches using regex (more reliable for player names)
        for player_name, player_info in LAKERS_ROSTER.items():
            for alias in [player_name] + player_info["aliases"]:
                # Use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(alias.lower()) + r'\b'
                matches = re.finditer(pattern, processed_text, re.IGNORECASE)
                
                for match in matches:
                    mention = {
                        "player_name": player_name,
                        "alias_found": alias,
                        "start_pos": match.start(),
                        "end_pos": match.end(),
                        "context": self._extract_context(text, match.start(), match.end()),
                        "confidence": 1.0,  # Exact match
                        "mention_type": "exact_match"
                    }
                    mentions.append(mention)
        
        # Remove duplicate mentions (same player, same position)
        unique_mentions = self._remove_duplicate_mentions(mentions)
        
        return unique_mentions
    
    def _extract_context(self, text: str, start: int, end: int, context_window: int = 50) -> str:
        """Extract context around a player mention"""
        context_start = max(0, start - context_window)
        context_end = min(len(text), end + context_window)
        return text[context_start:context_end].strip()
    
    def _remove_duplicate_mentions(self, mentions: List[Dict]) -> List[Dict]:
        """Remove duplicate mentions of the same player at the same position"""
        seen = set()
        unique_mentions = []
        
        for mention in mentions:
            key = (mention["player_name"], mention["start_pos"], mention["end_pos"])
            if key not in seen:
                seen.add(key)
                unique_mentions.append(mention)
        
        return unique_mentions
    
    def process_reddit_posts(self, posts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process Reddit posts to extract player mentions
        
        Args:
            posts_df: DataFrame containing Reddit posts
            
        Returns:
            DataFrame with player mentions added
        """
        logger.info(f"Processing {len(posts_df)} Reddit posts for player mentions")
        
        # Initialize columns for player mentions
        posts_df['mentioned_players'] = posts_df.apply(
            lambda row: self._extract_players_from_post(row), axis=1
        )
        
        posts_df['player_mention_count'] = posts_df['mentioned_players'].apply(len)
        
        # Create detailed mention records
        mention_records = []
        for idx, row in posts_df.iterrows():
            if row['mentioned_players']:
                for mention in row['mentioned_players']:
                    mention_records.append({
                        'post_id': row['id'],
                        'player_name': mention['player_name'],
                        'alias_found': mention['alias_found'],
                        'context': mention['context'],
                        'confidence': mention['confidence'],
                        'mention_type': mention['mention_type'],
                        'post_title': row.get('title', ''),
                        'post_author': row.get('author', ''),
                        'post_score': row.get('score', 0),
                        'created_utc': row.get('created_utc'),
                        'subreddit': row.get('subreddit', '')
                    })
        
        logger.info(f"Found {len(mention_records)} player mentions across {len(posts_df)} posts")
        
        return posts_df, pd.DataFrame(mention_records)
    
    def process_reddit_comments(self, comments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process Reddit comments to extract player mentions
        
        Args:
            comments_df: DataFrame containing Reddit comments
            
        Returns:
            DataFrame with player mentions added
        """
        logger.info(f"Processing {len(comments_df)} Reddit comments for player mentions")
        
        # Initialize columns for player mentions
        comments_df['mentioned_players'] = comments_df.apply(
            lambda row: self._extract_players_from_comment(row), axis=1
        )
        
        comments_df['player_mention_count'] = comments_df['mentioned_players'].apply(len)
        
        # Create detailed mention records
        mention_records = []
        for idx, row in comments_df.iterrows():
            if row['mentioned_players']:
                for mention in row['mentioned_players']:
                    mention_records.append({
                        'comment_id': row['id'],
                        'post_id': row['post_id'],
                        'player_name': mention['player_name'],
                        'alias_found': mention['alias_found'],
                        'context': mention['context'],
                        'confidence': mention['confidence'],
                        'mention_type': mention['mention_type'],
                        'comment_author': row.get('author', ''),
                        'comment_score': row.get('score', 0),
                        'created_utc': row.get('created_utc'),
                        'is_submitter': row.get('is_submitter', False)
                    })
        
        logger.info(f"Found {len(mention_records)} player mentions across {len(comments_df)} comments")
        
        return comments_df, pd.DataFrame(mention_records)
    
    def _extract_players_from_post(self, row) -> List[Dict]:
        """Extract player mentions from a single post"""
        text_parts = []
        
        # Combine title and selftext
        if pd.notna(row.get('title')):
            text_parts.append(str(row['title']))
        if pd.notna(row.get('selftext')):
            text_parts.append(str(row['selftext']))
        
        combined_text = ' '.join(text_parts)
        return self.extract_player_mentions(combined_text)
    
    def _extract_players_from_comment(self, row) -> List[Dict]:
        """Extract player mentions from a single comment"""
        if pd.notna(row.get('body')):
            return self.extract_player_mentions(str(row['body']))
        return []
    
    def get_player_mention_summary(self, mentions_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary statistics for player mentions
        
        Args:
            mentions_df: DataFrame containing player mention records
            
        Returns:
            DataFrame with summary statistics
        """
        if mentions_df.empty:
            return pd.DataFrame()
        
        summary = mentions_df.groupby('player_name').agg({
            'post_id': 'nunique',  # Unique posts
            'comment_id': 'nunique',  # Unique comments
            'confidence': 'mean',
            'post_score': 'sum',
            'comment_score': 'sum'
        }).reset_index()
        
        summary.columns = [
            'player_name', 'unique_posts', 'unique_comments', 
            'avg_confidence', 'total_post_score', 'total_comment_score'
        ]
        
        summary['total_mentions'] = summary['unique_posts'] + summary['unique_comments']
        summary = summary.sort_values('total_mentions', ascending=False)
        
        return summary
    
    def create_player_mention_tables(self, mentions_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Create separate tables for each player's mentions
        
        Args:
            mentions_df: DataFrame containing all player mention records
            
        Returns:
            Dictionary with player names as keys and their mention DataFrames as values
        """
        player_tables = {}
        
        for player_name in LAKERS_ROSTER.keys():
            player_mentions = mentions_df[mentions_df['player_name'] == player_name].copy()
            if not player_mentions.empty:
                player_tables[player_name] = player_mentions
                logger.info(f"Created table for {player_name} with {len(player_mentions)} mentions")
            else:
                logger.info(f"No mentions found for {player_name}")
        
        return player_tables


def test_ner_processor():
    """Test function for the NER processor"""
    try:
        processor = LakersNERProcessor()
        
        # Test text
        test_text = """
        LeBron James had an amazing game last night! Austin Reaves was also incredible.
        I think Luka Dončić will be a great addition to the team. Bronny James is showing promise.
        Marcus Smart's defense was outstanding. Rui Hachimura needs to step up his game.
        """
        
        mentions = processor.extract_player_mentions(test_text)
        
        print("NER Processor Test Results:")
        print("=" * 40)
        print(f"Test text: {test_text.strip()}")
        print(f"Found {len(mentions)} player mentions:")
        
        for mention in mentions:
            print(f"  - {mention['player_name']} (found as '{mention['alias_found']}')")
            print(f"    Context: {mention['context']}")
            print(f"    Confidence: {mention['confidence']}")
            print()
        
        print("NER Processor test completed successfully!")
        
    except Exception as e:
        print(f"NER Processor test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_ner_processor()
