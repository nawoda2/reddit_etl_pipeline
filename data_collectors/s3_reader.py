"""
S3 Data Reader Module for Lakers Sentiment Analysis Project
Reads Reddit data from S3 for sentiment analysis processing
"""

import pandas as pd
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import io
import json

from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, AWS_BUCKET_NAME, AWS_REGION

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3DataReader:
    """
    S3 data reader for Reddit data stored in S3 data lake
    """
    
    def __init__(self):
        self.s3_client = None
        self.bucket_name = AWS_BUCKET_NAME
        self._connect()
    
    def _connect(self):
        """Establish S3 connection"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_ACCESS_KEY,
                region_name=AWS_REGION
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("S3 connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise
    
    def list_reddit_files(self, prefix: str = "raw/", days: int = 30) -> List[str]:
        """
        List Reddit files in S3 for specified time period
        
        Args:
            prefix: S3 prefix for Reddit files
            days: Number of days to look back
            
        Returns:
            List of S3 file keys
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # List objects in S3
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning("No files found in S3 bucket")
                return []
            
            # Filter files by date range
            filtered_files = []
            for obj in response['Contents']:
                file_key = obj['Key']
                file_date = obj['LastModified']
                
                # Check if file is within date range
                if start_date <= file_date <= end_date:
                    filtered_files.append(file_key)
            
            logger.info(f"Found {len(filtered_files)} Reddit files in S3 for last {days} days")
            return filtered_files
            
        except Exception as e:
            logger.error(f"Failed to list Reddit files: {e}")
            return []
    
    def read_reddit_file(self, file_key: str) -> pd.DataFrame:
        """
        Read a single Reddit file from S3
        
        Args:
            file_key: S3 key for the file
            
        Returns:
            DataFrame with Reddit data
        """
        try:
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=file_key
            )
            
            # Read CSV data
            csv_data = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            
            logger.info(f"Successfully read {len(df)} rows from {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read file {file_key}: {e}")
            return pd.DataFrame()
    
    def read_reddit_data_batch(self, file_keys: List[str]) -> pd.DataFrame:
        """
        Read multiple Reddit files from S3 and combine them
        
        Args:
            file_keys: List of S3 keys for files to read
            
        Returns:
            Combined DataFrame with all Reddit data
        """
        try:
            all_dataframes = []
            
            for file_key in file_keys:
                df = self.read_reddit_file(file_key)
                if not df.empty:
                    # Add source file information
                    df['source_file'] = file_key
                    all_dataframes.append(df)
            
            if not all_dataframes:
                logger.warning("No data found in any of the specified files")
                return pd.DataFrame()
            
            # Combine all DataFrames
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Remove duplicates based on post ID
            if 'id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
            
            logger.info(f"Successfully combined {len(combined_df)} unique Reddit posts from {len(file_keys)} files")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to read batch of Reddit files: {e}")
            return pd.DataFrame()
    
    def get_reddit_data_for_period(self, days: int = 30) -> pd.DataFrame:
        """
        Get Reddit data for specified time period from S3
        
        Args:
            days: Number of days to look back
            
        Returns:
            DataFrame with Reddit data for the period
        """
        try:
            # List files for the period
            file_keys = self.list_reddit_files(days=days)
            
            if not file_keys:
                logger.warning(f"No Reddit files found in S3 for last {days} days")
                return pd.DataFrame()
            
            # Read and combine data
            df = self.read_reddit_data_batch(file_keys)
            
            if not df.empty:
                # Convert date columns
                if 'created_utc' in df.columns:
                    df['created_utc'] = pd.to_datetime(df['created_utc'])
                
                # Sort by creation date
                df = df.sort_values('created_utc', ascending=False)
                
                logger.info(f"Retrieved {len(df)} Reddit posts from S3 for last {days} days")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get Reddit data for period: {e}")
            return pd.DataFrame()
    
    def get_reddit_data_for_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get Reddit data for specific date range from S3
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with Reddit data for the date range
        """
        try:
            # Convert string dates to datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Calculate days difference
            days = (end_dt - start_dt).days + 1
            
            # Get data for the period
            df = self.get_reddit_data_for_period(days=days)
            
            if not df.empty and 'created_utc' in df.columns:
                # Filter by exact date range
                df = df[
                    (df['created_utc'].dt.date >= start_dt.date()) &
                    (df['created_utc'].dt.date <= end_dt.date())
                ]
                
                logger.info(f"Filtered to {len(df)} Reddit posts for date range {start_date} to {end_date}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get Reddit data for date range: {e}")
            return pd.DataFrame()
    
    def get_latest_reddit_data(self, hours: int = 24) -> pd.DataFrame:
        """
        Get latest Reddit data from S3
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            DataFrame with latest Reddit data
        """
        try:
            # Convert hours to days (minimum 1 day)
            days = max(1, hours // 24)
            
            # Get data for the period
            df = self.get_reddit_data_for_period(days=days)
            
            if not df.empty and 'created_utc' in df.columns:
                # Filter by exact time range
                cutoff_time = datetime.now() - timedelta(hours=hours)
                df = df[df['created_utc'] >= cutoff_time]
                
                logger.info(f"Retrieved {len(df)} Reddit posts from last {hours} hours")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get latest Reddit data: {e}")
            return pd.DataFrame()
    
    def get_reddit_data_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Get summary of Reddit data available in S3
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with data summary
        """
        try:
            # List files for the period
            file_keys = self.list_reddit_files(days=days)
            
            if not file_keys:
                return {
                    'total_files': 0,
                    'total_posts': 0,
                    'date_range': None,
                    'files': []
                }
            
            # Get data
            df = self.get_reddit_data_for_period(days=days)
            
            summary = {
                'total_files': len(file_keys),
                'total_posts': len(df),
                'date_range': None,
                'files': file_keys
            }
            
            if not df.empty and 'created_utc' in df.columns:
                summary['date_range'] = {
                    'earliest': df['created_utc'].min().isoformat(),
                    'latest': df['created_utc'].max().isoformat()
                }
                
                # Add subreddit breakdown
                if 'subreddit' in df.columns:
                    summary['subreddit_breakdown'] = df['subreddit'].value_counts().to_dict()
                
                # Add author breakdown
                if 'author' in df.columns:
                    summary['unique_authors'] = df['author'].nunique()
                
                # Add engagement metrics
                if 'score' in df.columns:
                    summary['avg_score'] = df['score'].mean()
                if 'num_comments' in df.columns:
                    summary['avg_comments'] = df['num_comments'].mean()
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get Reddit data summary: {e}")
            return {'error': str(e)}


def test_s3_data_reader():
    """Test function for the S3 data reader"""
    try:
        print("Testing S3 Data Reader:")
        print("=" * 50)
        
        reader = S3DataReader()
        
        # Test listing files
        print("Testing file listing...")
        files = reader.list_reddit_files(days=7)
        print(f"Found {len(files)} files in S3")
        
        # Test reading data
        if files:
            print("Testing data reading...")
            df = reader.read_reddit_data_batch(files[:2])  # Read first 2 files
            print(f"Read {len(df)} posts from {len(files[:2])} files")
        
        # Test summary
        print("Testing data summary...")
        summary = reader.get_reddit_data_summary(days=7)
        print(f"Data summary: {summary}")
        
        print("S3 Data Reader test completed successfully!")
        
    except Exception as e:
        print(f"S3 Data Reader test failed: {e}")


if __name__ == "__main__":
    test_s3_data_reader()
