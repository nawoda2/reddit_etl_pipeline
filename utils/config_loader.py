"""
Configuration loader utility for the Reddit ETL Pipeline
Provides a centralized way to load configuration from config.conf
"""

import configparser
import os
from typing import Dict, Any

class ConfigLoader:
    """Centralized configuration loader for the Reddit ETL Pipeline"""
    
    def __init__(self, config_path: str = None):
        """
        Initialize the config loader
        
        Args:
            config_path: Path to the config file. If None, uses default path.
        """
        if config_path is None:
            # Default to config/config.conf relative to project root
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            config_path = os.path.join(project_root, 'config', 'config.conf')
        
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._load_config()
    
    def _load_config(self):
        """Load the configuration file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        
        self.config.read(self.config_path)
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            'host': self.config.get('database', 'database_host'),
            'database': self.config.get('database', 'database_name'),
            'user': self.config.get('database', 'database_username'),
            'password': self.config.get('database', 'database_password'),
            'port': self.config.getint('database', 'database_port')
        }
    
    def get_aws_config(self) -> Dict[str, Any]:
        """Get AWS configuration"""
        return {
            'access_key_id': self.config.get('aws', 'aws_access_key_id'),
            'secret_access_key': self.config.get('aws', 'aws_secret_access_key'),
            'session_token': self.config.get('aws', 'aws_session_token'),
            'region': self.config.get('aws', 'aws_region'),
            'bucket_name': self.config.get('aws', 'aws_bucket_name')
        }
    
    def get_reddit_config(self) -> Dict[str, Any]:
        """Get Reddit API configuration"""
        return {
            'client_id': self.config.get('api_keys', 'reddit_client_id'),
            'secret_key': self.config.get('api_keys', 'reddit_secret_key')
        }
    
    def get_file_paths(self) -> Dict[str, str]:
        """Get file path configuration"""
        return {
            'input_path': self.config.get('file_paths', 'input_path'),
            'output_path': self.config.get('file_paths', 'output_path')
        }
    
    def get_etl_settings(self) -> Dict[str, Any]:
        """Get ETL settings configuration"""
        return {
            'batch_size': self.config.getint('etl_settings', 'batch_size'),
            'error_handling': self.config.get('etl_settings', 'error_handling'),
            'log_level': self.config.get('etl_settings', 'log_level')
        }
    
    def get_connection_string(self, db_type: str = 'postgresql') -> str:
        """Get database connection string"""
        db_config = self.get_database_config()
        return f"{db_type}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Convenience function for quick access
def load_config(config_path: str = None) -> ConfigLoader:
    """Load configuration from the specified path"""
    return ConfigLoader(config_path)

# Example usage
if __name__ == "__main__":
    try:
        config = load_config()
        print("✅ Configuration loaded successfully!")
        print(f"Database: {config.get_database_config()['host']}")
        print(f"AWS Region: {config.get_aws_config()['region']}")
        print(f"Reddit Client ID: {config.get_reddit_config()['client_id']}")
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
