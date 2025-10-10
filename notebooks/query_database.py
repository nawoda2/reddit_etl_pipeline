#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Query Script - Alternative to Jupyter Notebook
This script provides the same functionality as the notebook but runs as a regular Python script
"""

import os
import sys
import warnings
warnings.filterwarnings('ignore')

# Set UTF-8 encoding for Windows
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Add parent directory to path to access config and utils
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

print("Reddit ETL Pipeline - Database Query Script")
print("=" * 50)

# Import required libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import configparser

def load_database_config():
    """Load database configuration with fallback methods"""
    
    # Method 1: Try using the config loader utility
    try:
        from utils.config_loader import load_config
        config = load_config('../config/config.conf')
        db_config = config.get_database_config()
        connection_string = config.get_connection_string()
        print("SUCCESS: Database configuration loaded using config loader utility!")
        return db_config, connection_string
    except Exception as e:
        print(f"WARNING: Config loader failed: {e}")
    
    # Method 2: Fallback to direct config file reading
    try:
        config = configparser.ConfigParser()
        config_path = '../config/config.conf'
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")
        
        config.read(config_path)
        
        db_config = {
            'host': config.get('database', 'database_host'),
            'database': config.get('database', 'database_name'),
            'user': config.get('database', 'database_username'),
            'password': config.get('database', 'database_password'),
            'port': config.getint('database', 'database_port')
        }
        
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        
        print("SUCCESS: Database configuration loaded using direct config file reading!")
        return db_config, connection_string
        
    except Exception as e:
        print(f"ERROR: All configuration loading methods failed: {e}")
        raise

def test_database_connection(DB_CONFIG):
    """Test database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("SUCCESS: Database connection successful!")
        
        # Get database info
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()[0]
        print(f"Database Version: {db_version}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return False

def list_tables(engine):
    """List all tables in the database"""
    query_tables = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """
    
    try:
        tables_df = pd.read_sql(query_tables, engine)
        print("\nAvailable Tables:")
        print(tables_df)
        
        if len(tables_df) == 0:
            print("\nWARNING: No tables found in the public schema.")
            print("This might mean the ETL pipeline hasn't run yet or data hasn't been loaded.")
        else:
            print(f"\nFound {len(tables_df)} tables")
        
        return tables_df
        
    except Exception as e:
        print(f"ERROR: Error listing tables: {e}")
        return None

def query_reddit_posts(engine):
    """Query reddit_posts table if it exists"""
    query = "SELECT * FROM reddit_posts LIMIT 5;"
    
    try:
        df = pd.read_sql(query, engine)
        print("\nReddit Posts Sample Data:")
        print(df)
        print(f"\nTotal rows: {len(df)}")
        return df
    except Exception as e:
        print(f"ERROR: Error querying reddit_posts: {e}")
        print("This table might not exist yet. Run the ETL pipeline first.")
        return None

def query_sentiment_analysis(engine):
    """Query sentiment_analysis table if it exists"""
    query = "SELECT * FROM sentiment_analysis LIMIT 5;"
    
    try:
        df = pd.read_sql(query, engine)
        print("\nSentiment Analysis Sample Data:")
        print(df)
        print(f"\nTotal rows: {len(df)}")
        return df
    except Exception as e:
        print(f"ERROR: Error querying sentiment_analysis: {e}")
        print("This table might not exist yet. Run the ETL pipeline first.")
        return None

def main():
    """Main function"""
    print("Loading configuration...")
    
    # Load configuration
    try:
        DB_CONFIG, connection_string = load_database_config()
        print(f"Connected to: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        print("Using secure configuration loading (no hardcoded credentials)")
        
    except Exception as e:
        print(f"ERROR: Error loading database configuration: {e}")
        print("Make sure you're running this script from the notebooks/ directory")
        print("and that ../config/config.conf exists")
        return
    
    # Test database connection
    if not test_database_connection(DB_CONFIG):
        return
    
    # Create SQLAlchemy engine
    try:
        engine = create_engine(connection_string)
        print("SUCCESS: SQLAlchemy engine created successfully!")
    except Exception as e:
        print(f"ERROR: SQLAlchemy engine creation failed: {e}")
        return
    
    # List tables
    tables_df = list_tables(engine)
    
    # Query specific tables if they exist
    if tables_df is not None and len(tables_df) > 0:
        table_names = tables_df['table_name'].tolist()
        
        if 'reddit_posts' in table_names:
            query_reddit_posts(engine)
        
        if 'sentiment_analysis' in table_names:
            query_sentiment_analysis(engine)
    
    print("\nDatabase querying completed!")
    print("\nTo run custom queries, modify this script or use the Jupyter notebook.")

if __name__ == "__main__":
    main()
