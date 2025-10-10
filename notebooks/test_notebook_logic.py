#!/usr/bin/env python3
"""
Test script that mimics the exact logic from the notebook
This will help identify any issues with the notebook execution
"""

import os
import sys
import warnings
warnings.filterwarnings('ignore')

print("Testing notebook logic...")
print("=" * 40)

# Add parent directory to path to access config and utils
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

print(f"Current directory: {current_dir}")
print(f"Parent directory added to path: {parent_dir}")

# Import required libraries
try:
    import pandas as pd
    print("SUCCESS: pandas imported")
except Exception as e:
    print(f"ERROR: pandas import failed: {e}")
    sys.exit(1)

try:
    import psycopg2
    print("SUCCESS: psycopg2 imported")
except Exception as e:
    print(f"ERROR: psycopg2 import failed: {e}")
    sys.exit(1)

try:
    from sqlalchemy import create_engine
    print("SUCCESS: sqlalchemy imported")
except Exception as e:
    print(f"ERROR: sqlalchemy import failed: {e}")
    sys.exit(1)

try:
    import configparser
    print("SUCCESS: configparser imported")
except Exception as e:
    print(f"ERROR: configparser import failed: {e}")
    sys.exit(1)

# Test the exact function from the notebook
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

# Load configuration
try:
    DB_CONFIG, connection_string = load_database_config()
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("Using secure configuration loading (no hardcoded credentials)")
    
except Exception as e:
    print(f"ERROR: Error loading database configuration: {e}")
    print("Make sure you're running this from the notebooks/ directory")
    print("and that ../config/config.conf exists")
    sys.exit(1)

# Test database connection
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
    
except Exception as e:
    print(f"ERROR: Database connection failed: {e}")

# Create SQLAlchemy engine for pandas integration
try:
    engine = create_engine(connection_string)
    print("SUCCESS: SQLAlchemy engine created successfully!")
except Exception as e:
    print(f"ERROR: SQLAlchemy engine creation failed: {e}")

print("\nAll tests completed!")
