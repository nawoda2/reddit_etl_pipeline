#!/usr/bin/env python3
"""
Test script to verify notebook imports work correctly
Run this from the notebooks/ directory to test before opening the notebook
"""

import os
import sys

def test_imports():
    """Test all the imports needed for the notebook"""
    
    print("Testing notebook imports...")
    print("=" * 40)
    
    # Add parent directory to path
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    print(f"Current directory: {current_dir}")
    print(f"Parent directory: {parent_dir}")
    print(f"Parent directory in sys.path: {parent_dir in sys.path}")
    
    # Test basic imports
    try:
        import pandas as pd
        print("SUCCESS: pandas imported successfully")
    except ImportError as e:
        print(f"ERROR: pandas import failed: {e}")
        return False
    
    try:
        import psycopg2
        print("SUCCESS: psycopg2 imported successfully")
    except ImportError as e:
        print(f"ERROR: psycopg2 import failed: {e}")
        return False
    
    try:
        from sqlalchemy import create_engine
        print("SUCCESS: sqlalchemy imported successfully")
    except ImportError as e:
        print(f"ERROR: sqlalchemy import failed: {e}")
        return False
    
    try:
        import configparser
        print("SUCCESS: configparser imported successfully")
    except ImportError as e:
        print(f"ERROR: configparser import failed: {e}")
        return False
    
    # Test config file access
    try:
        config_path = '../config/config.conf'
        if os.path.exists(config_path):
            print("SUCCESS: Config file found")
        else:
            print(f"ERROR: Config file not found at {config_path}")
            return False
    except Exception as e:
        print(f"ERROR: Config file check failed: {e}")
        return False
    
    # Test utils import
    try:
        from utils.config_loader import load_config
        print("SUCCESS: utils.config_loader imported successfully")
        
        # Test config loading
        config = load_config('../config/config.conf')
        db_config = config.get_database_config()
        print(f"SUCCESS: Database config loaded: {db_config['host']}:{db_config['port']}")
        
    except Exception as e:
        print(f"WARNING: utils.config_loader import failed: {e}")
        print("   This is okay - the notebook has fallback methods")
    
    print("\nAll critical imports successful!")
    print("You can now safely open the Jupyter notebook")
    return True

if __name__ == "__main__":
    success = test_imports()
    if success:
        print("\nReady to run the notebook!")
    else:
        print("\nSome imports failed. Check the error messages above.")
        sys.exit(1)
