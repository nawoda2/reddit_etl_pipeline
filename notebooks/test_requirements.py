#!/usr/bin/env python3
"""
Test script to verify all requirements are properly installed
Run this from the notebooks/ directory to test before opening the notebook
"""

import sys
import os

def test_requirements():
    """Test all the requirements needed for the notebook"""
    
    print("🧪 Testing Reddit ETL Pipeline Requirements")
    print("=" * 50)
    
    # Add parent directory to path
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    print(f"📁 Current directory: {current_dir}")
    print(f"📁 Parent directory: {parent_dir}")
    
    # Test core data processing
    try:
        import pandas as pd
        print("✅ pandas imported successfully")
    except ImportError as e:
        print(f"❌ pandas import failed: {e}")
        return False
    
    try:
        import numpy as np
        print("✅ numpy imported successfully")
    except ImportError as e:
        print(f"❌ numpy import failed: {e}")
        return False
    
    # Test database connectivity
    try:
        import psycopg2
        print("✅ psycopg2 imported successfully")
    except ImportError as e:
        print(f"❌ psycopg2 import failed: {e}")
        return False
    
    try:
        from sqlalchemy import create_engine
        print("✅ sqlalchemy imported successfully")
    except ImportError as e:
        print(f"❌ sqlalchemy import failed: {e}")
        return False
    
    # Test visualization
    try:
        import matplotlib.pyplot as plt
        print("✅ matplotlib imported successfully")
    except ImportError as e:
        print(f"❌ matplotlib import failed: {e}")
        return False
    
    try:
        import seaborn as sns
        print("✅ seaborn imported successfully")
    except ImportError as e:
        print(f"❌ seaborn import failed: {e}")
        return False
    
    try:
        import plotly.graph_objects as go
        import plotly.express as px
        print("✅ plotly imported successfully")
    except ImportError as e:
        print(f"❌ plotly import failed: {e}")
        return False
    
    # Test sentiment analysis
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        print("✅ vaderSentiment imported successfully")
    except ImportError as e:
        print(f"❌ vaderSentiment import failed: {e}")
        return False
    
    try:
        import textblob
        print("✅ textblob imported successfully")
    except ImportError as e:
        print(f"❌ textblob import failed: {e}")
        return False
    
    # Test machine learning
    try:
        import sklearn
        print("✅ scikit-learn imported successfully")
    except ImportError as e:
        print(f"❌ scikit-learn import failed: {e}")
        return False
    
    # Test AWS
    try:
        import boto3
        print("✅ boto3 imported successfully")
    except ImportError as e:
        print(f"❌ boto3 import failed: {e}")
        return False
    
    # Test Reddit API
    try:
        import praw
        print("✅ praw imported successfully")
    except ImportError as e:
        print(f"❌ praw import failed: {e}")
        return False
    
    # Test sentiment dashboard import
    try:
        from visualization.sentiment_dashboard import SentimentDashboard
        print("✅ SentimentDashboard imported successfully")
        
        # Test initialization
        dashboard = SentimentDashboard()
        print("✅ SentimentDashboard initialized successfully")
        
    except ImportError as e:
        print(f"❌ SentimentDashboard import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ SentimentDashboard initialization failed: {e}")
        return False
    
    print("\n🎉 All requirements test passed!")
    print("✅ Your environment is ready for the notebook!")
    return True

if __name__ == "__main__":
    success = test_requirements()
    if not success:
        print("\n❌ Some requirements are missing.")
        print("💡 Try running: pip install -r requirements.txt")
        sys.exit(1)
    else:
        print("\n🚀 You can now run the notebook!")
