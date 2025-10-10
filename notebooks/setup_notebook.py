#!/usr/bin/env python3
"""
Setup script for the Reddit ETL Pipeline Database Query Notebook
This script installs the required dependencies and starts Jupyter Notebook
"""

import subprocess
import sys
import os

def install_requirements():
    """Install the required packages for the notebook"""
    print("🔧 Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing requirements: {e}")
        return False

def start_jupyter():
    """Start Jupyter Notebook"""
    print("🚀 Starting Jupyter Notebook...")
    try:
        # Start Jupyter notebook in the current directory
        subprocess.run([sys.executable, "-m", "jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"])
    except KeyboardInterrupt:
        print("\n👋 Jupyter Notebook stopped by user")
    except Exception as e:
        print(f"❌ Error starting Jupyter: {e}")

def main():
    """Main setup function"""
    print("📊 Reddit ETL Pipeline - Database Query Notebook Setup")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not os.path.exists("database_query_notebook.ipynb"):
        print("❌ Please run this script from the notebooks directory")
        print("   cd notebooks && python setup_notebook.py")
        return
    
    # Install requirements
    if not install_requirements():
        print("❌ Failed to install requirements. Please check the error above.")
        return
    
    print("\n🎉 Setup complete! Starting Jupyter Notebook...")
    print("📝 Open your browser and go to: http://localhost:8888")
    print("🔑 The notebook token will be displayed in the terminal")
    print("⏹️  Press Ctrl+C to stop the notebook server")
    print("=" * 60)
    
    # Start Jupyter
    start_jupyter()

if __name__ == "__main__":
    main()
