#!/usr/bin/env python3
"""
Lakers Sentiment Analytics Dashboard Runner
Starts the Streamlit dashboard for Lakers sentiment analysis
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Main function to run the Streamlit dashboard"""
    
    # Get the project root directory
    project_root = Path(__file__).parent
    streamlit_app_path = project_root / "streamlit_app" / "app.py"
    
    # Check if the Streamlit app exists
    if not streamlit_app_path.exists():
        print("❌ Streamlit app not found!")
        print(f"Expected path: {streamlit_app_path}")
        return 1
    
    print("🏀 Starting Lakers Sentiment Analytics Dashboard...")
    print(f"📁 Project root: {project_root}")
    print(f"📱 Streamlit app: {streamlit_app_path}")
    print("🌐 Dashboard will be available at: http://localhost:8501")
    print("=" * 60)
    
    try:
        # Change to project root directory
        os.chdir(project_root)
        
        # Run Streamlit
        cmd = [
            sys.executable, "-m", "streamlit", "run", 
            str(streamlit_app_path),
            "--server.port", "8501",
            "--server.address", "localhost",
            "--browser.gatherUsageStats", "false"
        ]
        
        print(f"🚀 Running command: {' '.join(cmd)}")
        print("=" * 60)
        
        # Execute the command
        result = subprocess.run(cmd, check=True)
        
        return result.returncode
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to start Streamlit dashboard: {e}")
        return e.returncode
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
