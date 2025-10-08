#!/usr/bin/env python3
"""
Startup script for the Reddit Sentiment Analysis Dashboard
"""
import subprocess
import time
import requests
import sys
import os

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False

def wait_for_service(url, service_name, timeout=60):
    """Wait for a service to be ready"""
    print(f"⏳ Waiting for {service_name} to be ready...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {service_name} is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(2)
    
    print(f"❌ {service_name} failed to start within {timeout} seconds")
    return False

def main():
    """Main startup function"""
    print("🚀 Starting Reddit Sentiment Analysis Dashboard")
    print("=" * 50)
    
    # Check if Docker is running
    if not run_command("docker --version", "Checking Docker"):
        print("❌ Docker is not installed or not running. Please install Docker first.")
        sys.exit(1)
    
    # Check if Docker Compose is available
    if not run_command("docker-compose --version", "Checking Docker Compose"):
        print("❌ Docker Compose is not installed. Please install Docker Compose first.")
        sys.exit(1)
    
    # Stop any existing containers
    print("🛑 Stopping any existing containers...")
    subprocess.run("docker-compose -f docker-compose-full.yml down", shell=True)
    
    # Start the services
    if not run_command("docker-compose -f docker-compose-full.yml up -d", "Starting all services"):
        print("❌ Failed to start services")
        sys.exit(1)
    
    # Wait for services to be ready
    print("\n⏳ Waiting for services to start...")
    
    # Wait for Airflow
    if not wait_for_service("http://localhost:8080", "Airflow Web UI", timeout=120):
        print("❌ Airflow failed to start")
        sys.exit(1)
    
    # Wait for API
    if not wait_for_service("http://localhost:8000", "FastAPI Backend", timeout=60):
        print("❌ API failed to start")
        sys.exit(1)
    
    # Success!
    print("\n🎉 Dashboard is ready!")
    print("=" * 50)
    print("📊 Airflow Web UI: http://localhost:8080")
    print("   Username: admin")
    print("   Password: admin")
    print("")
    print("🚀 Sentiment Dashboard: http://localhost:8000/dashboard")
    print("🔧 API Documentation: http://localhost:8000/docs")
    print("")
    print("💡 Tips:")
    print("   - Run the 'etl_reddit_pipeline' DAG in Airflow to collect data")
    print("   - The dashboard will update automatically as new data is processed")
    print("   - Check the logs with: docker-compose -f docker-compose-full.yml logs -f")
    print("")
    print("🛑 To stop all services: docker-compose -f docker-compose-full.yml down")

if __name__ == "__main__":
    main()


