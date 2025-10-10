# Reddit ETL Pipeline - Database Query Notebook

This directory contains a Jupyter notebook specifically designed for querying the PostgreSQL database used by the Reddit ETL pipeline.

## 🚀 Quick Start

### Option 1: Using the Setup Script (Recommended)
```bash
cd notebooks
python setup_notebook.py
```

### Option 2: Manual Setup
```bash
cd notebooks
pip install -r requirements.txt
jupyter notebook
```

## 📊 What's Included

### `database_query_notebook.ipynb`
A comprehensive Jupyter notebook with:

- **Database Connection**: Pre-configured connection to your RDS PostgreSQL database
- **Schema Exploration**: Queries to discover available tables and their structure
- **Sample Queries**: Ready-to-run queries for common data analysis tasks
- **Custom Query Section**: Space for your own SQL queries
- **Data Visualization**: Examples of how to create charts and graphs
- **Error Handling**: Robust error handling for common database issues

### Key Features:
- ✅ **Direct RDS Connection**: Connects to `reddit-etl-db.cfu0u0e26k4w.us-west-1.rds.amazonaws.com`
- ✅ **Pandas Integration**: Easy data manipulation with pandas DataFrames
- ✅ **SQLAlchemy Support**: Advanced database operations
- ✅ **Visualization Ready**: Matplotlib and Plotly examples
- ✅ **Error Handling**: Graceful handling of missing tables or connection issues

## 🔧 Prerequisites

- Python 3.7+
- Access to the RDS database (credentials are pre-configured)
- Internet connection for package installation

## 📋 Available Queries

The notebook includes sample queries for:

1. **Table Discovery**: List all available tables
2. **Reddit Posts**: Sample data from reddit_posts table
3. **Sentiment Analysis**: Results from sentiment_analysis table
4. **Database Statistics**: Table statistics and metadata
5. **Custom Queries**: Template for your own SQL queries

## 🎯 Common Use Cases

- **Data Exploration**: Discover what data is available
- **Quality Checks**: Verify data integrity and completeness
- **Performance Analysis**: Monitor ETL pipeline performance
- **Business Intelligence**: Create reports and dashboards
- **Debugging**: Troubleshoot ETL pipeline issues

## 🔒 Security Features

- **No Hardcoded Credentials**: All database credentials are loaded from `../config/config.conf`
- **Centralized Configuration**: Uses the same config file as the ETL pipeline
- **Secure Loading**: Credentials are never exposed in the notebook code
- **Environment Consistency**: Ensures the notebook uses the same settings as the pipeline
- **RDS Security**: The RDS instance should be properly secured with appropriate security groups

## 🆘 Troubleshooting

### Connection Issues
- Verify your IP is whitelisted in the RDS security group
- Check that the RDS instance is running
- Ensure the database credentials are correct

### Missing Tables
- Run the ETL pipeline first to populate the database
- Check the Airflow logs for any data loading errors
- Verify the pipeline completed successfully

### Package Installation Issues
- Ensure you have Python 3.7+ installed
- Try using a virtual environment
- Check your internet connection

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review the Airflow logs for ETL pipeline status
3. Verify database connectivity and permissions
