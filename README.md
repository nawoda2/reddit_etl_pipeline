# Lakers Reddit Sentiment Analysis Pipeline

A production-ready data pipeline that extracts sentiment from Lakers subreddit discussions and correlates it with NBA player performance data. This system processes Reddit posts, performs multi-model sentiment analysis, collects NBA statistics, and provides statistical correlation analysis between fan sentiment and on-court performance.

## Project Overview

This project addresses the question: **How does fan sentiment on Reddit correlate with Lakers players' actual performance metrics?** The system combines natural language processing, sports analytics, and statistical analysis to provide data-driven insights into the relationship between fan perception and player performance.

### Core Components

- **Reddit Data Collection**: Automated extraction of posts and comments from r/lakers
- **Multi-Model Sentiment Analysis**: VADER, TextBlob, and RoBERTa-based sentiment scoring
- **NBA Data Integration**: Real-time collection of player statistics and game logs
- **Statistical Correlation Analysis**: Pearson correlation analysis between sentiment and performance
- **Data Visualization**: Interactive dashboards and trend analysis
- **Production Orchestration**: Apache Airflow-based workflow management

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Reddit API    │    │   NBA Stats API  │    │   PostgreSQL    │
│   (PRAW)        │    │   (stats.nba.com)│    │   Database      │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          ▼                      ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Sentiment      │    │  Performance     │    │  Correlation    │
│  Analysis       │    │  Data Collection │    │  Analysis       │
│  (Multi-Model)  │    │  (Game Logs)     │    │  (Statistical)  │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          └──────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │    Airflow DAG          │
                    │  (Orchestration)        │
                    └─────────┬───────────────┘
                              ▼
                    ┌─────────────────────────┐
                    │   Visualization         │
                    │   Dashboard             │
                    └─────────────────────────┘
```

## Data Processing Pipeline

### 1. Data Collection Phase
- **Reddit Extraction**: Collects posts and comments from r/lakers subreddit using PRAW
- **NBA Data Collection**: Retrieves player statistics, game logs, and team performance data
- **Data Validation**: Ensures data quality and completeness before processing

### 2. Data Processing Phase
- **Text Preprocessing**: Cleans and normalizes Reddit text data
- **Player Mention Detection**: Identifies which Lakers players are mentioned in each post
- **Sentiment Analysis**: Applies multiple sentiment analysis models for robust scoring
- **Performance Data Normalization**: Standardizes NBA statistics for analysis

### 3. Data Storage Phase
- **PostgreSQL Database**: Stores processed data in structured tables
- **S3 Data Lake**: Archives raw data for historical analysis
- **Data Indexing**: Optimizes database queries for performance

### 4. Analysis Phase
- **Correlation Analysis**: Statistical analysis of sentiment vs performance relationships
- **Trend Analysis**: Time-series analysis of sentiment patterns
- **Visualization**: Interactive charts and dashboards for data exploration

## Installation and Setup

### Prerequisites

- Python 3.9 or higher
- PostgreSQL 12 or higher
- Apache Airflow 2.7 or higher
- Docker (optional, for containerized deployment)

### Step-by-Step Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/reddit_etl_pipeline.git
   cd reddit_etl_pipeline
   ```

2. **Create Python Virtual Environment**
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Download Required NLP Models**
   ```bash
   python -m spacy download en_core_web_sm
   ```

5. **Configure Environment Variables**
   ```bash
   cp config/config_template.conf config/config.conf
   ```
   
   Edit `config/config.conf` with your API credentials:
   ```ini
   [reddit]
   client_id = your_reddit_client_id
   secret = your_reddit_secret
   user_agent = your_app_name
   
   [database]
   database_host = localhost
   database_name = airflow_reddit
   database_port = 5432
   database_username = postgres
   database_password = your_password
   
   [aws]
   aws_access_key_id = your_aws_key
   aws_secret_access_key = your_aws_secret
   aws_bucket_name = your_bucket_name
   aws_region = us-east-1
   ```

6. **Set Up PostgreSQL Database**
   ```sql
   CREATE DATABASE airflow_reddit;
   CREATE USER postgres WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE airflow_reddit TO postgres;
   ```

7. **Initialize Database Schema**
   ```bash
   python -c "from data_processors.database_manager import DatabaseManager; db = DatabaseManager(); db.create_tables()"
   ```

## Running the System

### Development Mode

1. **Start Airflow Services**
   ```bash
   # Terminal 1: Start Airflow Webserver
   airflow webserver --port 8080
   
   # Terminal 2: Start Airflow Scheduler
   airflow scheduler
   ```

2. **Access Airflow UI**
   - Open http://localhost:8080 in your browser
   - Default credentials: admin/admin

3. **Trigger the Pipeline**
   - Navigate to the `etl_reddit_pipeline` DAG
   - Click "Trigger DAG" to start the pipeline

### Production Mode

1. **Docker Deployment**
   ```bash
   docker-compose up -d
   ```

2. **Monitor Pipeline Execution**
   - Check Airflow UI for pipeline status
   - Monitor logs for any errors or issues

## Project Structure

```
reddit_etl_pipeline/
├── config/                          # Configuration files
│   ├── config.conf                  # Main configuration file
│   └── config_template.conf         # Configuration template
├── data_collectors/                 # Data collection modules
│   ├── reddit_collector.py          # Reddit data extraction
│   ├── nba_collector.py             # NBA statistics collection
│   ├── aws_s3_client.py             # AWS S3 operations
│   └── s3_reader.py                 # S3 data reading utilities
├── data_processors/                 # Data processing modules
│   ├── sentiment_analyzer.py        # Multi-model sentiment analysis
│   ├── database_manager.py          # Database operations
│   ├── performance_pipeline.py      # Performance analysis pipeline
│   └── streamlined_sentiment_pipeline.py  # Optimized sentiment pipeline
├── workflows/                       # Workflow orchestration
│   ├── reddit_workflow.py           # Reddit data workflow
│   ├── aws_workflow.py              # AWS S3 workflow
│   └── sentiment_workflow.py        # Sentiment analysis workflow
├── dags/                           # Airflow DAG definitions
│   └── reddit_dag.py               # Main orchestration DAG
├── visualization/                   # Data visualization
│   └── sentiment_dashboard.py      # Interactive dashboard
├── api/                            # REST API endpoints
│   └── app.py                      # FastAPI application
├── tests/                          # Test suite
│   └── test_sentiment_analysis.py  # Comprehensive tests
├── utils/                          # Utility functions
│   └── constants.py                # Configuration constants
├── data/                           # Data storage
│   ├── input/                      # Raw data input
│   └── output/                     # Processed data output
├── logs/                           # Airflow execution logs
├── requirements.txt                # Python dependencies
├── docker-compose.yml              # Docker orchestration
├── Dockerfile                      # Container definition
├── Dockerfile.api                  # API container definition
└── README.md                       # This file
```

## Usage Examples

### Basic Sentiment Analysis

```python
from data_processors.sentiment_analyzer import LakersSentimentAnalyzer

# Initialize the analyzer
analyzer = LakersSentimentAnalyzer()

# Analyze a single text
text = "LeBron James is playing amazing basketball this season!"
result = analyzer.analyze_sentiment_comprehensive(text)

print(f"VADER Score: {result['vader_compound']}")
print(f"TextBlob Polarity: {result['textblob_polarity']}")
print(f"Mentioned Players: {result['mentioned_players']}")
print(f"Overall Sentiment: {result['overall_sentiment']}")
```

### NBA Data Collection

```python
from data_collectors.nba_collector import NBADataCollector

# Initialize the collector
collector = NBADataCollector()

# Get player performance data
player_id = 2544  # LeBron James
game_logs = collector.get_player_game_logs(player_id, season="2025-26")

# Get season statistics
season_stats = collector.get_player_season_stats(player_id, season="2025-26")
print(f"Points per Game: {season_stats.get('PTS', 'N/A')}")
print(f"Rebounds per Game: {season_stats.get('REB', 'N/A')}")
print(f"Assists per Game: {season_stats.get('AST', 'N/A')}")
```

### Full Pipeline Execution

```python
from data_processors.performance_pipeline import SentimentPerformancePipeline

# Initialize the pipeline
pipeline = SentimentPerformancePipeline()

# Run complete analysis
results = pipeline.run_full_pipeline(
    subreddit='lakers',
    time_filter='day',
    limit=100,
    nba_days=30,
    correlation_days=30
)

print(f"Pipeline Status: {results['pipeline_success']}")
print(f"Posts Processed: {results['posts_processed']}")
print(f"Correlation Results: {results['correlation_results']}")
```

### Database Operations

```python
from data_processors.database_manager import DatabaseManager

# Initialize database manager
db = DatabaseManager()

# Store sentiment data
success = db.store_reddit_data_with_sentiment(
    processed_df, 
    summary_data, 
    'lakers', 
    '2025-01-08'
)

# Query historical data
historical_data = db.get_sentiment_data_by_date_range(
    start_date='2025-01-01',
    end_date='2025-01-08'
)
```

## Configuration Details

### Reddit API Setup

1. Create a Reddit application at https://www.reddit.com/prefs/apps
2. Note your client ID and secret
3. Update `config/config.conf` with your credentials

### NBA API Configuration

- No authentication required
- Rate limiting: 60 requests per minute
- Base URL: https://stats.nba.com/stats

### Database Configuration

The system uses PostgreSQL with the following tables:
- `reddit_posts`: Raw Reddit post data
- `sentiment_analysis`: Processed sentiment scores
- `nba_player_stats`: NBA player statistics
- `correlation_analysis`: Sentiment-performance correlations

### AWS S3 Configuration

- Used for data lake storage
- Configure bucket name and region
- Set up IAM credentials with S3 access

## Testing

### Run Test Suite

```bash
python -m pytest tests/ -v
```

### Test Coverage

The test suite covers:
- Sentiment analysis accuracy
- Database operations
- NBA data collection
- Pipeline integration
- Error handling
- Data validation

### Manual Testing

```bash
# Test Reddit data collection
python -c "from data_collectors.reddit_collector import connect_reddit; print('Reddit connection:', connect_reddit('test', 'test', 'test'))"

# Test NBA data collection
python -c "from data_collectors.nba_collector import NBADataCollector; collector = NBADataCollector(); print('NBA collector initialized')"

# Test sentiment analysis
python -c "from data_processors.sentiment_analyzer import LakersSentimentAnalyzer; analyzer = LakersSentimentAnalyzer(); print('Sentiment analyzer initialized')"
```

## Monitoring and Maintenance

### Log Monitoring

- Airflow logs: `logs/scheduler/` and `logs/dag_processor_manager/`
- Application logs: Check console output during execution
- Database logs: Monitor PostgreSQL logs for performance

### Performance Optimization

- Database indexing on frequently queried columns
- S3 data partitioning by date
- Airflow task parallelism configuration
- Memory optimization for large datasets

### Data Quality Checks

- Automated validation of Reddit data completeness
- NBA API response validation
- Sentiment score range validation
- Database constraint enforcement

## Troubleshooting

### Common Issues

1. **Reddit API Rate Limiting**
   - Solution: Implement exponential backoff
   - Check: Reddit API status and quotas

2. **NBA API Timeouts**
   - Solution: Increase timeout values
   - Check: Network connectivity and API status

3. **Database Connection Issues**
   - Solution: Verify PostgreSQL service status
   - Check: Connection parameters and credentials

4. **Memory Issues with Large Datasets**
   - Solution: Implement data chunking
   - Check: Available system memory

### Debug Mode

Enable debug logging by setting:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Make your changes
5. Add tests for new functionality
6. Run the test suite: `python -m pytest tests/`
7. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function parameters
- Write comprehensive docstrings
- Include unit tests for new features

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Support

For questions, issues, or contributions:
- Create an issue on GitHub
- Check the documentation in the `docs/` directory
- Review the troubleshooting section above

---

**Built for Lakers fans and data science enthusiasts**