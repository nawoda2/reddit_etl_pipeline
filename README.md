# Lakers Sentiment Analysis Project

A comprehensive data pipeline that analyzes sentiment from Lakers subreddit discussions and correlates it with NBA player performance data to uncover insights about fan perception and on-court performance.

## 🏀 Project Overview

This project combines social media sentiment analysis with sports analytics to answer the question: **How does fan sentiment on Reddit correlate with Lakers players' on-court performance?**

### Key Features

- **Advanced Sentiment Analysis**: Uses multiple NLP models (VADER, TextBlob, RoBERTa) for robust sentiment scoring
- **Player-Specific Analysis**: Identifies and analyzes sentiment for individual Lakers players
- **NBA Data Integration**: Collects real-time player performance statistics
- **Correlation Analysis**: Statistical analysis of sentiment vs performance relationships
- **Interactive Visualizations**: Dynamic dashboards for data exploration
- **Scalable Architecture**: Built with Airflow, PostgreSQL, and S3 for production use

## 🏗️ Architecture

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

## 📊 Data Flow

1. **Data Collection**
   - Reddit posts from r/lakers subreddit
   - NBA player statistics and game logs
   - Raw data stored in S3 data lake

2. **Data Processing**
   - Text preprocessing and player mention extraction
   - Multi-model sentiment analysis
   - Performance data normalization

3. **Data Storage**
   - Processed data stored in PostgreSQL
   - Structured tables for efficient querying
   - Historical data retention

4. **Analysis & Visualization**
   - Correlation analysis between sentiment and performance
   - Interactive dashboards and charts
   - Automated reporting

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 12+
- Apache Airflow 2.7+
- Docker (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd reddit_project
   ```

2. **Create virtual environment**
   ```bash
   python -m venv reddit_env
   source reddit_env/bin/activate  # On Windows: reddit_env\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Download spaCy model**
   ```bash
   python -m spacy download en_core_web_sm
   ```

5. **Configure environment**
   ```bash
   cp config/config_template.conf config/config.conf
   # Edit config.conf with your API keys and database credentials
   ```

6. **Set up PostgreSQL database**
   ```sql
   CREATE DATABASE airflow_reddit;
   CREATE USER postgres WITH PASSWORD 'postgres';
   GRANT ALL PRIVILEGES ON DATABASE airflow_reddit TO postgres;
   ```

7. **Initialize database tables**
   ```bash
   python -c "from etls.database_etl import DatabaseManager; db = DatabaseManager(); db.create_tables()"
   ```

### Running the Pipeline

1. **Start Airflow**
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

2. **Trigger the DAG**
   - Open Airflow UI at http://localhost:8080
   - Find the `etl_reddit_pipeline` DAG
   - Click "Trigger DAG"

3. **Run individual components**
   ```bash
   # Test sentiment analysis
   python etls/sentiment_analysis.py
   
   # Test NBA data collection
   python etls/nba_data_etl.py
   
   # Test full pipeline
   python pipelines/sentiment_performance_pipeline.py
   ```

## 🧪 Testing

Run the comprehensive test suite:

```bash
python tests/test_sentiment_analysis.py
```

The test suite covers:
- Sentiment analysis functionality
- Database operations
- NBA data collection
- Pipeline integration
- End-to-end workflows

## 📈 Usage Examples

### Basic Sentiment Analysis

```python
from etls.sentiment_analysis import LakersSentimentAnalyzer

analyzer = LakersSentimentAnalyzer()

# Analyze a single text
result = analyzer.analyze_sentiment_comprehensive(
    "LeBron James is playing amazing basketball this season!"
)

print(f"Sentiment: {result['vader_compound']}")
print(f"Mentioned Players: {result['mentioned_players']}")
```

### Player Performance Analysis

```python
from etls.nba_data_etl import NBADataCollector

collector = NBADataCollector()

# Get LeBron's recent performance
summary = collector.get_player_performance_summary('lebron', days=30)
print(f"Average Points: {summary['avg_points']}")
print(f"Win Percentage: {summary['win_percentage']}")
```

### Full Pipeline Execution

```python
from pipelines.sentiment_performance_pipeline import SentimentPerformancePipeline

pipeline = SentimentPerformancePipeline()

# Run complete analysis
results = pipeline.run_full_pipeline(
    subreddit='lakers',
    time_filter='day',
    limit=100,
    nba_days=30,
    correlation_days=30
)

print(f"Analysis completed: {results['pipeline_success']}")
```

### Data Visualization

```python
from visualization.sentiment_dashboard import SentimentDashboard

dashboard = SentimentDashboard()

# Create sentiment trend chart
fig = dashboard.create_sentiment_trend_chart(sentiment_data, 'lebron')
fig.show()

# Create correlation heatmap
fig = dashboard.create_correlation_heatmap(correlation_data)
fig.show()
```

## 📊 Key Metrics

### Sentiment Analysis Metrics
- **VADER Compound Score**: Overall sentiment (-1 to 1)
- **TextBlob Polarity**: Sentiment polarity (-1 to 1)
- **TextBlob Subjectivity**: Opinion vs fact (0 to 1)
- **Transformer Sentiment**: Advanced model classification

### Performance Metrics
- **Points per Game**: Average points scored
- **Rebounds/Assists**: Key performance indicators
- **Plus/Minus**: Team impact metric
- **Win Percentage**: Game outcome correlation

### Correlation Analysis
- **Pearson Correlation**: Linear relationship strength
- **P-value**: Statistical significance
- **Sample Size**: Data reliability indicator

## 🔧 Configuration

### API Keys Required

1. **Reddit API**
   - Client ID and Secret from Reddit app
   - User agent string

2. **NBA Stats API**
   - No authentication required
   - Rate limiting applied

3. **AWS S3** (Optional)
   - Access key and secret
   - Bucket name and region

### Database Configuration

```ini
[database]
database_host = localhost
database_name = airflow_reddit
database_port = 5432
database_username = postgres
database_password = postgres
```

## 📁 Project Structure

```
reddit_project/
├── config/                 # Configuration files
│   ├── config.conf        # Main configuration
│   └── config_template.conf
├── dags/                  # Airflow DAGs
│   └── reddit_dag.py     # Main orchestration DAG
├── etls/                  # ETL modules
│   ├── reddit_etl.py     # Reddit data extraction
│   ├── sentiment_analysis.py  # Sentiment analysis
│   ├── database_etl.py   # Database operations
│   ├── nba_data_etl.py   # NBA data collection
│   └── aws_etl.py        # S3 operations
├── pipelines/             # Data pipelines
│   ├── reddit_pipeline.py
│   ├── aws_s3_pipeline.py
│   └── sentiment_performance_pipeline.py
├── tests/                 # Test suite
│   └── test_sentiment_analysis.py
├── visualization/         # Dashboard and charts
│   └── sentiment_dashboard.py
├── utils/                 # Utilities
│   └── constants.py      # Configuration constants
├── data/                  # Data storage
│   └── output/           # CSV outputs
├── logs/                  # Airflow logs
├── requirements.txt       # Python dependencies
├── docker-compose.yml     # Docker setup
├── Dockerfile            # Container definition
└── README.md             # This file
```

## 🎯 Key Insights

### Sentiment Analysis Findings
- **Multi-model approach** provides more robust sentiment scoring
- **Player-specific mentions** enable targeted analysis
- **Context-aware models** (RoBERTa) handle sports terminology better
- **Temporal analysis** reveals sentiment trends over time

### Performance Correlation Insights
- **Strong correlations** found between sentiment and key performance metrics
- **Win/loss impact** significantly affects fan sentiment
- **Individual player analysis** reveals unique patterns
- **Statistical significance** varies by player and metric

## 🔮 Future Enhancements

### Planned Features
- **Real-time streaming** analysis
- **Machine learning** prediction models
- **Social media expansion** (Twitter, Instagram)
- **Advanced visualizations** (3D charts, animations)
- **API endpoints** for external access
- **Mobile dashboard** application

### Research Opportunities
- **Causal analysis** of sentiment on performance
- **Sentiment prediction** models
- **Cross-team comparisons**
- **Historical trend analysis**
- **Fan engagement metrics**

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run linting
flake8 .
black .

# Run tests
pytest tests/
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Reddit API** for social media data access
- **NBA Stats API** for player performance data
- **Hugging Face** for transformer models
- **Apache Airflow** for workflow orchestration
- **Plotly** for interactive visualizations

## 📞 Support

For questions, issues, or contributions:
- Create an issue on GitHub
- Contact: [your-email@example.com]
- Documentation: [project-wiki-url]

---

**Built with ❤️ for Lakers fans and data enthusiasts**
