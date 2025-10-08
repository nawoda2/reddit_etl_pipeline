# 🏀 Lakers Reddit Sentiment Analysis Dashboard

A comprehensive sentiment analysis pipeline for the Lakers subreddit with a beautiful real-time dashboard.

## 🎯 Features

- **Real-time Data Collection**: Automated Reddit post extraction using Airflow
- **Sentiment Analysis**: VADER sentiment analysis for social media text
- **Data Storage**: PostgreSQL database with daily aggregations
- **Beautiful Dashboard**: Interactive web dashboard with charts and metrics
- **REST API**: FastAPI backend for data access
- **Dockerized**: Complete containerized solution

## 🏗️ Architecture

```
Reddit API → Airflow → Sentiment Analysis → PostgreSQL → FastAPI → React Dashboard
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local development)

### 1. Clone and Setup

```bash
git clone <your-repo>
cd reddit_etl_pipeline
```

### 2. Configure Reddit API

Edit `config/config.conf` and add your Reddit API credentials:

```ini
[reddit]
client_id = your_reddit_client_id
client_secret = your_reddit_client_secret
```

### 3. Start Everything

```bash
python start_dashboard.py
```

This will:
- Start all Docker containers
- Initialize the database
- Start Airflow, API, and dashboard services

### 4. Access the Dashboard

- **Dashboard**: http://localhost:8000/dashboard
- **Airflow**: http://localhost:8080 (admin/admin)
- **API Docs**: http://localhost:8000/docs

## 📊 Dashboard Features

### Real-time Metrics
- Total posts analyzed
- Average sentiment score
- Sentiment distribution (positive/negative/neutral)
- Engagement metrics

### Visualizations
- Sentiment distribution pie chart
- Sentiment trends over time
- Top performing posts

### Data Insights
- Most positive/negative posts
- Engagement correlation with sentiment
- Text length analysis

## 🔧 API Endpoints

### Sentiment Data
- `GET /api/sentiment/summary` - Overall sentiment summary
- `GET /api/sentiment/trends` - Sentiment trends over time
- `GET /api/sentiment/distribution` - Sentiment distribution

### Posts Data
- `GET /api/posts/recent` - Recent posts with sentiment
- `GET /api/posts/top` - Top posts by engagement

## 📈 Airflow Pipeline

The pipeline runs daily and includes:

1. **Reddit Extraction**: Collects top posts from r/lakers
2. **S3 Upload**: Stores raw data in AWS S3
3. **Sentiment Analysis**: Analyzes sentiment using VADER
4. **Database Storage**: Stores processed data in PostgreSQL

### Manual Trigger

To run the pipeline manually:
1. Go to http://localhost:8080
2. Find the `etl_reddit_pipeline` DAG
3. Click "Trigger DAG"

## 🛠️ Development

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start database
docker-compose up -d postgres redis

# Run API locally
cd api
python app.py

# Run Airflow locally
airflow webserver --port 8080
airflow scheduler
```

### Project Structure

```
reddit_etl_pipeline/
├── api/                    # FastAPI backend
│   └── app.py             # Main API application
├── dags/                  # Airflow DAGs
│   └── reddit_dag.py      # Main ETL pipeline
├── etls/                  # ETL modules
│   ├── reddit_etl.py      # Reddit data extraction
│   ├── sentiment_analysis.py  # Sentiment analysis
│   └── database_etl.py    # Database operations
├── pipelines/             # Pipeline orchestration
│   ├── reddit_pipeline.py # Reddit data pipeline
│   ├── aws_s3_pipeline.py # S3 upload pipeline
│   └── sentiment_pipeline.py # Sentiment pipeline
├── utils/                 # Utilities
│   └── constants.py       # Configuration constants
├── config/                # Configuration files
│   └── config.conf        # Main configuration
└── data/                  # Data storage
    ├── input/             # Raw data
    └── output/            # Processed data
```

## 🔍 Sentiment Analysis

### VADER Sentiment Analysis

The system uses VADER (Valence Aware Dictionary and sEntiment Reasoner) for sentiment analysis, which is specifically designed for social media text.

### Features
- **Compound Score**: Overall sentiment (-1 to +1)
- **Individual Scores**: Positive, negative, neutral percentages
- **Sentiment Labels**: Categorized as positive, negative, or neutral
- **Confidence Score**: Strength of the sentiment

### Text Preprocessing
- URL removal
- Reddit markdown formatting cleanup
- HTML entity decoding
- Whitespace normalization

## 📊 Database Schema

### reddit_posts
- Post metadata (id, title, author, score, etc.)
- Sentiment scores (compound, positive, negative, neutral)
- Text analysis (length, word count)
- Engagement metrics

### sentiment_daily_summary
- Daily aggregated sentiment data
- Post counts by sentiment
- Average metrics
- Most positive/negative posts

## 🚀 Deployment

### Production Considerations

1. **Environment Variables**: Use environment variables for sensitive data
2. **Database Security**: Configure PostgreSQL with proper authentication
3. **API Security**: Add authentication and rate limiting
4. **Monitoring**: Set up logging and monitoring
5. **Scaling**: Use multiple Airflow workers for high volume

### Docker Production

```bash
# Build production images
docker-compose -f docker-compose-full.yml build

# Run in production mode
docker-compose -f docker-compose-full.yml up -d
```

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check if PostgreSQL is running
   - Verify database credentials in config

2. **Reddit API Rate Limits**
   - Reduce the limit parameter in the DAG
   - Add delays between requests

3. **Sentiment Analysis Errors**
   - Check if vaderSentiment is installed
   - Verify text preprocessing

4. **Dashboard Not Loading**
   - Check if API is running on port 8000
   - Verify database has data

### Logs

```bash
# View all logs
docker-compose -f docker-compose-full.yml logs -f

# View specific service logs
docker-compose -f docker-compose-full.yml logs -f api
docker-compose -f docker-compose-full.yml logs -f airflow-webserver
```

## 📝 Configuration

### Reddit API Setup

1. Go to https://www.reddit.com/prefs/apps
2. Create a new application
3. Note the client ID and secret
4. Update `config/config.conf`

### AWS S3 Setup

1. Create an S3 bucket
2. Set up IAM user with S3 permissions
3. Update AWS credentials in `config/config.conf`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Reddit API for data access
- VADER for sentiment analysis
- Airflow for workflow orchestration
- FastAPI for the backend API
- Chart.js for dashboard visualizations


