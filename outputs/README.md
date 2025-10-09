# Reddit Sentiment Analysis Outputs

This directory contains all the outputs from the Reddit ETL pipeline with sentiment analysis.

## Directory Structure

```
outputs/
├── raw_data/           # Raw Reddit data before processing
├── sentiment_data/     # Processed data with sentiment analysis
├── reports/           # Analysis reports and summaries
└── README.md          # This file
```

## Files Description

### Raw Data (`raw_data/`)
- **reddit_data_20251008_172817.csv**: Initial Reddit data collection
- **reddit_data_20251008_172841.csv**: Updated Reddit data collection

**Columns:**
- `id`: Reddit post ID
- `title`: Post title
- `selftext`: Post content (if any)
- `author`: Post author
- `score`: Upvotes - downvotes
- `num_comments`: Number of comments
- `upvote_ratio`: Ratio of upvotes to total votes
- `created_utc`: Creation timestamp
- `subreddit`: Source subreddit (r/lakers)

### Sentiment Data (`sentiment_data/`)

#### Post Sentiment Analysis
- **sentiment_analysis_20251008_183047.csv**: Post sentiment analysis results

**Columns:**
- All raw data columns plus:
- `vader_compound`: Vader sentiment compound score (-1 to 1)
- `vader_positive`: Vader positive sentiment score
- `vader_negative`: Vader negative sentiment score
- `vader_neutral`: Vader neutral sentiment score
- `transformer_positive`: Transformer positive probability
- `transformer_negative`: Transformer negative probability
- `transformer_neutral`: Transformer neutral probability
- `transformer_sentiment`: Transformer classification
- `vader_classification`: Derived Vader classification
- `agreement`: Classifier agreement level (agree/disagree)
- `analysis_timestamp`: When sentiment analysis was performed

#### Comment Sentiment Analysis
- **comment_sentiment_analysis_20251008_183038.csv**: Comment sentiment analysis results

**Columns:**
- `comment_id`: Reddit comment ID
- `post_id`: Associated post ID
- `post_title`: Title of the associated post
- `author`: Comment author
- `body`: Comment text content
- `score`: Comment upvotes - downvotes
- `created_utc`: Comment creation timestamp
- `parent_id`: Parent comment ID (for nested comments)
- `is_submitter`: Whether commenter is the post author
- `edited`: Whether comment was edited
- All sentiment analysis columns (same as posts)
- `mentioned_players`: Lakers players mentioned in comment

### Reports (`reports/`)
- **sentiment_analysis_report_20251008_180017.txt**: Comprehensive analysis report

## Data Summary

### Posts
- **Total Posts**: 19 (after duplicate cleanup)
- **Average Score**: 190.9 points
- **Average Comments**: 38.8 per post
- **High-Scoring Posts**: 8 posts with >100 points

### Comments
- **Total Comments**: 86 (with meaningful content)
- **Average Comment Score**: 45.2 points
- **Comments from Submitters**: 5 comments
- **Edited Comments**: 0 comments

### Sentiment Distribution (Posts)

| Classifier | Positive | Negative | Neutral |
|------------|----------|----------|---------|
| Vader      | 37.5%    | 8.3%     | 54.2%   |
| Transformer| 12.5%    | 0.0%     | 87.5%   |

### Sentiment Distribution (Comments)

| Classifier | Positive | Negative | Neutral |
|------------|----------|----------|---------|
| Vader      | 61.6%    | 9.3%     | 29.1%   |
| Transformer| 47.7%    | 16.3%    | 36.0%   |

## Usage

### Viewing Data
- Open CSV files in Excel, Google Sheets, or any spreadsheet application
- Use Python pandas: `pd.read_csv('sentiment_analysis_20251008_175709.csv')`

### API Access
- Live API: http://127.0.0.1:8001
- Interactive docs: http://127.0.0.1:8001/docs

### Database Access
- PostgreSQL database with full data
- Connection details in `config/config.conf`

## Technical Notes

- All timestamps are in UTC
- Sentiment scores are normalized between -1 and 1
- Classifier agreement indicates consistency between different sentiment analysis methods
- Data is updated daily via Airflow DAG

## File Naming Convention

- Raw data: `reddit_data_YYYYMMDD_HHMMSS.csv`
- Post sentiment: `sentiment_analysis_YYYYMMDD_HHMMSS.csv`
- Comment sentiment: `comment_sentiment_analysis_YYYYMMDD_HHMMSS.csv`
- Reports: `sentiment_analysis_report_YYYYMMDD_HHMMSS.txt`

Where YYYYMMDD_HHMMSS is the timestamp when the file was created.
