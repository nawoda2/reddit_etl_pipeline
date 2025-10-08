"""
FastAPI Backend for Reddit Sentiment Analysis Dashboard
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import sys

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processors.database_manager import RedditDatabaseETL

app = FastAPI(
    title="Reddit Sentiment Analysis API",
    description="API for Lakers subreddit sentiment analysis dashboard",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database ETL
db_etl = RedditDatabaseETL()

# Pydantic models for API responses
class SentimentSummary(BaseModel):
    total_posts: int
    avg_sentiment_compound: float
    positive_posts: int
    negative_posts: int
    neutral_posts: int
    avg_engagement: float
    avg_text_length: float

class PostData(BaseModel):
    id: str
    title: str
    author: str
    score: int
    num_comments: int
    created_utc: str
    sentiment_compound: float
    sentiment_label: str
    engagement_score: float

class TrendData(BaseModel):
    date: str
    avg_sentiment_compound: float
    positive_posts: int
    negative_posts: int
    neutral_posts: int
    total_posts: int

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    if not db_etl.connect():
        raise Exception("Failed to connect to database")

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Reddit Sentiment Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "sentiment_summary": "/api/sentiment/summary",
            "recent_posts": "/api/posts/recent",
            "sentiment_trends": "/api/sentiment/trends",
            "dashboard": "/dashboard"
        }
    }

@app.get("/api/sentiment/summary")
async def get_sentiment_summary(subreddit: str = "lakers", days: int = 7):
    """Get sentiment summary for the specified period"""
    try:
        # Get recent posts
        df = db_etl.get_recent_posts(subreddit, limit=1000)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Filter by date range
        cutoff_date = datetime.now() - timedelta(days=days)
        df['created_utc'] = pd.to_datetime(df['created_utc'])
        df = df[df['created_utc'] >= cutoff_date]
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for the specified period")
        
        # Calculate summary
        summary = {
            "total_posts": len(df),
            "avg_sentiment_compound": df['sentiment_compound'].mean(),
            "positive_posts": len(df[df['sentiment_label'] == 'positive']),
            "negative_posts": len(df[df['sentiment_label'] == 'negative']),
            "neutral_posts": len(df[df['sentiment_label'] == 'neutral']),
            "avg_engagement": df['engagement_score'].mean(),
            "avg_text_length": df['text_length'].mean()
        }
        
        return summary
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/posts/recent")
async def get_recent_posts(subreddit: str = "lakers", limit: int = 50, 
                          sentiment_filter: Optional[str] = None):
    """Get recent posts with optional sentiment filtering"""
    try:
        df = db_etl.get_recent_posts(subreddit, limit=limit)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No posts found")
        
        # Filter by sentiment if specified
        if sentiment_filter and sentiment_filter in ['positive', 'negative', 'neutral']:
            df = df[df['sentiment_label'] == sentiment_filter]
        
        # Select relevant columns
        posts = df[['id', 'title', 'author', 'score', 'num_comments', 
                   'created_utc', 'sentiment_compound', 'sentiment_label', 
                   'engagement_score']].to_dict('records')
        
        return posts
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sentiment/trends")
async def get_sentiment_trends(subreddit: str = "lakers", days: int = 30):
    """Get sentiment trends over time"""
    try:
        df = db_etl.get_sentiment_trends(subreddit, days)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No trend data found")
        
        # Convert to list of dictionaries
        trends = df.to_dict('records')
        
        return trends
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sentiment/distribution")
async def get_sentiment_distribution(subreddit: str = "lakers", days: int = 7):
    """Get sentiment distribution for visualization"""
    try:
        df = db_etl.get_recent_posts(subreddit, limit=1000)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Filter by date range
        cutoff_date = datetime.now() - timedelta(days=days)
        df['created_utc'] = pd.to_datetime(df['created_utc'])
        df = df[df['created_utc'] >= cutoff_date]
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for the specified period")
        
        # Calculate distribution
        distribution = df['sentiment_label'].value_counts().to_dict()
        
        return {
            "positive": distribution.get('positive', 0),
            "negative": distribution.get('negative', 0),
            "neutral": distribution.get('neutral', 0)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/posts/top")
async def get_top_posts(subreddit: str = "lakers", limit: int = 10, 
                       sort_by: str = "engagement_score"):
    """Get top posts by engagement or sentiment"""
    try:
        df = db_etl.get_recent_posts(subreddit, limit=1000)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No posts found")
        
        # Sort by specified column
        if sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=False)
        else:
            df = df.sort_values(by='engagement_score', ascending=False)
        
        # Get top posts
        top_posts = df.head(limit)[['id', 'title', 'author', 'score', 'num_comments', 
                                   'sentiment_compound', 'sentiment_label', 'engagement_score']].to_dict('records')
        
        return top_posts
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard HTML"""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Lakers Reddit Sentiment Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                overflow: hidden;
            }
            .header {
                background: linear-gradient(135deg, #ff6b6b, #ffa500);
                color: white;
                padding: 30px;
                text-align: center;
            }
            .header h1 {
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }
            .header p {
                margin: 10px 0 0 0;
                opacity: 0.9;
            }
            .dashboard-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                padding: 30px;
            }
            .card {
                background: white;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.08);
                border: 1px solid #f0f0f0;
            }
            .card h3 {
                margin: 0 0 15px 0;
                color: #333;
                font-size: 1.2em;
            }
            .metric {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 10px 0;
                border-bottom: 1px solid #f0f0f0;
            }
            .metric:last-child {
                border-bottom: none;
            }
            .metric-label {
                color: #666;
            }
            .metric-value {
                font-weight: bold;
                color: #333;
            }
            .sentiment-positive { color: #4CAF50; }
            .sentiment-negative { color: #f44336; }
            .sentiment-neutral { color: #ff9800; }
            .chart-container {
                position: relative;
                height: 300px;
                margin-top: 20px;
            }
            .loading {
                text-align: center;
                padding: 40px;
                color: #666;
            }
            .error {
                color: #f44336;
                text-align: center;
                padding: 20px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏀 Lakers Reddit Sentiment Dashboard</h1>
                <p>Real-time sentiment analysis of r/lakers community</p>
            </div>
            
            <div class="dashboard-grid">
                <div class="card">
                    <h3>📊 Sentiment Summary</h3>
                    <div id="sentiment-summary" class="loading">Loading...</div>
                </div>
                
                <div class="card">
                    <h3>📈 Sentiment Distribution</h3>
                    <div class="chart-container">
                        <canvas id="sentiment-chart"></canvas>
                    </div>
                </div>
                
                <div class="card">
                    <h3>📅 Recent Trends</h3>
                    <div class="chart-container">
                        <canvas id="trends-chart"></canvas>
                    </div>
                </div>
                
                <div class="card">
                    <h3>🔥 Top Posts</h3>
                    <div id="top-posts" class="loading">Loading...</div>
                </div>
            </div>
        </div>

        <script>
            // API Base URL
            const API_BASE = window.location.origin;
            
            // Load dashboard data
            async function loadDashboard() {
                try {
                    // Load sentiment summary
                    const summary = await fetch(`${API_BASE}/api/sentiment/summary`).then(r => r.json());
                    displaySentimentSummary(summary);
                    
                    // Load sentiment distribution
                    const distribution = await fetch(`${API_BASE}/api/sentiment/distribution`).then(r => r.json());
                    createSentimentChart(distribution);
                    
                    // Load trends
                    const trends = await fetch(`${API_BASE}/api/sentiment/trends`).then(r => r.json());
                    createTrendsChart(trends);
                    
                    // Load top posts
                    const topPosts = await fetch(`${API_BASE}/api/posts/top`).then(r => r.json());
                    displayTopPosts(topPosts);
                    
                } catch (error) {
                    console.error('Error loading dashboard:', error);
                    document.getElementById('sentiment-summary').innerHTML = '<div class="error">Failed to load data</div>';
                }
            }
            
            function displaySentimentSummary(summary) {
                const container = document.getElementById('sentiment-summary');
                container.innerHTML = `
                    <div class="metric">
                        <span class="metric-label">Total Posts</span>
                        <span class="metric-value">${summary.total_posts}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Avg Sentiment</span>
                        <span class="metric-value sentiment-${summary.avg_sentiment_compound > 0 ? 'positive' : 'negative'}">
                            ${summary.avg_sentiment_compound.toFixed(3)}
                        </span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Positive Posts</span>
                        <span class="metric-value sentiment-positive">${summary.positive_posts}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Negative Posts</span>
                        <span class="metric-value sentiment-negative">${summary.negative_posts}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Neutral Posts</span>
                        <span class="metric-value sentiment-neutral">${summary.neutral_posts}</span>
                    </div>
                `;
            }
            
            function createSentimentChart(distribution) {
                const ctx = document.getElementById('sentiment-chart').getContext('2d');
                new Chart(ctx, {
                    type: 'doughnut',
                    data: {
                        labels: ['Positive', 'Negative', 'Neutral'],
                        datasets: [{
                            data: [distribution.positive, distribution.negative, distribution.neutral],
                            backgroundColor: ['#4CAF50', '#f44336', '#ff9800'],
                            borderWidth: 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'bottom'
                            }
                        }
                    }
                });
            }
            
            function createTrendsChart(trends) {
                const ctx = document.getElementById('trends-chart').getContext('2d');
                const labels = trends.map(t => new Date(t.date).toLocaleDateString());
                const sentimentData = trends.map(t => t.avg_sentiment_compound);
                
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Sentiment Score',
                            data: sentimentData,
                            borderColor: '#667eea',
                            backgroundColor: 'rgba(102, 126, 234, 0.1)',
                            tension: 0.4,
                            fill: true
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true,
                                min: -1,
                                max: 1
                            }
                        }
                    }
                });
            }
            
            function displayTopPosts(posts) {
                const container = document.getElementById('top-posts');
                container.innerHTML = posts.map(post => `
                    <div class="metric">
                        <div>
                            <div style="font-weight: bold; margin-bottom: 5px;">${post.title.substring(0, 60)}...</div>
                            <div style="font-size: 0.9em; color: #666;">
                                by ${post.author} • ${post.score} points • ${post.num_comments} comments
                            </div>
                        </div>
                        <div style="text-align: right;">
                            <div class="sentiment-${post.sentiment_label}">${post.sentiment_label}</div>
                            <div style="font-size: 0.8em; color: #666;">${post.sentiment_compound.toFixed(2)}</div>
                        </div>
                    </div>
                `).join('');
            }
            
            // Load dashboard on page load
            loadDashboard();
            
            // Refresh every 5 minutes
            setInterval(loadDashboard, 300000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


