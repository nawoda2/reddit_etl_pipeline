#!/usr/bin/env python3
"""
Simple API to serve Reddit sentiment data
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

app = FastAPI(
    title="Reddit Sentiment Analysis API",
    description="Simple API for Reddit sentiment data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database
db_manager = DatabaseManager()

class SentimentSummary(BaseModel):
    total_posts: int
    avg_sentiment: float
    positive_posts: int
    negative_posts: int
    neutral_posts: int

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Reddit Sentiment Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "posts": "/posts",
            "sentiment": "/sentiment",
            "health": "/health"
        }
    }

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/posts")
async def get_posts():
    """Get all Reddit posts with sentiment data"""
    try:
        with db_manager.engine.connect() as conn:
            query = text("""
                SELECT 
                    rp.id,
                    rp.title,
                    rp.author,
                    rp.score,
                    rp.num_comments,
                    rp.upvote_ratio,
                    rp.created_utc,
                    ss.vader_compound,
                    -- TextBlob fields removed
                    ss.transformer_sentiment
                FROM reddit_posts rp
                JOIN sentiment_scores ss ON rp.id = ss.post_id
                ORDER BY rp.score DESC
            """)
            
            result = conn.execute(query)
            posts = []
            for row in result:
                posts.append({
                    "id": row[0],
                    "title": row[1],
                    "author": row[2],
                    "score": row[3],
                    "num_comments": row[4],
                    "upvote_ratio": float(row[5]),
                    "created_utc": row[6].isoformat() if row[6] else None,
                    "vader_sentiment": float(row[7]) if row[7] else None,
                    "transformer_sentiment": row[8]
                })
            
            return {"posts": posts, "count": len(posts)}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sentiment")
async def get_sentiment_summary():
    """Get sentiment analysis summary"""
    try:
        with db_manager.engine.connect() as conn:
            # Get sentiment summary
            query = text("""
                SELECT 
                    COUNT(*) as total_posts,
                    ROUND(AVG(vader_compound)::numeric, 3) as avg_sentiment,
                    COUNT(CASE WHEN vader_compound > 0.05 THEN 1 END) as positive_posts,
                    COUNT(CASE WHEN vader_compound < -0.05 THEN 1 END) as negative_posts,
                    COUNT(CASE WHEN vader_compound BETWEEN -0.05 AND 0.05 THEN 1 END) as neutral_posts
                FROM sentiment_scores
            """)
            
            result = conn.execute(query)
            row = result.fetchone()
            
            return {
                "total_posts": row[0],
                "average_sentiment": float(row[1]) if row[1] else 0,
                "positive_posts": row[2],
                "negative_posts": row[3],
                "neutral_posts": row[4],
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/posts/{post_id}")
async def get_post(post_id: str):
    """Get specific post by ID"""
    try:
        with db_manager.engine.connect() as conn:
            query = text("""
                SELECT 
                    rp.id,
                    rp.title,
                    rp.selftext,
                    rp.author,
                    rp.score,
                    rp.num_comments,
                    rp.upvote_ratio,
                    rp.created_utc,
                    ss.vader_compound,
                    ss.textblob_polarity,
                    ss.transformer_sentiment
                FROM reddit_posts rp
                JOIN sentiment_scores ss ON rp.id = ss.post_id
                WHERE rp.id = :post_id
            """)
            
            result = conn.execute(query, {"post_id": post_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Post not found")
            
            return {
                "id": row[0],
                "title": row[1],
                "selftext": row[2],
                "author": row[3],
                "score": row[4],
                "num_comments": row[5],
                "upvote_ratio": float(row[6]) if row[6] else None,
                "created_utc": row[7].isoformat() if row[7] else None,
                "vader_sentiment": float(row[8]) if row[8] else None,
                "textblob_sentiment": float(row[9]) if row[9] else None,
                "transformer_sentiment": row[10]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
