import praw
import sys
from praw import Reddit
import pandas as pd
import numpy as np

from utils.constants import POST_FIELDS

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:

    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent)
        print("Connected to Reddit API successfully.")
        return reddit
    except Exception as e:
        print(f"Failed to connect to Reddit API: {e}")
        sys.exit(1)



def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter:str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    posts_lists = []


    # print(posts)
    for post in posts:
        post_dict = vars(post)
        # print(post_dict)
        post = {key: post_dict[key] for key in POST_FIELDS}
        posts_lists.append(post)

    return posts_lists


def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    post_df['subreddit'] = post_df['subreddit'].astype(str)  # Convert subreddit object to string
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(float)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['title'] = post_df['title'].astype(str)


    return post_df

def extract_comments(reddit_instance: Reddit, post_id: str, limit: int = 100):
    """
    Extract comments for a specific Reddit post
    
    Args:
        reddit_instance: Reddit API instance
        post_id: Reddit post ID
        limit: Maximum number of comments to extract
        
    Returns:
        List of comment dictionaries
    """
    try:
        submission = reddit_instance.submission(id=post_id)
        submission.comments.replace_more(limit=0)  # Remove "more comments" placeholders
        
        comments_list = []
        
        for comment in submission.comments.list()[:limit]:
            if hasattr(comment, 'body') and comment.body != '[deleted]' and comment.body != '[removed]':
                comment_dict = {
                    'id': comment.id,
                    'post_id': post_id,
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'body': comment.body,
                    'score': comment.score,
                    'created_utc': comment.created_utc,
                    'parent_id': comment.parent_id,
                    'is_submitter': comment.is_submitter,
                    'edited': comment.edited
                }
                comments_list.append(comment_dict)
        
        return comments_list
        
    except Exception as e:
        print(f"Error extracting comments for post {post_id}: {e}")
        return []


def extract_posts_with_comments(reddit_instance: Reddit, subreddit: str, time_filter: str, limit: int = None, comment_limit: int = 50):
    """
    Extract Reddit posts along with their top comments
    
    Args:
        reddit_instance: Reddit API instance
        subreddit: Subreddit name
        time_filter: Time filter for posts ('day', 'week', 'month', 'year', 'all')
        limit: Maximum number of posts to extract
        comment_limit: Maximum number of comments per post
        
    Returns:
        Tuple of (posts_list, comments_list)
    """
    posts_list = extract_posts(reddit_instance, subreddit, time_filter, limit)
    all_comments = []
    
    print(f"Extracting comments for {len(posts_list)} posts...")
    
    for i, post in enumerate(posts_list):
        post_id = post['id']
        print(f"Extracting comments for post {i+1}/{len(posts_list)}: {post_id}")
        
        comments = extract_comments(reddit_instance, post_id, comment_limit)
        all_comments.extend(comments)
        
        print(f"  Found {len(comments)} comments")
    
    print(f"Total comments extracted: {len(all_comments)}")
    return posts_list, all_comments


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)


def load_comments_to_csv(comments_df: pd.DataFrame, filename: str):
    comments_df.to_csv(filename, index=False)
    print(f"Comments saved to {filename}")