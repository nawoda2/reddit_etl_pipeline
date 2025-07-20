import praw
import sys
from praw import Reddit

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

    # posts_lists = []


    print(posts)
    # for post in posts: