def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):

    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    