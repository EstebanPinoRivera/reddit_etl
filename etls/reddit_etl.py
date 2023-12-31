import praw
import sys
from praw import Reddit
from config.credentials import reddit_client_id, reddit_secret_key, password, user_agent, username
import pandas as pd
import numpy as np

def connect_reddit():
    try:
        reddit = praw.Reddit(
                        client_id=reddit_client_id,
                        client_secret=reddit_secret_key,
                        password=password,
                        user_agent=user_agent,
                        username=username,)
        print(reddit.user.me())
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

def extract_posts(reddit_instance: Reddit, subreddit_name: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit_name)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    post_lists = []
    
    POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)
    
    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
    
    return post_lists

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = post_df['over_18'] == True
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)
    
    return post_df
    

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
         
        