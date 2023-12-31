from etls.reddit_etl import connect_reddit, extract_posts, load_data_to_csv, transform_data
import pandas as pd
from config.credentials import output_path

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    #connecting to reddit instance
    instance = connect_reddit()
    #extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    #transformation
    post_df = transform_data(post_df) 
    #loading to csv
    file_path = f'{output_path}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)
    
    return file_path