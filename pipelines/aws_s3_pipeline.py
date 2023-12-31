from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_to_s3
from config.credentials import aws_bucket_name

def upload_s3_pipeline(ti):
    #Get the file_path from the output of the 'reddit_extraction' task
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    #Connect to Amazon S3
    s3 = connect_to_s3()
    #Create the S3 bucket if it doesn't exist
    create_bucket_if_not_exists(s3, aws_bucket_name)
    #Upload the file to S3 using the obtained file_path
    upload_to_s3(s3, file_path, aws_bucket_name, file_path.split('/')[-1])
    