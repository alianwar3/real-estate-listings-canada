# DEVELOPER : Ali Anwar
# FILE      : fetch_df_from_s3.py

import boto3
import calendar
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()



# create an S3 client
s3 = boto3.client(
            's3',
            aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        )


# FUNCTION  : fetch_df_from_s3
# PURPOSE   : To get the latest file from s3 bucket
def fetch_df_from_s3():
    bucket = os.environ['S3_BUCKET']
    key = os.environ['S3_KEY']

    # get files stored in s3
    files = []
    response = s3.list_objects(Bucket=bucket, Prefix=key)
    for obj in response['Contents']:
        if 'transformed-data/real-estate' in obj['Key']:
            files.append(obj['Key'])

    # get object
    response_obj = s3.get_object(Bucket=bucket, Key=files[-1])
    csv_content = response_obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    parts = files[-1].split("-transformed-", 1)  # Use 1 as the second argument to split only once
    timestamp = parts[-1].replace('.csv', '').split('-')

    # timestamp
    month = calendar.month_name[int(timestamp[1])]
    day = timestamp[2]
    year = timestamp[0]
    hour = timestamp[3]
    min = timestamp[4]
    sec = timestamp[5]

    refresh_date = f'{month} {day}, {year} on {hour}:{min}:{sec}'

    return {'file': files[-1],
            'refresh_date': refresh_date,
            'df': df}