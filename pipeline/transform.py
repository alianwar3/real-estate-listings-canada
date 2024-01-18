# DEVELOPER : Ali Anwar
# FILE      : transform.py

import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO



# FUNCTION : lambda_handler
# PURPOSE  : To transform dataset with adding timestamp of point
#            when data was transformed.
def lambda_handler(event, context):

    # write file to s3
    s3 = boto3.client('s3')

    files = []
    for file in s3.list_objects(Bucket='real-estate-canada-etl-project', Prefix='raw-data/')['Contents']:
        file_key = file['Key']
        files.append(file_key)

    # get the latest file from S3
    latest_file_key = files[-1]

    # read the file content into a Pandas DataFrame
    response = s3.get_object(Bucket='real-estate-canada-etl-project', Key=latest_file_key)
    file_content = response['Body'].read()

    # read to dataframe
    df = pd.read_csv(BytesIO(file_content))

    # drop duplicate values
    df = df.drop_duplicates()

    # convert all columns to strings except for 'latitude', 'longitude', and 'price'
    df = df.astype({
        'latitude': 'float64',
        'longitude': 'float64',
        'price': 'int64'
    }, errors='ignore')

    # convert all other columns to strings
    df = df.map(str)

    row_count = len(df)

    # get timestamps
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H:%M:%S")
    timestamps = [formatted_datetime for _ in range(row_count)]
    df['timestamp'] = timestamps  # append new column

    # write to csv file with timestamp in the name
    date_time = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")
    file = f'real-estate-listings-transformed-{date_time}.csv'  # Updated file name
    transformed_data = df.to_csv(index=False)

    s3.put_object(
        Bucket='real-estate-canada-etl-project',
        Key='transformed-data/' + file,
        Body=transformed_data.encode('utf-8')
    )