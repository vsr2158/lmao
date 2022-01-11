import boto3
import io
import pandas as pd

S3_BUCKET = '432177588334-quotas'
AWS_REGION = 'us-east-1'
QUOTAS_FILE = 'quotas.csv'

s3_resource = boto3.resource('s3', region_name = AWS_REGION)
s3_object = s3_resource.Object(S3_BUCKET,QUOTAS_FILE)

with io.BytesIO(s3_object.get()['Body'].read()) as f:
    df = pd.read_csv(f)

print(df)

