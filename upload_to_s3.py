from pprint import pprint as print
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession
import os

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'smw-walmart-dms')
spark = SparkSession.builder.getOrCreate()

def get_new_or_modified_csv_keys(tables, s3_last_modified):
    csv_keys = []
    path = f'{os.curdir}/data'
    for table in tables:
        files = os.listdir(f'{path}/{table}')
        for file in files:
            last_modified = datetime.fromtimestamp(os.path.getmtime(f'{path}/{table}/{file}'), tz=timezone.utc)
            if not s3_last_modified.get(table, {}).get(file, None) or last_modified > s3_last_modified[table][file]:
                csv_keys.append(f'data/{table}/{file}')
    return csv_keys


def get_s3_last_modified(s3, table_name):
    file_updates = {}
    s3_objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter='/', Prefix=f'data/{table_name}/')['Contents']
    for obj in s3_objects:
        key = obj['Key']
        if key.endswith('.csv'):
            file_name = key.split('/')[-1]
            last_modified = obj['LastModified']
            file_updates[file_name] = last_modified

    return file_updates

def main():
    # Boto3 client
    s3 = boto3.client('s3')

    tables = os.listdir(f'{os.curdir}/data')
    s3_last_modified = {}
    for table in tables:
        s3_last_modified[table] = get_s3_last_modified(s3, table)
    keys = get_new_or_modified_csv_keys(tables, s3_last_modified)
    for key in keys:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=open(f'./{key}', 'rb')
        )

if __name__ == '__main__':
    main()

