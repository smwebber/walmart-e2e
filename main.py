
import os
import time
import platform

import boto3
from watchdog.observers import Observer

from constants import BUCKET_NAME
from file_listener import FileHandler
from upload_to_s3 import get_new_or_modified_csv_keys, get_s3_last_modified


def main(file_path=None, event_type=None):
    # Boto3 client
    s3 = boto3.client('s3')

    delimiter = '\\' if platform.system() == 'Windows' else '/'
    tables = os.listdir(f'{os.curdir}/data') if not file_path else [file_path.split(delimiter)[-2]]
    s3_last_modified = {}

    for table in tables:
        s3_last_modified[table] = get_s3_last_modified(s3, table)
    keys = get_new_or_modified_csv_keys(tables, s3_last_modified) if event_type != 'deleted' else [file_path.replace('\\', '/')]
    for key in keys:
        if event_type == 'deleted':
            s3.delete_object(Bucket=BUCKET_NAME, Key=key)
            print(f'Deleted {key} from S3')
            continue
        csv = open(f'{os.curdir}/{key}', 'rb')
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=csv
        )
        csv.close()
        print(f'Uploaded {key} to S3')

if __name__ == '__main__':
    # Watchdog observer
    event_handler = FileHandler(main)
    observer = Observer()
    observer.schedule(event_handler, path='data', recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
            print('Checking for new or modified files...')
    except KeyboardInterrupt:
        observer.stop()
    observer.join()