import os
import sys
import json
import base64
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage


def error_handler(event, context):

    # Print out the data from Pub/Sub, to prove that it worked
    request = json.loads(base64.b64decode(event['data']).decode('utf-8'))['data']
    print(request)

    now: datetime = datetime.now(timezone.utc)
    date: str = now.strftime('%Y-%m-%d')
    time: str = now.strftime('%H:%M:%S.%f')

    request_json: dict = request['message']
    request_json['time'] = time

    bucket_name: str = os.environ.get('LOG_BUCKET',
                                      'aou-curation-omop-dev_transfer_fhir')

    path: str = os.environ.get('LOG_PATH', 'logs')
    site: str = 'unknown'
    if len(request_json['path'].split('/')) > 3:
        site = request_json['path'].split('/')[-4]

    file_path: str = f'{path}/{site}/{date}/errors.csv'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    columns: tuple = ('time', 'status', 'reason', 'exc', 'status_code')

    if not storage.Blob(bucket=bucket, name=file_path).exists(storage_client):
        empty_csv: str = pd.DataFrame([], columns=columns).to_csv(index=False)
        blob.upload_from_string(data=empty_csv, content_type='text/csv')

    logs = pd.read_csv(f'gs://{bucket_name}/{file_path}')
    logs = logs.append(request_json, ignore_index=True)
    blob.upload_from_string(data=logs.to_csv(index=False),
                            content_type='text/csv')
    return request_json
