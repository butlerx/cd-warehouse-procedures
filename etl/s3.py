import json

import botocore
from boto3 import Session

with open('./config/config.json') as data:
    data = json.load(data)

aws_bucket = data['s3']['bucket']
aws_access_key = data['s3']['access']
aws_secret_key = data['s3']['secret']


def download(db):
    session = Session(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket(aws_bucket)
    sBackups = bucket.objects.filter(Prefix='zen' + db)
    try:
        for obj in sBackups:
            bucket.download_file(obj.key, '/db/' + db + '.tar.gz')
            break
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
