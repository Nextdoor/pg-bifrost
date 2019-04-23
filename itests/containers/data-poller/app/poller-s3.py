"""
This script obtains records from Kinesis and writes them to a local file as
as defined by OUT_FILE. It will exit when either EXPECTED_COUNT or WAIT_TIME
is reached.
"""

import os
import sys
import time

import boto3
from botocore import exceptions
from gzip import GzipFile
from io import BytesIO
from retry import retry

# Variables
OUT_FILE = os.getenv('OUT_FILE', '/output/test')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'itests')
CREATE_BUCKET = bool(os.getenv('CREATE_BUCKET', '1'))
ENDPOINT_URL = os.getenv('ENDPOINT_URL', 'http://localstack:4572')
WAIT_TIME = int(os.getenv('WAIT_TIME', '90'))
EXPECTED_COUNT = int(os.getenv('EXPECTED_COUNT', '1'))
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

client = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      region_name=AWS_REGION)

# Create a bucket
# @retry(exceptions.EndpointConnectionError, tries=10, delay=.5)
def _create_bucket(name):
    print("Trying to create bucket {}".format(name))
    return client.create_bucket(
        Bucket=name)


@retry(exceptions.EndpointConnectionError, tries=10, delay=.5)
def _get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    resp = client.list_objects(Bucket=bucket)

    file_list = resp['Contents']

    for s3_key in file_list:
        keys.append(s3_key['Key'])

    return keys


if CREATE_BUCKET:
    # Create the bucket
    print("Creating a bucket")
    try:
        _create_bucket(BUCKET_NAME)
    except exceptions.EndpointConnectionError:
        print("Unable to contact endpoint at {}".format(ENDPOINT_URL))
        exit(1)
    except exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e


keys = None

timeout = time.time() + 60*5
while True:
    if time.time() > timeout:
        print("No data received to poller. Exiting.")
        exit(1)

    print("Getting keys list...")
    sys.stdout.flush()
    try:
        keys = _get_all_s3_keys(BUCKET_NAME)
        break
    except KeyError:
        time.sleep(2)
        pass

# Start timer and iterate over keys
end = time.time() + WAIT_TIME

print("Records expected: {}".format(EXPECTED_COUNT))

key_i = 0
total = 0

print("Records expected: {}".format(EXPECTED_COUNT))

while total < EXPECTED_COUNT:
    if time.time() >= end:
        break

    record_count = 0

    resp = client.get_object(
        Bucket=BUCKET_NAME,
        Key=keys[key_i],
    )

    bytestream = BytesIO(resp['Body'].read())
    got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
    records = got_text.split('\n')
    sys.stdout.flush()

    with open(OUT_FILE + "." + str(key_i), "a") as fp:
        for record in records:
            fp.write(record)
            fp.write('\n')

        fp.flush()

        record_count += len(records)

    key_i += 1
    total += record_count
    print("total so far: {}".format(total))

    if record_count == 0:
        time.sleep(1)
    sys.stdout.flush()

print("Records read {}".format(total))
sys.stdout.flush()
