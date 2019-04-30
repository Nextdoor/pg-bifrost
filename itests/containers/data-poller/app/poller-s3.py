"""
This script obtains records from Kinesis and writes them to a local file as
as defined by OUT_FILE. It will exit when no additional files have been read for WAIT_TIME.
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
INITIAL_WAIT_TIME = int(os.getenv('INITIAL_WAIT_TIME', '60')) # time to wait for initial list of keys
WAIT_TIME = int(os.getenv('WAIT_TIME', '5')) # incremental time to wait for new keys if none have been seen
EXPECTED_COUNT = int(os.getenv('EXPECTED_COUNT', '1')) # expect number of records (only used for logging)
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


# get initial set of keys with a deadline of INITIAL_WAIT_TIME
all_keys = []
timeout_for_first_keys = time.time() + INITIAL_WAIT_TIME

while True:
    if time.time() > timeout_for_first_keys:
        print("No data received to poller. Exiting.")
        exit(1)

    print("Getting keys list...")
    sys.stdout.flush()
    try:
        all_keys = _get_all_s3_keys(BUCKET_NAME)
        break
    except KeyError:
        time.sleep(1)
        pass

all_keys.sort()

key_i = 0
total = 0

print("Records expected: {}".format(EXPECTED_COUNT))

# Start the moving deadline and iterate over new keys
moving_deadline = time.time() + WAIT_TIME


while time.time() <= moving_deadline:
    if key_i >= len(all_keys):
        # our pointer is past the length of the keys we have seen, so we wait for more...
        print("Waiting for more keys...")
        time.sleep(1)

        # get additional, unique keys and update the moving deadline, then loop back around
        all_keys = list(set(all_keys + _get_all_s3_keys(BUCKET_NAME)))
        all_keys.sort()

        moving_deadline = time.time() + WAIT_TIME
        continue

    record_count = 0

    # get object data
    resp = client.get_object(
        Bucket=BUCKET_NAME,
        Key=all_keys[key_i],
    )

    bytestream = BytesIO(resp['Body'].read())
    got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
    records = got_text.split('\n')

    # filter out any empty lines
    records = filter(None, records)

    sys.stdout.flush()

    # write out a single key/object to a single output file
    with open(OUT_FILE + "." + str(key_i), "a") as fp:
        for record in records:
            fp.write(record)
            fp.write('\n')

        fp.flush()
        record_count += len(records)

    # update pointer in keys read
    key_i += 1

    total += record_count
    print("total so far: {}".format(total))

    if record_count == 0:
        time.sleep(1)
    sys.stdout.flush()

print("Records read {}".format(total))
sys.stdout.flush()
