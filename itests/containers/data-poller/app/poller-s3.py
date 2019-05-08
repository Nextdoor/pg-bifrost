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
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
EXPECTED_COUNT = int(os.getenv('EXPECTED_COUNT', '1')) # expect number of records (only used for logging)
INITIAL_WAIT_TIME = int(os.getenv('S3_POLLER_INITIAL_WAIT_TIME', '90')) # time to wait for initial list of keys
WAIT_TIME = int(os.getenv('S3_POLLER_WAIT_TIME', '10')) # incremental time to wait for new keys if none have been seen
MAP_KEYS_TO_OUTPUT_FILES = bool(os.getenv('S3_POLLER_MAP_KEYS_TO_OUTPUT_FILES', '')) # whether to create a single output file

client = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      region_name=AWS_REGION)

# Create a bucket
@retry(exceptions.EndpointConnectionError, tries=10, delay=.5)
def _create_bucket(name):
    print("Trying to create bucket {}".format(name))
    return client.create_bucket(
        Bucket=name)


@retry(ValueError, tries=10, delay=.5)
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

    print("Getting initial keys list...")
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
        sys.stdout.flush()
        time.sleep(1)

        remote_keys = _get_all_s3_keys(BUCKET_NAME)
        if len(remote_keys) > len(all_keys):
            # if there are new keys, update our all_keys list and process
            all_keys = list(set(all_keys + remote_keys))
            all_keys.sort()

            # update deadline as if we had new keys
            moving_deadline = time.time() + WAIT_TIME
        else:
            # else, look back around
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

    # By default we only create a single file no matter how many S3 keys we have
    _file_num = 0

    if MAP_KEYS_TO_OUTPUT_FILES:
        _file_num = key_i

    with open(OUT_FILE + "." + str(_file_num), "a") as fp:
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
