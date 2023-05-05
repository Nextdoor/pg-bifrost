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
from retry import retry

# Variables
OUT_FILE = os.getenv('OUT_FILE', '/output/test')
STREAM_NAME = os.getenv('STREAM_NAME', 'itests')
ENDPOINT_URL = os.getenv('ENDPOINT_URL', 'http://localstack:4566')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
EXPECTED_COUNT = int(os.getenv('EXPECTED_COUNT', '1'))
WAIT_TIME = int(os.getenv('KINESIS_POLLER_WAIT_TIME', '90'))
SHARD_COUNT = int(os.getenv('KINESIS_POLLER_SHARD_COUNT', '1'))

client = boto3.client('kinesis',
                      endpoint_url=ENDPOINT_URL,
                      region_name=AWS_REGION)


# Create a stream
@retry(exceptions.EndpointConnectionError, tries=10, delay=.5)
def _create_stream(name):
    print "Trying to create stream {} with {} shards".format(name, SHARD_COUNT)
    return client.create_stream(
        StreamName=name,
        ShardCount=SHARD_COUNT
    )


@retry(Exception, tries=10, delay=1)
def _check_stream_ready(name):
    print("checking if stream {} is ready".format(name))

    resp = client.describe_stream(
        StreamName=name
    )

    status = resp.get('StreamDescription', []).get('StreamStatus', 'UNKNOWN')
    if status != 'ACTIVE':
        print("stream status: {}".format(status))
        raise Exception("stream not active")


@retry(ValueError, tries=10, delay=.5)
def _get_shard_ids(name):
    response = client.list_shards(
        StreamName=name,
    )

    if len(response['Shards']) != SHARD_COUNT:
        raise ValueError

    shard_ids = []

    for shard in response['Shards']:
        shard_ids.append(shard.get('ShardId'))

    print("shard ids: {}".format(shard_ids))
    return shard_ids


# Create the stream
print "Creating a stream"
try:
    _create_stream(STREAM_NAME)
except exceptions.EndpointConnectionError:
    print "Unable to contact endpoint at {}".format(ENDPOINT_URL)
    exit(1)
except exceptions.ClientError as e:
    if e.response['Error']['Code'] != 'ResourceInUseException':
        raise e

_check_stream_ready(STREAM_NAME)

# Get list of shards
print "Getting shard list..."
shard_ids = _get_shard_ids(STREAM_NAME)

# Get a shard iterator
print "Getting shard iterators"
shard_iterators = []

for shard_id in shard_ids:
    response = client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON',
    )

    shard_iterators.append(response['ShardIterator'])

sys.stdout.flush()

# Iterate over stream
end = time.time() + WAIT_TIME

total = 0

print("Records expected: {}".format(EXPECTED_COUNT))

while total < EXPECTED_COUNT:
    if time.time() >= end:
        break

    record_count = 0

    for i, shard_iterator in enumerate(shard_iterators):
        response = client.get_records(
            ShardIterator=shard_iterator,
            Limit=10000
        )

        shard_iterators[i] = response['NextShardIterator']

        with open(OUT_FILE + "." + str(i), "a") as fp:
            for record in response['Records']:
                fp.write(record['Data'])
                fp.write('\n')

            fp.flush()

        record_count += len(response['Records'])

    total += record_count
    print("{} total so far: {}".format(time.time(), total))

    if record_count == 0:
        time.sleep(1)
    sys.stdout.flush()

# NOTE: 'Records read' is used by the tests framework to know when all expected data has been read.
print("Records read {}".format(total))
sys.stdout.flush()
