import os
import sys
import time

from retry import retry

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP_HOST = os.getenv('KAFKA_BOOTSTRAP_HOST', 'kafka')
KAFKA_BOOTSTRAP_PORT = os.getenv('KAFKA_BOOTSTRAP_PORT', '9092')
TOPIC_NAME = os.getenv('BIFROST_KAFKA_TOPIC', 'itests')
EXPECTED_COUNT = int(os.getenv('EXPECTED_COUNT', '1'))
WAIT_TIME = int(os.getenv('KAFKA_POLLER_WAIT_TIME', '90'))
OUT_FILE = os.getenv('OUT_FILE', '/output/test')
KAFKA_PARTITION_COUNT = int(os.getenv('KAFKA_PARTITION_COUNT', '1'))

admin_conf = {'bootstrap.servers': f"{KAFKA_BOOTSTRAP_HOST}:{KAFKA_BOOTSTRAP_PORT}"}
admin_client = AdminClient(admin_conf)


consumer_conf = {'bootstrap.servers': f"{KAFKA_BOOTSTRAP_HOST}:{KAFKA_BOOTSTRAP_PORT}",
                 'group.id': "itests",
                 'auto.offset.reset': 'earliest'}
consumer = Consumer(consumer_conf)


@retry(Exception, tries=60, delay=.5)
def _create_topic(name):
    print("Trying to create topic {}".format(name))
    fs = admin_client.create_topics([NewTopic(name, num_partitions=KAFKA_PARTITION_COUNT, replication_factor=1)])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic created: {}".format(topic))
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                return
            raise e
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
            exit(1)

_create_topic(TOPIC_NAME)
# Iterate over stream
end = time.time() + WAIT_TIME

total = 0

print("Records expected: {}".format(EXPECTED_COUNT))

try:
    consumer.subscribe([TOPIC_NAME])
    while total < EXPECTED_COUNT:
        if time.time() >= end:
            break

        print("{} total so far: {}".format(time.time(), total))
        sys.stdout.flush()

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            elif msg.error():
                raise KafkaException(msg.error())
            continue

        with open(f"{OUT_FILE}.{msg.partition()}", "a") as fp:
            fp.write(msg.value().decode('utf-8'))
            fp.write('\n')
            fp.flush()

        consumer.commit(asynchronous=False)
        total += 1
finally:
    consumer.close()

# NOTE: 'Records read' is used by the tests framework to know when all expected data has been read.
print("Records read {}".format(total))
sys.stdout.flush()
