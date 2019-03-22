# pg-bifrost

[![CircleCI](https://circleci.com/gh/Nextdoor/pg-bifrost/tree/master.svg?style=svg)](https://circleci.com/gh/Nextdoor/pg-bifrost/tree/master)

pg-bifrost is a [logical decoding](https://www.postgresql.org/docs/9.6/logicaldecoding.html) tool for PostgresSQL that writes the database's stream of events (creates, inserts, and deletes) to [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/) or [RabbitMQ](https://www.rabbitmq.com/). It is written in a modular manner that allows adding additional sinks such as S3, DynamoDB, and non-AWS destinations as well.

## Installation

A precompiled binary for Linux AMD 64 of the latest version of pg-kinesis can be obtained under the [Releases](https://github.com/Nextdoor/pg-bifrost/releases) tab.

## Example

**Startup:**

```
[Slava pg-bifrost.git]$ export AWS_SECRET_ACCESS_KEY=secretaccesskey
[Slava pg-bifrost.git]$ export AWS_ACCESS_KEY_ID=accesskeyid
[Slava pg-bifrost.git]$ export AWS_REGION=us-east-1
[Slava pg-bifrost.git]$ pg-bifrost --dbname mydb --host 127.0.0.1 replicate --create-slot kinesis --kinesis-stream dbstream
```

**Input:**

```
CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO customers (first_name, last_name) VALUES ('Goodbye', 'World');
UPDATE customers SET last_name = 'Friends' where first_name = 'Hello';
DELETE FROM customers WHERE first_name = 'Goodbye';
```

**Output:**

```
{"time":"1970-01-01T00:00:01Z","lsn":"0/1510A58","table":"public.customers","operation":"INSERT","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Hello"}},"id":{"new":{"q":"false","t":"integer","v":"1"}},"last_name":{"new":{"q":"true","t":"text","v":"World"}}}}
{"time":"1970-01-01T00:00:01Z","lsn":"0/1510B60","table":"public.customers","operation":"INSERT","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Goodbye"}},"id":{"new":{"q":"false","t":"integer","v":"2"}},"last_name":{"new":{"q":"true","t":"text","v":"World"}}}}
{"time":"1970-01-01T00:00:01Z","lsn":"0/1510C20","table":"public.customers","operation":"UPDATE","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Hello"}},"id":{"new":{"q":"false","t":"integer","v":"1"}},"last_name":{"new":{"q":"true","t":"text","v":"Friends"}}}}
{"time":"1970-01-01T00:00:01Z","lsn":"0/1510CA8","table":"public.customers","operation":"DELETE","columns":{"id":{"old":{"q":"false","t":"integer","v":"2"}}}}
```

## Usage

### main
```
USAGE:
   pg-bifrost [global options] command [command options] [arguments...]

COMMANDS:
     create, c     create a replication slot
     drop, d       drop a replication slot
     replicate, r  start logical replication
     help, h       Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config value    bifrost YAML config file (default: "config.yaml")
   --slot value      postgres replication slot (default: "pg_bifrost") [$REPLICATION_SLOT]
   --user value      postgres replication user (default: "replication") [$PGUSER]
   --password value  postgres replication user password [$PGPASSWORD]
   --host value      postgres connection host (default: "127.0.0.1") [$PGHOST]
   --port value      postgres connection port (default: "5432") [$PGPORT]
   --dbname value    postgres database name (default: "postgres") [$PGDATABASE]
   --help, -h        show help
   --version, -v     print the version
```

### replicate 
```
NAME:
   pg-bifrost replicate - start logical replication

USAGE:
   pg-bifrost replicate command [command options] [arguments...]

COMMANDS:
     stdout    replicate to stdout
     kinesis   replicate to kinesis
     rabbitmq  replicate to rabbitmq

OPTIONS:
   --create-slot, -s                  Creates replication slot if it doesn't exist before replicating [$CREATE_SLOT]
   --workers value                    number of workers for transport (default: 1) [$WORKERS]
   --client-buffer-size value         number of messages to buffer from postgres (default: 10000) [$CLIENT_BUFFER_SIZE]
   --batch-flush-update-age value     amount of time to wait in milliseconds for a new message before batch is written. If a new message is added then the timer is reset. This is evaluated every second. (default: 500) [$BATCH_FLUSH_UPDATE_AGE]
   --batch-flush-max-age value        maximum amount of time in milliseconds to wait before writing a batch. This overrides 'batch-flush-update-age'. This is evaluated every second. (default: 1000) [$BATCH_FLUSH_MAX_AGE]
   --batch-queue-depth value          number of batches that can be queued per worker (default: 2) [$BATCH_QUEUE_DEPTH]
   --batcher-memory-soft-limit value  maximum amount of memory to use when batching messages. Note this is only evaluated every 'batch-flush-timeout'. Also, note that if you use the 'partition' batcher-routing-method and have a high cardinality partition key you may need to tweak this value to create batches of meaningful size. (default: 104857600) [$BATCHER_MEMORY_SOFT_LIMIT]
   --batcher-routing-method value     determines how to route batches to workers. Options are 'round-robin' (default) and 'partition'. If you require strict ordering of data then use 'partition'. (default: "round-robin") [$BATCHER_ROUTING_METHOD]
   --partition-method value           determines how messages will be split into batches. Options are 'none' (default) , 'tablename', 'transaction', and 'transaction-bucket'. 'transaction' will ensure that a batch will only ever have messages from a single transaction. This can be dangerous if your database does lots of small transactions. 'transaction-bucket' performs a hash to pick the partition. This will mean that entire transactions will go into the same partition but that partition may have other transactions as well. 'tablename' partitions on the table name of the message. (default: "none") [$PARTITION_METHOD]
   --batcher-partition-count value    number of buckets to use when bucketing partitions in partition-method='transaction-bucket'. (default: 1) [$PARTITION_COUNT]
   --whitelist value                  A whitelist of tables to include. All others will be excluded. [$WHITELIST]
   --blacklist value                  A blacklist of tables to exclude. All others will be included. [$BLACKLIST]
   --help, -h                         show help
```

## Configuration

pg-bifrost can be configured using cli arguments, environment variables, and a config file. See `--help` for a detailed list of options.

### Memory and Throughput

Config Var | Description
-- | --
workers | Number of workers that push write data to kinesis.
client-buffer-size | Number of messages to buffer from postgres
batch-flush-timeout | Number of milliseconds to wait for new messages before writing a batch
batch-queue-depth | Number of batches that can be queued per worker
max-memory-bytes | Maximum amount of memory to use when batching messages. Note that if you use the 'partition' batcher-routing-method and have a high cardinality partition key you may need to tweak this value to create batches of meaningful size.


### Filtering

pg-bifrost currently supports table level filtering in both a whitelist and blacklist mode. Use `whitelist` to include specific tables and `blacklist` to exclude tables. Note these are mutually exclusive.


### Partitioning

One of the most important and complicated parts of the configuration is partitioning. Here is a cheat sheet for different types of partitioning strategies and how to achieve them.

**Config Options:**

Config Var | Description
-- | --
partition-method | determines how messages will be split into batches. Options are `none` (default) , `none`, `transaction`, and `transaction-bucket`. `transaction` will ensure that a batch will only ever have messages from a single transaction. This can be dangerous if your database does lots of small transactions. `transaction-bucket` performs a hash to pick the partition. This will mean that entire transactions will go into the same partition but that partition may have other transactions as well. `tablename` partitions on the table name of the message. (default: `none`)
batcher-routing-method | determines how to route batches to workers. Options are `round-robin` (default) and `partition`. If you require strict ordering of data then use `partition`. (default: `round-robin`)

**Cheat Sheet:**

Strategy | partition-method | batcher-routing-method
-- | -- | --
(default) Messages distributed across shards (unordered) | none | round-robin
Messages from the same transaction go to the same shard (unordered) | transaction-bucket | round-robin
Messages from the same transaction go to the same shard (ordered) | transaction-bucket | partition
Messages from the same table go to the same shard (unordered) | tablename | round-robin
Messages from the same table go to the same shard (ordered) | tablename | partition


## Development

### Bugs
Please report non-security issues on the GitHub tracker.

### Contributing

Help us improve this project by:

- Reporting bugs
- Improving documentation
- Adding new features
- Increasing test coverage

### Running integration tests
The integration tests require [docker](https://docs.docker.com/get-started/) and [docker-compose](https://docs.docker.com/compose/install/).

The integration tests are setup and run with:

```
# Checkout bats submodule
git submodule sync
git submodule update --init

# Build binary
make build

# Create docker images
make docker

# Run the integration tests
make itests
```

Example:

```
[Slava pg-bifrost.git] $ make build
Running go generate ...
Creating GO binary
mkdir -p target
GOOS=linux GOARCH=amd64 go build -o target/pg-bifrost github.com/Nextdoor/pg-bifrost.git/main
ln -f target/pg-bifrost itests/containers/pg-bifrost/app/pg-bifrost

[Slava pg-bifrost.git] $ make docker
Building docker integration test images
TEST_NAME=test_basic docker-compose -f itests/docker-compose.yml build
Building bifrost
Step 1/3 : FROM alpine:3.8
...

[Slava pg-bifrost.git] $ make itests
Running integration tests
cd ./itests && ./integration_tests.bats -r tests
 ✓ test_basic
 ✓ test_high_kinesis_errors
 ✓ test_large_sleep
 ✓ test_large_txn
 ✓ test_multi_worker_high_kinesis_errors
 ✓ test_multi_worker_no_kinesis_errors
 ✓ test_small_queue
 ✓ test_staggered

8 tests, 0 failures

```


## Acknowledgements

This tool was inspired by [nickelser/pg_kinesis](https://github.com/nickelser/pg_kinesis).


## License

`pg-bifrost` is distributed under the terms of the Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
