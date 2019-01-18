# High Level Design

## Components

* Replication Client
  * Connects to postgres and pulls messages
  * Sends `SendStandbyStatus` (progress) back to postgres

* Filter
  * Filters out messages from specific tables

* PK Lookup (optional)
  * Looks up PKs from postgres
  * Adds PKs to messages

* JSON Marshaller
  * Marshalles postgres messages to JSON

* Transport Manager
  * Creates transporter threads
  * Shards messages across transporters as it receives them

* Transporter
  * Writes messages to Kinesis and sends maxWal to replication client

* Stats Aggregator
  * Receives stats messages from components, aggregates them pushes to the Stats Reporter.

* Stats Reporter
  * Formats and writes stats out. Initially just stdout.

## Diagram

```
+----------------------------------------------------------------+
|                                                                |
|                                +--------------------+          |        +-------------------+
|                                | Replication Client <-------------------+    Postgres DB    |
|              +-----------------+                    <--------+ |        +---------+---------+
|              |                 +---------+----------+        | |                  |
|              |                           |                   | |                  |
|              | statsMessage              | <WalMessage       | |                  |
|              |                           |                   | |                  |
|              |                 +---------v----------+        | |                  |
|              +-----------------+       Filter       |        | |                  |
|              |                 +---------+----------+        | |                  |
|              |                           |                   | |                  |
|              |                           | <WalMessage       | |                  |
|              |                           |                   | |                  |
|              |                 +---------v----------+        | |                  |
|              +-------------+---+     PK Lookup      <-----------------------------+
|              |             |   +---------+----------+        | |
|    +---------v--------+    |             |                   | |
|    | Stats Aggregator |    |             | <WalMessage       | |
|    +---------+--------+    |             |                   | |
|              |             |   +---------v----------+        | |
|              |             +---+  JSON Marshaller   |        | |
|    +---------v--------+    |   +---------+----------+        | |
|    |  Stats Reporter  |    |             |                   | |
|    +---------+--------+    |             | <MarshalledMessage| |
|              |             |             |                   | |
|              |             |   +---------+----------+        | |
|              |             +---+ Transport Manager  |        | |
|              |             |   +---------+----------+        | |
|              |             |             |                   | |
|              |             |             | <MarshalledMessage| |
|              |             |             |                   | |
|              |             |   +---------v----------+        | |
|              |             +---+     Transport      +--------+ |
|              |                 +---------+----------+          |
|              |                           |                     |
+----------------------------------------------------------------+
               |                           |
      +--------v--------+        +---------v----------+
      |     Stdout      |        |      Kinesis       |
      +-----------------+        +--------------------+

```

