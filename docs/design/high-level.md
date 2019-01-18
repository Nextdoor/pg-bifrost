# High Level Design
-

## Components

### Replication Client

Responsible for connecting to Postgres and pulling WAL logs and sending back progress reports.

### Filter
Filters out messages from specific tables.

### Marshaller
Marshalles Postgres WAL messages to JSON.

### Transporter

The Transporter handles batching and transporting messages to the final destination.

#### Manager

The Transport Manager orchestrates the other modules in the Transporter. It itself does not do any work beyond setting up and linking the other modules. The Manager creates a Multiplexer, a Progress Tracker, and N Batcher+Transport pairs.

##### Multiplexer

Sends a marshalled message to 1 of N Batcher+Transport pair. The current implementation uses hashing for equal distribution.

##### Batcher

The Batcher reads from the Multiplexer and stores messages in an internal buffer. Once the buffer is full it is sent to the Transport for delivery.

##### Transport

Sends a batch of messages downstream and reports to the Progress Tracker the youngest successfully sent message.

##### Progress Tracker

Aggregates progress from all Transport routines and sends a single progress back to the Replication Client.

### Stats Aggregator
Receives stats messages from components, aggregates them, and pushes to the Stats Reporter.

### Stats Reporter
Formats and writes stats out to stdout.


## Diagram

Simplified:

```
+-----------------------------------------------------------------------------+
|                            +--------------------+                           |   +-----------------+
|            +--------------+| Replication Client |<------------------------------+ Postgres Server |
|            |               |                    |<------------+             |   +-----------------+
|            |               +--------------------+             |             |
|            |                          |                       |             |
|            +--------------+           |                       |             |
|            |              |           |                       |             |
|            |              |           |                       |             |
|   +--------v---------+    |   +-------v--------+              |             |
|   | Stats Aggregator |    +---+     Filter     |              |             |
|   +--------+---------+    |   +-------+--------+              |             |
|            |              |           |                       |             |
|            |              |           |                       |             |
|            |              |           |                       |             |
|   +--------v---------+    |   +-------v--------+              |             |
|   |  Stats Reporter  |    +---+   Marshaller   |              |             |
|   +--------+---------+    |   +-------+--------+              |             |
|            |              |           |                       |             |
|            |              |           |                       |             |
|            |              |           |                       |             |
|            |              |   +-------v--------+     +--------+---------+   |
|            |              +---+  Transporter   +-----> Progress Tracker |   |
|            |                  +-------+--------+     +------------------+   |
|            |                          |                                     |
|            |                          |                                     |
+-----------------------------------------------------------------------------+
             |                          |
             |                          |
      +------v-------+          +-------v--------+
      |    Stdout    |          |    Kinesis     |
      +--------------+          +----------------+


```


With Transporter:
```
+---------------------------------------------------------------------------------+
|                                                                                 |
|                            +----------------------+                             |   +-----------------+
|            +---------------+  Replication Client  <---------------------------------> Postgres Server |
|            |               |                      <--------------+              |   +-----------------+
|            |               +----------+-----------+              |              |
|            |                          |                          |              |
|            +--------------+           |                          |              |
|            |              |           |                          |              |
|            |              |           |                          |              |
|   +--------v---------+    |   +-------v--------+                 |              |
|   | Stats Aggregator |    +---+     Filter     |                 |              |
|   +--------+---------+    |   +-------+--------+                 |              |
|            |              |           |                          |              |
|            |              |           |                          |              |
|            |              |           |                          |              |
|   +--------v---------+    |   +-------v--------+                 |              |
|   |  Stats Reporter  |    +---+   Marshaller   |                 |              |
|   +--------+---------+    |   +-------+--------+                 |              |
|            |              |           |                          |              |
|            |              |           |                          |              |
|            |              |           |   Transporter            |              |
|            |  +-------------------------------------------------------------+   |
|            |  |           |           |                          |          |   |
|            |  |   +-------+-----------v--+             +---------+--------+ |   |
|            |  |   |       Batcher        +-------------> Progress Tracker | |   |
|            |  |   +---+-------+----------+             +---------^--------+ |   |
|            |  |       |       |                                  |          |   |
|            |  |       |       |                                  |          |   |
|            |  |       |       |                                  |          |   |
|            |  |       |       |                                  |          |   |
|            |  |       |       |           +---------------+      |          |   |
|            |  |       |       |           |               +------+          |   |
|            |  |       |       +----------->   Transport   |      |          |   |
|            |  |       |                   |               +--------------------------+
|            |  |       |                   +---------------+      |          |   |    |
|            |  |       |                                          |          |   |    |   +----------------+
|            |  |       |                   +---------------+      |          |   |    +--->     Kinesis    |
|            |  |       |                   |               +------+          |   |    |   +----------------+
|            |  |       +------------------->   Transport   |                 |   |    |
|            |  |                           |               +--------------------------+
|            |  |                           +---------------+                 |   |
|            |  |                                                             |   |
|            |  +-------------------------------------------------------------+   |
|            |                                                                    |
+---------------------------------------------------------------------------------+
             |
             |
      +------v-----+
      |   Stdout   |
      +------------+
```