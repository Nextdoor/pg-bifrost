/*
  Copyright 2023 Nextdoor.com, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package kafka

import (
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarKafkaBatchSize       = "kafka-batch-size"
	ConfVarKafkaFlushBytes      = "kafka-flush-bytes"
	ConfVarKafkaFlushFrequency  = "kafka-flush-frequency"
	ConfVarKafkaMaxMessageBytes = "kafka-max-message-bytes"
	ConfVarKafkaRetryMax        = "kafka-flush-retry-max"
	ConfVarKafkaTopic           = "kafka-topic"
	ConfVarKafkaPartitionCount  = "kafka-partition-count"
	ConfVarBootstrapHost        = "kafka-bootstrap-host"
	ConfVarBootstrapPort        = "kafka-bootstrap-port"
	ConfVarKafkaTls             = "kafka-tls"
	ConfVarKafkaClusterCA       = "kafka-cluster-ca"
	ConfVarKafkaPrivateKey      = "kafka-private-key"
	ConfVarKafkaPublicKey       = "kafka-public-key"
	ConfVarKafkaVerifyProducer  = "kafka-verify-producer"
)

var Flags = []cli.Flag{
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaBatchSize,
		Usage:  "Maximum number of kafka messages in a batch",
		EnvVar: "KAFKA_BATCH_SIZE",
		Value:  5000,
	}),
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaFlushBytes,
		Usage:  "Number of bytes needed to trigger a flush",
		EnvVar: "KAFKA_FLUSH_BYTES",
		Value:  262144,
	}),
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaFlushFrequency,
		Usage:  "Best-effort frequency of flushes in ms",
		EnvVar: "KAFKA_FLUSH_FREQUENCY",
		Value:  2500,
	}),
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaMaxMessageBytes,
		Usage:  "Max byte size of a message",
		EnvVar: "KAFKA_MAX_MESSAGE_SIZE",
		Value:  1000000,
	}),
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaRetryMax,
		Usage:  "Max retries for a failed send",
		EnvVar: "KAFKA_RETRY_MAX",
		Value:  10,
	}),
	cli.StringFlag{
		Name:   ConfVarKafkaTopic,
		Usage:  "Kafka topic name ",
		EnvVar: "BIFROST_KAFKA_TOPIC",
	},
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaPartitionCount,
		Usage:  "Number of Kafka partitions",
		EnvVar: "KAFKA_PARTITION_COUNT",
		Value:  1,
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarBootstrapHost,
		Usage:  "Kafka bootstrap host",
		EnvVar: "KAFKA_BOOTSTRAP_HOST",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarBootstrapPort,
		Usage:  "Kafka bootstrap port",
		EnvVar: "KAFKA_BOOTSTRAP_PORT",
	}),

	altsrc.NewBoolFlag(cli.BoolFlag{
		Name:   ConfVarKafkaTls,
		Usage:  "Whether to use TLS when writing to kafka",
		EnvVar: "KAFKA_TLS",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarKafkaClusterCA,
		Usage:  "File path of kafka cluster ca",
		EnvVar: "KAFKA_CLUSTER_CA",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarKafkaPrivateKey,
		Usage:  "File path of kafka client private key",
		EnvVar: "KAFKA_CLIENT_PRIVATE_KEY",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarKafkaPublicKey,
		Usage:  "File path of kafka client public key",
		EnvVar: "KAFKA_CLIENT_PUBLIC_KEY",
	}),
	altsrc.NewBoolFlag(cli.BoolFlag{
		Name:   ConfVarKafkaVerifyProducer,
		Usage:  "Whether or not to verify producer upon startup",
		EnvVar: "KAFKA_VERIFY_PRODUCER",
	}),
}
