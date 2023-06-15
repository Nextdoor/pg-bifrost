package kafka

import (
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarKafkaBatchSize  = "kafka-batch-size"
	ConfVarKafkaTopic      = "kafka-topic"
	ConfVarBootstrapHost   = "kafka-bootstrap-host"
	ConfVarBootstrapPort   = "kafka-bootstrap-port"
	ConfVarKafkaTls        = "kafka-tls"
	ConfVarKafakClusterCA  = "kafka-cluster-ca"
	ConfVarKafakPrivateKey = "kafka-private-key"
	ConfVarKafakPublicKey  = "kafka-public-key"
)

var Flags = []cli.Flag{
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaBatchSize,
		Usage:  "Kafka Number of messages to write before waiting for confirmations",
		EnvVar: "KAFKA_BATCH_SIZE",
		Value:  5000,
	}),
	cli.StringFlag{
		Name:   ConfVarKafkaTopic,
		Usage:  "Kafka topic name ",
		EnvVar: "BIFROST_KAFKA_TOPIC",
	},
	cli.StringFlag{
		Name:   ConfVarBootstrapHost,
		Usage:  "Kafka bootstrap host",
		EnvVar: "KAFKA_BOOTSTRAP_HOST",
	},
	cli.StringFlag{
		Name:   ConfVarBootstrapPort,
		Usage:  "Kafka bootstrap port",
		EnvVar: "KAFKA_BOOTSTRAP_PORT",
	},
	cli.BoolFlag{
		Name:   ConfVarKafkaTls,
		Usage:  "Whether to use TLS when writing to kafka",
		EnvVar: "KAFKA_TLS",
	},
	cli.StringFlag{
		Name:   ConfVarKafakClusterCA,
		Usage:  "File path of kafka cluster ca",
		EnvVar: "KAFKA_CLUSTER_CA",
	},
	cli.StringFlag{
		Name:   ConfVarKafakPrivateKey,
		Usage:  "File path of kafka client private key",
		EnvVar: "KAFKA_CLIENT_PRIVATE_KEY",
	},
	cli.StringFlag{
		Name:   ConfVarKafakPublicKey,
		Usage:  "File path of kafka client public key",
		EnvVar: "KAFKA_CLIENT_PUBLIC_KEY",
	},
}
