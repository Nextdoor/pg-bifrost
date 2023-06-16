package kafka

import (
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarKafkaBatchSize       = "kafka-batch-size"
	ConfVarKafkaMaxMessageBytes = "kafka-max-message-bytes"
	ConfVarKafkaTopic           = "kafka-topic"
	ConfVarBootstrapHost        = "kafka-bootstrap-host"
	ConfVarBootstrapPort        = "kafka-bootstrap-port"
	ConfVarKafkaTls             = "kafka-tls"
	ConfVarKafakClusterCA       = "kafka-cluster-ca"
	ConfVarKafakPrivateKey      = "kafka-private-key"
	ConfVarKafakPublicKey       = "kafka-public-key"
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
		Name:   ConfVarKafkaMaxMessageBytes,
		Usage:  "Max byte size of a message",
		EnvVar: "KAFKA_MAX_MESSAGE_SIZE",
		Value:  1000000,
	}),
	cli.StringFlag{
		Name:   ConfVarKafkaTopic,
		Usage:  "Kafka topic name ",
		EnvVar: "BIFROST_KAFKA_TOPIC",
	},
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
		Name:   ConfVarKafakClusterCA,
		Usage:  "File path of kafka cluster ca",
		EnvVar: "KAFKA_CLUSTER_CA",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarKafakPrivateKey,
		Usage:  "File path of kafka client private key",
		EnvVar: "KAFKA_CLIENT_PRIVATE_KEY",
	}),

	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarKafakPublicKey,
		Usage:  "File path of kafka client public key",
		EnvVar: "KAFKA_CLIENT_PUBLIC_KEY",
	}),
	altsrc.NewBoolFlag(cli.BoolFlag{
		Name:   ConfVarKafkaVerifyProducer,
		Usage:  "Whether or not to verify producer upon startup",
		EnvVar: "KAFKA_VERIFY_PRODUCER",
	}),
}
