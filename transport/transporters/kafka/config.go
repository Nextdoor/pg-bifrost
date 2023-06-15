package kafka

import (
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarKafkaBatchSize = "kafka-batch-size"
	ConfVarTopic          = "kafka-topic"
	ConfVarBootstrapHost  = "kafka-bootstrap-host"
	ConfVarBootstrapPort  = "kafka-bootstrap-port"
)

var Flags = []cli.Flag{
	altsrc.NewIntFlag(cli.IntFlag{
		Name:   ConfVarKafkaBatchSize,
		Usage:  "Kafka Number of messages to write before waiting for confirmations",
		EnvVar: "KAFKA_BATCH_SIZE",
		Value:  5000,
	}),
	cli.StringFlag{
		Name:   ConfVarTopic,
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
}
