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
	"fmt"
	"os"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/transporter"
	"github.com/Shopify/sarama"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "kafka")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

func New(
	shutdownHandler shutdown.ShutdownHandler,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	workers int,
	inputChans []<-chan transport.Batch,

	transportConfig map[string]interface{}) []*transport.Transporter {

	bootstrapHost := transportConfig[ConfVarBootstrapHost]
	kafkaHost, ok := bootstrapHost.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarBootstrapHost, "string")
	}

	bootstrapPort := transportConfig[ConfVarBootstrapPort]
	kafkaPort, ok := bootstrapPort.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarBootstrapPort, "string")
	}

	kafkaTopic := transportConfig[ConfVarKafkaTopic]
	topic, ok := kafkaTopic.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaTopic, "string")
	}

	kafkaTls := transportConfig[ConfVarKafkaTls]
	tls, ok := kafkaTls.(bool)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaTls, "bool")
	}

	clusterCA := transportConfig[ConfVarKafkaClusterCA]
	ca, ok := clusterCA.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaClusterCA, "string")
	}

	clientPrivateKey := transportConfig[ConfVarKafkaPrivateKey]
	privateKey, ok := clientPrivateKey.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaPrivateKey, "string")
	}

	clientPublicKey := transportConfig[ConfVarKafkaPublicKey]
	publicKey, ok := clientPublicKey.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaPublicKey, "string")
	}

	kafkaFlushBytesVar := transportConfig[ConfVarKafkaFlushBytes]
	kafkaFlushBytes, ok := kafkaFlushBytesVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaFlushBytes, "int")
	}

	kafkaFlushFrequencyVar := transportConfig[ConfVarKafkaFlushFrequency]
	kafkaFlushFrequency, ok := kafkaFlushFrequencyVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaFlushFrequency, "int")
	}

	maxMessageBytesVar := transportConfig[ConfVarKafkaMaxMessageBytes]
	maxMessageBytes, ok := maxMessageBytesVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaMaxMessageBytes, "int")
	}

	kafkaRetryMaxVar := transportConfig[ConfVarKafkaRetryMax]
	kafkaRetryMax, ok := kafkaRetryMaxVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaRetryMax, "int")
	}

	config, _ := producerConfig(tls, ca, privateKey, publicKey, kafkaFlushBytes, kafkaFlushFrequency, maxMessageBytes, kafkaRetryMax)
	bootstrapServer := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	syncProducer, _ := sarama.NewSyncProducer([]string{bootstrapServer}, config)

	verifyProducerVar := transportConfig[ConfVarKafkaVerifyProducer]
	verifyProducer, ok := verifyProducerVar.(bool)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaVerifyProducer, "bool")
	}

	if verifyProducer {
		if err := verifySend(&syncProducer, topic); err != nil {
			panic(err)
		}
	}

	transports := make([]*transport.Transporter, workers)

	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(
			shutdownHandler,
			inputChans[i],
			statsChan,
			txnsWritten,
			*log,
			syncProducer,
			topic)
		transports[i] = &t
	}

	return transports
}

type KafkaBatchFactory struct {
	topic           string
	maxMessageBytes int
	batchSize       int
}

func verifySend(producer *sarama.SyncProducer, topic string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Placeholder message to verify broker communication"),
	}
	if _, _, err := (*producer).SendMessage(msg); err != nil {
		return err
	}
	return nil
}

func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	kafkaTopic := transportConfig[ConfVarKafkaTopic]
	topic, ok := kafkaTopic.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaTopic, "string")
	}

	maxMessageBytesVar := transportConfig[ConfVarKafkaMaxMessageBytes]
	maxMessageBytes, ok := maxMessageBytesVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaMaxMessageBytes, "int")
	}

	batchSizeVar := transportConfig[ConfVarKafkaBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaBatchSize, "int")
	}

	return KafkaBatchFactory{topic, maxMessageBytes, batchSize}
}

func (f KafkaBatchFactory) NewBatch(partitionKey string) transport.Batch {
	return batch.NewKafkaBatch(f.topic, partitionKey, f.batchSize, f.maxMessageBytes)
}
