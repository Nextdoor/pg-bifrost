package kafka

import (
	"fmt"
	"os"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/transporter"
	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "kafka")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.DebugLevel)
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

	kafkaTopic := transportConfig[ConfVarTopic]
	topic, ok := kafkaTopic.(string)
	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarTopic, "string")
	}
	config, _ := producerConfig()
	bootstrapServer := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	syncProducer, _ := sarama.NewSyncProducer([]string{bootstrapServer}, config)
	if err := verifySend(&syncProducer, topic); err != nil {
		panic(err)
	}

	retryPolicy := &backoff.ExponentialBackOff{
		InitialInterval:     1500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.2,
		MaxInterval:         5 * time.Second,
		MaxElapsedTime:      time.Duration(time.Minute * 5),
		Clock:               backoff.SystemClock,
	}

	transports := make([]*transport.Transporter, 1)

	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(
			shutdownHandler,
			inputChans[i],
			statsChan,
			txnsWritten,
			*log,
			retryPolicy,
			syncProducer,
			topic)
		transports[i] = &t
	}

	return transports
}

// NewBatchFactory returns a GenericBatchFactory configured for Kafka
func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	batchSizeVar := transportConfig[ConfVarKafkaBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaBatchSize, "int")
	}

	return batch.NewGenericBatchFactory(batchSize)
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
