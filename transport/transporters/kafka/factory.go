package kafka

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
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
	asyncProducer, _ := sarama.NewAsyncProducer([]string{bootstrapServer}, config)
	if err := verifySend(&asyncProducer, topic); err != nil {
		panic(err)
	}

	go kafkaConsumer(kafkaHost, kafkaPort, topic)

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
			asyncProducer,
			topic)
		transports[i] = &t
	}

	return transports
}

// NewBatchFactory returns a GenericBatchFactory configured for Kafka
func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	//TODO: set this as a flag
	batchSizeVar := transportConfig[ConfVarKafkaBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKafkaBatchSize, "int")
	}

	return batch.NewGenericBatchFactory(batchSize)
}

func verifySend(producer *sarama.AsyncProducer, topic string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Placeholder message to verify broker communication"),
	}
	(*producer).Input() <- msg
	select {
	case <-(*producer).Successes():
		return nil
	case err := <-(*producer).Errors():
		return err
	case <-time.After(15 * time.Second):
		return errors.New("timed out verifying producer")
	}
}

func kafkaConsumer(bootstrapHost string, bootstrapPort string, topic string) {
	log.Info("starting consumer")
	// Set up configuration
	bootstrapServer := fmt.Sprintf("%s:%s", bootstrapHost, bootstrapPort)
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config, _ = configureTLS(config)
	// NewConsumer creates a new consumer using the given broker addresses and configuration.
	consumer, _ := sarama.NewConsumer([]string{bootstrapServer}, config)

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Println("Failed to close consumer:", err)
		}
	}()

	partition := int32(0)

	// Start consuming from the specified topic and partition
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln("Failed to start consumer for partition:", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println("Failed to close partition consumer:", err)
		}
	}()

	// Handle messages
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Start a goroutine to consume messages
	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("Received message: Topic = %s, Partition = %d, Offset = %d, Key = %s, Value = %s\n",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			case err := <-partitionConsumer.Errors():
				log.Println("Consumer error:", err)
			case <-signals:
				return
			}
		}
	}()

	// Wait for termination signal
	<-signals
	log.Println("Consumer terminated")
}
