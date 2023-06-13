package kafka

import (
	"flag"
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

	bootstrapHost = flag.String("kafka_bootstrap_host", "ndgroups-kafka-bootstrap", "Kafka bootstrap server host.")
	bootstrapPort = flag.String("kafka_bootstrap_port", "9092", "Kafka bootstrap server port.")
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

	// TODO: create ths as a flag
	//kafkaTopic := transportConfig["kafka-topic"]

	transports := make([]*transport.Transporter, 1)
	retryPolicy := &backoff.ExponentialBackOff{
		InitialInterval:     1500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.2,
		MaxInterval:         5 * time.Second,
		MaxElapsedTime:      time.Duration(time.Minute * 5),
		Clock:               backoff.SystemClock,
	}

	config, _ := producerConfig()
	bootstrapServer := fmt.Sprintf("%s:%s", *bootstrapHost, *bootstrapPort)
	asyncProducer, _ := sarama.NewAsyncProducer([]string{bootstrapServer}, config)
	log.Info("Creating producer")

	msg := &sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.ByteEncoder("Placeholder message to verify broker communication"),
	}
	log.Info("Sending to input")
	asyncProducer.Input() <- msg
	log.Info("Sent to input")

	select {
	case <-asyncProducer.Successes():
		log.Info("Successfully validated producer")
	case err := <-asyncProducer.Errors():
		log.Info("Error sending %v", err)
	case <-time.After(15 * time.Second):
		log.Info("timed out waiting for message receipt")
	}

	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(
			shutdownHandler,
			inputChans[i],
			statsChan,
			txnsWritten,
			*log,
			retryPolicy,
			asyncProducer)

		transports[i] = &t
	}

	return transports
}

// NewBatchFactory returns a GenericBatchFactory configured for Kafka
func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	//TODO: set this as a flag
	//batchSizeVar := transportConfig["kafka-batch-size"]
	var batchSizeVar interface{} = 10
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarPutBatchSize, "int")
	}

	return batch.NewGenericBatchFactory(batchSize)
}
