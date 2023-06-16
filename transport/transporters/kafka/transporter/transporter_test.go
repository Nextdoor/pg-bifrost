package transporter

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/mocks"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	utils_mocks "github.com/Nextdoor/pg-bifrost.git/utils/mocks"
	"github.com/Shopify/sarama"
	"github.com/cevaris/ordered_map"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "kafka")
)

func resetTimeSource() {
	TimeSource = utils.RealTime{}
}

func TestSendOk(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	mockProducer := mockProducer(t)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	topic := "test"
	batchSize := 1
	maxMessageBytes := 1000000

	tp := NewTransporter(sh, in, statsChan, txns, *log, mockProducer, topic)
	b := batch.NewKafkaBatch(topic, "", batchSize, maxMessageBytes)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	mockProducer.ExpectSendMessageAndSucceed()

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))

	in <- b

	// Start test
	go tp.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kafka_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("kafka_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("kafka_transport", "written", int64(1), int64(4000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)

	time.Sleep(50 * time.Millisecond)

}

func TestInputClosed(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	mockProducer := mockProducer(t)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	topic := "test"

	tp := NewTransporter(sh, in, statsChan, txns, *log, mockProducer, topic)

	go tp.StartTransporting()

	// Wait for transporter to start
	time.Sleep(time.Millisecond * 5)

	// Close input
	close(in)
	time.Sleep(time.Millisecond * 5)

	// Verify shutdown
	_, ok := <-txns
	fmt.Println("checking if channel is closed")
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}

	_, ok = <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestTerminationContext(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	mockProducer := mockProducer(t)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	topic := "test"

	tp := NewTransporter(sh, in, statsChan, txns, *log, mockProducer, topic)

	go tp.StartTransporting()

	// Wait for transporter to start
	time.Sleep(time.Millisecond * 5)

	// Cancel
	sh.CancelFunc()
	time.Sleep(time.Millisecond * 5)

	// Verify shutdown
	_, ok := <-txns
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestPanicHandling(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	mockProducer := mockProducer(t)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	topic := "test"
	batchSize := 1
	maxMessageBytes := 1000000

	tp := NewTransporter(sh, in, statsChan, txns, *log, mockProducer, topic)
	b := batch.NewKafkaBatch(topic, "", batchSize, maxMessageBytes)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}
	_, _ = b.Add(&marshalledMessage)

	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(testMessageCheckerWithPanic())

	mockTime.EXPECT().UnixNano().Return(int64(0))

	// Start Test
	in <- b

	go tp.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify shutdown
	_, ok := <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestFailedSend(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	mockProducer := mockProducer(t)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	topic := "test"
	batchSize := 1
	maxMessageBytes := 1000000

	tp := NewTransporter(sh, in, statsChan, txns, *log, mockProducer, topic)
	b := batch.NewKafkaBatch(topic, "", batchSize, maxMessageBytes)

	marshalledMessageOne := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("test-1"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	marshalledMessageTwo := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("test-1"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessageOne)

	_, _ = b.Add(&marshalledMessageTwo)

	firstErr := &sarama.ProducerError{
		&sarama.ProducerMessage{
			Value: sarama.StringEncoder("test-1"),
		},
		errors.New("failed send"),
	}
	secondErr := &sarama.ProducerError{
		&sarama.ProducerMessage{
			Value: sarama.StringEncoder("test-2"),
		},
		errors.New("failed send"),
	}

	var produceError sarama.ProducerErrors = []*sarama.ProducerError{firstErr, secondErr}

	mockProducer.ExpectSendMessageAndFail(produceError)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))

	in <- b

	// Start test
	go tp.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 1000)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kafka_transport", "failure", int64(2), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("kafka_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
	}
	stats.VerifyStats(t, statsChan, expected)

	time.Sleep(50 * time.Millisecond)
}

func testMessageCheckerWithPanic() mocks.MessageChecker {
	return func(msg *sarama.ProducerMessage) error {
		panic("handle panic")
	}
}

func mockProducer(t *testing.T) *mocks.SyncProducer {
	//maxMessageBytes := 1000000
	//config, err := kafka.ProducerConfig(false, "empty", "empty", "empty", maxMessageBytes)
	//assert.Nil(t, err)
	config := mocks.NewTestConfig()
	producer := mocks.NewSyncProducer(t, config)
	return producer
}
