/*
The following code is copied and modified from Shopify's sarama repository.
Shopify's copyright MIT License is provided below:

Copyright (c) 2013 Shopify

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

package mocks

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	errProduceSuccess    error = nil
	errOutOfExpectations       = errors.New("No more expectations set on mock")
)

// SyncProducer implements sarama's SyncProducer interface for testing purposes.
// Before you can use it, you have to set expectations on the mock SyncProducer
// to tell it how to handle calls to SendMessage, so you can easily test success
// and failure scenarios.
type SyncProducer struct {
	l            sync.Mutex
	t            ErrorReporter
	expectations []*producerExpectation
	lastOffset   int64

	*TopicConfig
	newPartitioner sarama.PartitionerConstructor
	partitioners   map[string]sarama.Partitioner

	isTransactional bool
	txnLock         sync.Mutex
	txnStatus       sarama.ProducerTxnStatusFlag
}

// NewSyncProducer instantiates a new SyncProducer mock. The t argument should
// be the *testing.T instance of your test method. An error will be written to it if
// an expectation is violated. The config argument is validated and used to handle
// partitioning.
func NewSyncProducer(t ErrorReporter, config *sarama.Config) *SyncProducer {
	if config == nil {
		config = sarama.NewConfig()
	}
	if err := config.Validate(); err != nil {
		t.Errorf("Invalid mock configuration provided: %s", err.Error())
	}
	return &SyncProducer{
		t:               t,
		expectations:    make([]*producerExpectation, 0),
		TopicConfig:     NewTopicConfig(),
		newPartitioner:  config.Producer.Partitioner,
		partitioners:    make(map[string]sarama.Partitioner, 1),
		isTransactional: config.Producer.Transaction.ID != "",
		txnStatus:       sarama.ProducerTxnFlagReady,
	}
}

////////////////////////////////////////////////
// Implement SyncProducer interface
////////////////////////////////////////////////

// SendMessage corresponds with the SendMessage method of sarama's SyncProducer implementation.
// You have to set expectations on the mock producer before calling SendMessage, so it knows
// how to handle them. You can set a function in each expectation so that the message value
// checked by this function and an error is returned if the match fails.
// If there is no more remaining expectation when SendMessage is called,
// the mock producer will write an error to the test state object.
func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	sp.l.Lock()
	defer sp.l.Unlock()

	if sp.IsTransactional() && sp.txnStatus&sarama.ProducerTxnFlagInTransaction == 0 {
		sp.t.Errorf("attempt to send message when transaction is not started or is in ending state.")
		return -1, -1, errors.New("attempt to send message when transaction is not started or is in ending state")
	}

	if len(sp.expectations) > 0 {
		expectation := sp.expectations[0]
		sp.expectations = sp.expectations[1:]
		topic := msg.Topic
		partition, err := sp.partitioner(topic).Partition(msg, sp.partitions(topic))
		if err != nil {
			sp.t.Errorf("Partitioner returned an error: %s", err.Error())
			return -1, -1, err
		}
		msg.Partition = partition
		if expectation.CheckFunction != nil {
			errCheck := expectation.CheckFunction(msg)
			if errCheck != nil {
				sp.t.Errorf("Check function returned an error: %s", errCheck.Error())
				return -1, -1, errCheck
			}
		}
		if errors.Is(expectation.Result, errProduceSuccess) {
			sp.lastOffset++
			msg.Offset = sp.lastOffset
			return 0, msg.Offset, nil
		}
		return -1, -1, expectation.Result
	}
	sp.t.Errorf("No more expectation set on this mock producer to handle the input message.")
	return -1, -1, errOutOfExpectations
}

// SendMessages corresponds with the SendMessages method of sarama's SyncProducer implementation.
// You have to set expectations on the mock producer before calling SendMessages, so it knows
// how to handle them. If there is no more remaining expectations when SendMessages is called,
// the mock producer will write an error to the test state object.
func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) >= len(msgs) {
		expectations := sp.expectations[0:len(msgs)]
		sp.expectations = sp.expectations[len(msgs):]
		var produceErrors sarama.ProducerErrors
		for i, expectation := range expectations {
			topic := msgs[i].Topic
			errFlag := false
			partition, err := sp.partitioner(topic).Partition(msgs[i], sp.partitions(topic))
			if err != nil {
				sp.t.Errorf("Partitioner returned an error: %s", err.Error())
				produceErrors = append(produceErrors, &sarama.ProducerError{Msg: msgs[i], Err: err})
				errFlag = true
			}
			msgs[i].Partition = partition
			if expectation.CheckFunction != nil {
				errCheck := expectation.CheckFunction(msgs[i])
				if errCheck != nil {
					sp.t.Errorf("Check function returned an error: %s", errCheck.Error())
					produceErrors = append(produceErrors, &sarama.ProducerError{Msg: msgs[i], Err: errCheck})
					errFlag = true
				}
			}
			if !errors.Is(expectation.Result, errProduceSuccess) {
				produceErrors = append(produceErrors, &sarama.ProducerError{Msg: msgs[i], Err: expectation.Result})
				errFlag = true
			}
			if errFlag {
				sp.lastOffset++
				msgs[i].Offset = sp.lastOffset
			}
		}
		if len(produceErrors) > 0 {
			return produceErrors
		}
		return nil
	}
	sp.t.Errorf("Insufficient expectations set on this mock producer to handle the input messages.")
	return errOutOfExpectations
}

func (sp *SyncProducer) partitioner(topic string) sarama.Partitioner {
	partitioner := sp.partitioners[topic]
	if partitioner == nil {
		partitioner = sp.newPartitioner(topic)
		sp.partitioners[topic] = partitioner
	}
	return partitioner
}

// Close corresponds with the Close method of sarama's SyncProducer implementation.
// By closing a mock syncproducer, you also tell it that no more SendMessage calls will follow,
// so it will write an error to the test state if there's any remaining expectations.
func (sp *SyncProducer) Close() error {
	sp.l.Lock()
	defer sp.l.Unlock()

	if len(sp.expectations) > 0 {
		sp.t.Errorf("Expected to exhaust all expectations, but %d are left.", len(sp.expectations))
	}

	return nil
}

////////////////////////////////////////////////
// Setting expectations
////////////////////////////////////////////////

// ExpectSendMessageWithMessageCheckerFunctionAndSucceed sets an expectation on the mock producer
// that SendMessage will be called. The mock producer will first call the given function to check
// the message. It will cascade the error of the function, if any, or handle the message as if it
// produced successfully, i.e. by returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageWithMessageCheckerFunctionAndSucceed(cf MessageChecker) *SyncProducer {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: errProduceSuccess, CheckFunction: cf})

	return sp
}

// ExpectSendMessageWithMessageCheckerFunctionAndFail sets an expectation on the mock producer that
// SendMessage will be called. The mock producer will first call the given function to check the
// message. It will cascade the error of the function, if any, or handle the message as if it
// failed to produce successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageWithMessageCheckerFunctionAndFail(cf MessageChecker, err error) *SyncProducer {
	sp.l.Lock()
	defer sp.l.Unlock()
	sp.expectations = append(sp.expectations, &producerExpectation{Result: err, CheckFunction: cf})

	return sp
}

// ExpectSendMessageWithCheckerFunctionAndSucceed sets an expectation on the mock producer that SendMessage
// will be called. The mock producer will first call the given function to check the message value.
// It will cascade the error of the function, if any, or handle the message as if it produced
// successfully, i.e. by returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageWithCheckerFunctionAndSucceed(cf ValueChecker) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(messageValueChecker(cf))

	return sp
}

// ExpectSendMessageWithCheckerFunctionAndFail sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will first call the given function to check the message value.
// It will cascade the error of the function, if any, or handle the message as if it failed
// to produce successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageWithCheckerFunctionAndFail(cf ValueChecker, err error) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndFail(messageValueChecker(cf), err)

	return sp
}

// ExpectSendMessageAndSucceed sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it produced successfully, i.e. by
// returning a valid partition, and offset, and a nil error.
func (sp *SyncProducer) ExpectSendMessageAndSucceed() *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(nil)

	return sp
}

// ExpectSendMessageAndFail sets an expectation on the mock producer that SendMessage will be
// called. The mock producer will handle the message as if it failed to produce
// successfully, i.e. by returning the provided error.
func (sp *SyncProducer) ExpectSendMessageAndFail(err error) *SyncProducer {
	sp.ExpectSendMessageWithMessageCheckerFunctionAndFail(nil, err)

	return sp
}

func (sp *SyncProducer) IsTransactional() bool {
	return sp.isTransactional
}

func (sp *SyncProducer) BeginTxn() error {
	sp.txnLock.Lock()
	defer sp.txnLock.Unlock()

	sp.txnStatus = sarama.ProducerTxnFlagInTransaction
	return nil
}

func (sp *SyncProducer) CommitTxn() error {
	sp.txnLock.Lock()
	defer sp.txnLock.Unlock()

	sp.txnStatus = sarama.ProducerTxnFlagReady
	return nil
}

func (sp *SyncProducer) AbortTxn() error {
	sp.txnLock.Lock()
	defer sp.txnLock.Unlock()

	sp.txnStatus = sarama.ProducerTxnFlagReady
	return nil
}

func (sp *SyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sp.txnStatus
}

func (sp *SyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (sp *SyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

////////////////////////////////////////////////
// Provides mocks that can be used for testing
// applications that use Sarama
////////////////////////////////////////////////

// ErrorReporter is a simple interface that includes the testing.T methods we use to report
// expectation violations when using the mock objects.
type ErrorReporter interface {
	Errorf(string, ...interface{})
}

// ValueChecker is a function type to be set in each expectation of the producer mocks
// to check the value passed.
type ValueChecker func(val []byte) error

// MessageChecker is a function type to be set in each expectation of the producer mocks
// to check the message passed.
type MessageChecker func(*sarama.ProducerMessage) error

// messageValueChecker wraps a ValueChecker into a MessageChecker.
// Failure to encode the message value will return an error and not call
// the wrapped ValueChecker.
func messageValueChecker(f ValueChecker) MessageChecker {
	if f == nil {
		return nil
	}
	return func(msg *sarama.ProducerMessage) error {
		val, err := msg.Value.Encode()
		if err != nil {
			return fmt.Errorf("Input message encoding failed: %w", err)
		}
		return f(val)
	}
}

const AnyOffset int64 = -1000

type producerExpectation struct {
	Result        error
	CheckFunction MessageChecker
}

// TopicConfig describes a mock topic structure for the mock producers’ partitioning needs.
type TopicConfig struct {
	overridePartitions map[string]int32
	defaultPartitions  int32
}

// NewTopicConfig makes a configuration which defaults to 32 partitions for every topic.
func NewTopicConfig() *TopicConfig {
	return &TopicConfig{
		overridePartitions: make(map[string]int32, 0),
		defaultPartitions:  32,
	}
}

// SetDefaultPartitions sets the number of partitions any topic not explicitly configured otherwise
// (by SetPartitions) will have from the perspective of created partitioners.
func (pc *TopicConfig) SetDefaultPartitions(n int32) {
	pc.defaultPartitions = n
}

// SetPartitions sets the number of partitions the partitioners will see for specific topics. This
// only applies to messages produced after setting them.
func (pc *TopicConfig) SetPartitions(partitions map[string]int32) {
	for p, n := range partitions {
		pc.overridePartitions[p] = n
	}
}

func (pc *TopicConfig) partitions(topic string) int32 {
	if n, found := pc.overridePartitions[topic]; found {
		return n
	}
	return pc.defaultPartitions
}

// NewTestConfig returns a config meant to be used by tests.
// Due to inconsistencies with the request versions the clients send using the default Kafka version
// and the response versions our mocks use, we default to the minimum Kafka version in most tests
func NewTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	return config
}
