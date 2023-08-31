/*
  Copyright 2019 Nextdoor.com, Inc.

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

package marshaller

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	gojson "github.com/goccy/go-json"

	"github.com/Nextdoor/pg-bifrost.git/parselogical"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "marshaller")
)

const epochFormatted = "1970-01-01T00:00:00Z"

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

var colValuesPool = sync.Pool{
	New: func() interface{} {
		return map[string]string{}
	},
}

var usedColValues []map[string]string

func getColValue() map[string]string {
	v := colValuesPool.Get().(map[string]string)
	usedColValues = append(usedColValues, v)
	return v
}

func clearColValues() {
	for _, m := range usedColValues {
		colValuesPool.Put(m)
	}

	usedColValues = usedColValues[0:0]
}

var colValuePairPool = sync.Pool{
	New: func() interface{} {
		return map[string]map[string]string{}
	},
}

var usedColValueParis []map[string]map[string]string

func getColValuePair() map[string]map[string]string {
	v := colValuePairPool.Get().(map[string]map[string]string)
	usedColValueParis = append(usedColValueParis, v)
	return v
}

func clearColValuePairs() {
	for _, m := range usedColValueParis {
		delete(m, "old")
		delete(m, "new")
		colValuePairPool.Put(m)
	}

	usedColValueParis = usedColValueParis[0:0]
}

type Marshaller struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan  <-chan *replication.WalMessage
	OutputChan chan *MarshalledMessage

	statsChan chan stats.Stat

	noMarshalOldValue bool
}

// New is a simple constructor which create a marshaller.
func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *replication.WalMessage,
	statsChan chan stats.Stat,
	noMarshalOldValue bool) Marshaller {

	outputChan := make(chan *MarshalledMessage)

	return Marshaller{shutdownHandler, inputChan, outputChan, statsChan, noMarshalOldValue}
}

// jsonWalEntry is a helper struct which has json field tags
type jsonWalEntry struct {
	Time      string                                  `json:"time"`
	Lsn       string                                  `json:"lsn"` // Log Sequence Number that determines position in WAL
	Table     string                                  `json:"table"`
	Operation string                                  `json:"operation"`
	Columns   map[string]map[string]map[string]string `json:"columns"`
}

// shutdown idempotently closes the output channels and cancels the termination context
func (m Marshaller) shutdown() {
	log.Info("shutting down")
	m.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	log.Debug("closing output channel")
	close(m.OutputChan)
}

// Start loops off replication.WalMessages from the input channels and marshals them to Json to the output channel.
func (m Marshaller) Start() {
	log.Info("starting")
	defer m.shutdown()

	var ok bool
	var walMessage *replication.WalMessage

	for {
		select {
		case <-m.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return

		case walMessage, ok = <-m.inputChan:
			// pass
		}

		select {
		case <-m.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
			// pass
		}

		if !ok {
			log.Debug("input channel is closed")
			return
		}

		byteMessage, err := marshalWalToJson(walMessage, m.noMarshalOldValue)

		if err != nil {
			m.statsChan <- stats.NewStatCount("marshaller", "failure", 1, time.Now().UnixNano())
			log.Error("error in marshalWalToJson")
			continue
		}

		marshalledMessage := MarshalledMessage{
			walMessage.Pr.Operation,
			walMessage.Pr.Relation,
			byteMessage,
			walMessage.TimeBasedKey,
			walMessage.WalStart,
			walMessage.Pr.Transaction,
			walMessage.PartitionKey,
		}

		stat := stats.NewStatCount("marshaller", "success", 1, time.Now().UnixNano())

		select {
		case m.OutputChan <- &marshalledMessage:
			// pass
		case <-m.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		}

		m.statsChan <- stat
	}
}

// marshalColumnValue marshals the parselogical column value
func marshalColumnValue(cv *parselogical.ColumnValue) map[string]string {
	quoted := "false"
	if cv.Quoted {
		quoted = "true"
	}

	mp := getColValue()
	mp["v"] = cv.Value
	mp["t"] = cv.Type
	mp["q"] = quoted

	return mp
}

// marshalColumnValuePair marshals the column value pairs which shows the old and new columns
func marshalColumnValuePair(newValue *parselogical.ColumnValue, oldValue *parselogical.ColumnValue) map[string]map[string]string {
	if oldValue != nil && newValue != nil {
		cvp := getColValuePair()
		cvp["old"] = marshalColumnValue(oldValue)
		cvp["new"] = marshalColumnValue(newValue)
		return cvp
	} else if newValue != nil {
		cvp := getColValuePair()
		cvp["new"] = marshalColumnValue(newValue)
		return cvp
	} else if oldValue != nil {
		cvp := getColValuePair()
		cvp["old"] = marshalColumnValue(oldValue)
		return cvp
	}

	return nil
}

// Vars reused across executions of marshalWalToJson in order to avoid allocations
var colsTemp = map[string]map[string]map[string]string{}
var reusedWalEntry = &jsonWalEntry{}
var lsnBuffer bytes.Buffer
var lnsWriter = bufio.NewWriter(&lsnBuffer)

// marshalWalToJson marshals a WalMessage using parselogical to parse the columns and returns a byte slice
func marshalWalToJson(msg *replication.WalMessage, noMarshalOldValue bool) ([]byte, error) {
	// clear last used
	for k, _ := range colsTemp {
		delete(colsTemp, k)
	}
	var columns = colsTemp

	for k, v := range msg.Pr.Columns {
		oldV, ok := msg.Pr.OldColumns[k]

		if msg.Pr.Operation == "DELETE" {
			columns[k] = marshalColumnValuePair(nil, &v)
			continue
		}

		if ok && v.Value != oldV.Value {
			// When column is TOAST-ed use the previous value instead of "unchanged-toast-datum"
			if v.Value == "unchanged-toast-datum" {
				if noMarshalOldValue {
					columns[k] = marshalColumnValuePair(&oldV, nil)
				} else {
					columns[k] = marshalColumnValuePair(&oldV, &oldV)
				}
				continue
			}

			if noMarshalOldValue {
				columns[k] = marshalColumnValuePair(&v, nil)
			} else {
				columns[k] = marshalColumnValuePair(&v, &oldV)
			}
		} else {
			columns[k] = marshalColumnValuePair(&v, nil)
		}
	}

	var t string
	if msg.ServerTime != 0 {
		// ServerTime * 1,000,000 to convert from milliseconds to nanoseconds
		t = time.Unix(0, int64(msg.ServerTime)*1000000).UTC().Format(time.RFC3339)
	} else {
		t = epochFormatted
	}

	// Write LSN to string without additional memory allocations
	lnsWriter.Reset(&lsnBuffer)
	lsnBuffer.Reset()
	_, _ = fmt.Fprintf(lnsWriter, "%X/%X", uint32(msg.WalStart>>32), uint32(msg.WalStart))
	_ = lnsWriter.Flush()
	lsnBytes := lsnBuffer.Bytes()

	// Construct WalEntry
	reusedWalEntry.Time = t
	reusedWalEntry.Lsn = *(*string)(unsafe.Pointer(&lsnBytes))
	reusedWalEntry.Table = msg.Pr.Relation
	reusedWalEntry.Operation = msg.Pr.Operation
	reusedWalEntry.Columns = columns

	ret, err := gojson.Marshal(reusedWalEntry)

	clearColValues()
	clearColValuePairs()

	return ret, err
}
