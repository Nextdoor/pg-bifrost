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
	"github.com/json-iterator/go"

	"os"
	"time"

	"github.com/Nextdoor/parselogical"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "marshaller")
	json   = jsoniter.ConfigCompatibleWithStandardLibrary
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Marshaller struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan  <-chan *replication.WalMessage
	OutputChan chan *MarshalledMessage

	statsChan chan stats.Stat

	noMarshalOldValue bool
	inferUpdatedNulls bool
}

// New is a simple constructor which create a marshaller.
func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *replication.WalMessage,
	statsChan chan stats.Stat,
	noMarshalOldValue bool,
	inferUpdatedNulls bool) Marshaller {

	outputChan := make(chan *MarshalledMessage)

	return Marshaller{shutdownHandler, inputChan, outputChan, statsChan, noMarshalOldValue, inferUpdatedNulls}
}

// jsonWalEntry is a helper struct which has json field tags
type jsonWalEntry struct {
	Time      *string                                  `json:"time"`
	Lsn       *string                                  `json:"lsn"` // Log Sequence Number that determines position in WAL
	Table     *string                                  `json:"table"`
	Operation *string                                  `json:"operation"`
	Columns   *map[string]map[string]map[string]string `json:"columns"`
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
		recover()
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

		byteMessage, err := marshalWalToJson(walMessage, m.noMarshalOldValue, m.inferUpdatedNulls)

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
	return map[string]string{"v": cv.Value, "t": cv.Type, "q": quoted}
}

// marshalColumnValuePair marshals the column value pairs which shows the old and new columns
func marshalColumnValuePair(newValue *parselogical.ColumnValue, oldValue *parselogical.ColumnValue) map[string]map[string]string {
	if oldValue != nil && newValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
			"new": marshalColumnValue(newValue),
		}
	} else if newValue != nil {
		return map[string]map[string]string{
			"new": marshalColumnValue(newValue),
		}
	} else if oldValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
		}
	}

	return nil
}

// marshalWalToJson marshals a WalMessage using parselogical to parse the columns and returns a byte slice
func marshalWalToJson(msg *replication.WalMessage, noMarshalOldValue bool, inferUpdatedNulls bool) ([]byte, error) {
	lsn := pglogrepl.LSN(msg.WalStart).String()

	// ServerTime * 1,000,000 to convert from milliseconds to nanoseconds
	t := time.Unix(0, int64(msg.ServerTime)*1000000).Format(time.RFC3339)
	columns := make(map[string]map[string]map[string]string)

	for k, v := range msg.Pr.Columns {
		oldV, ok := msg.Pr.OldColumns[k]

		if msg.Pr.Operation == "DELETE" {
			columns[k] = marshalColumnValuePair(nil, &v)
			continue
		}

		if ok && v.Value != oldV.Value {
			// When column is TOAST-ed use the previous value instead of "unchanged-toast-datum"
			if !v.Quoted && v.Value == "unchanged-toast-datum" {
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
		} else if inferUpdatedNulls && !noMarshalOldValue && !ok && msg.Pr.Operation == "UPDATE" {
			// The test_decoding output omits NULL values from the old-key.
			//
			// Ordinarily the "old" key being omitted from the value pair represents an unchanged column, which
			// consequently means that downstream consumers cannot differentiate between an unchanged column and
			// an update from NULL to not NULL.

			// Construct a NULL value when the column is missing from OldColumns
			columns[k] = marshalColumnValuePair(&v, &parselogical.ColumnValue{
				Value:  "null",
				Type:   v.Type,
				Quoted: false,
			})
		} else {
			columns[k] = marshalColumnValuePair(&v, nil)
		}
	}

	return json.Marshal(&jsonWalEntry{
		Time:      &t,
		Lsn:       &lsn,
		Table:     &msg.Pr.Relation,
		Operation: &msg.Pr.Operation,
		Columns:   &columns,
	})
}
