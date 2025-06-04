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
	"testing"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/parselogical"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetLevel(logrus.InfoLevel)
}

func _basicInsertMessage() *replication.WalMessage {
	columns := map[string]parselogical.ColumnValue{}

	column := parselogical.ColumnValue{
		Value:  "Foo",
		Type:   "string",
		Quoted: true,
	}

	columns["first_name"] = column

	oldColumns := map[string]parselogical.ColumnValue{}

	msg := string("test")
	ps := parselogical.ParseState{
		Msg:           &msg,
		Current:       1,
		Prev:          1,
		TokenStart:    1,
		OldKey:        false,
		CurColumnName: "username",
		CurColumnType: "string",
	}

	pr := parselogical.ParseResult{
		State:       ps,
		Transaction: "0",
		Relation:    "test.users",
		Operation:   "INSERT",
		NoTupleData: false,
		Columns:     columns,
		OldColumns:  oldColumns,
	}

	walMessage := replication.WalMessage{
		WalStart:     1,
		ServerWalEnd: 0,
		ServerTime:   0,
		TimeBasedKey: "0-0",
		Pr:           &pr,
	}

	return &walMessage
}

func _basicUpdateMessage() *replication.WalMessage {
	columns := map[string]parselogical.ColumnValue{}

	column := parselogical.ColumnValue{
		Value:  "Foo",
		Type:   "string",
		Quoted: true,
	}

	columns["first_name"] = column

	oldColumn := parselogical.ColumnValue{
		Value:  "Bar",
		Type:   "string",
		Quoted: true,
	}

	oldColumns := map[string]parselogical.ColumnValue{}
	oldColumns["first_name"] = oldColumn

	msg := string("test")
	ps := parselogical.ParseState{
		Msg:           &msg,
		Current:       1,
		Prev:          1,
		TokenStart:    1,
		OldKey:        false,
		CurColumnName: "username",
		CurColumnType: "string",
	}

	pr := parselogical.ParseResult{
		State:       ps,
		Transaction: "0",
		Relation:    "test.users",
		Operation:   "INSERT",
		NoTupleData: false,
		Columns:     columns,
		OldColumns:  oldColumns,
	}

	walMessage := replication.WalMessage{
		WalStart:     1,
		ServerWalEnd: 0,
		ServerTime:   0,
		TimeBasedKey: "0-0",
		Pr:           &pr,
	}

	return &walMessage
}

func _setupMarshaller(noMarshalOldValue bool) (Marshaller, chan *replication.WalMessage, chan error, chan stats.Stat) {
	sh := shutdown.NewShutdownHandler()
	in := make(chan *replication.WalMessage)
	errorChan := make(chan error)
	statsChan := make(chan stats.Stat, 1000)

	m := New(sh, in, statsChan, noMarshalOldValue)

	return m, in, errorChan, statsChan
}

func TestBasicInsertMessage(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	in <- _basicInsertMessage()

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "INSERT", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := "{\"time\":\"1970-01-01T00:00:00Z\",\"time_ms\":0,\"txn\":\"0-0\",\"lsn\":\"0/1\",\"table\":\"test.users\",\"operation\":\"INSERT\",\"columns\":{\"first_name\":{\"new\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Foo\"}}}}"
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestBasicUpdateMessage(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	in <- _basicUpdateMessage()

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "INSERT", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := "{\"time\":\"1970-01-01T00:00:00Z\",\"time_ms\":0,\"txn\":\"0-0\",\"lsn\":\"0/1\",\"table\":\"test.users\",\"operation\":\"INSERT\",\"columns\":{\"first_name\":{\"new\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Foo\"},\"old\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Bar\"}}}}"
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestUpdateWithNull(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Construct message
	columns := map[string]parselogical.ColumnValue{}

	columns["first_name"] = parselogical.ColumnValue{
		Value:  "Foo",
		Type:   "string",
		Quoted: true,
	}

	columns["middle_name"] = parselogical.ColumnValue{
		Value:  "null",
		Type:   "string",
		Quoted: false,
	}

	columns["last_name"] = parselogical.ColumnValue{
		Value:  "Last",
		Type:   "string",
		Quoted: true,
	}

	oldColumns := map[string]parselogical.ColumnValue{}
	oldColumns["first_name"] = parselogical.ColumnValue{
		Value:  "Bar",
		Type:   "string",
		Quoted: true,
	}

	msg := string("test")
	ps := parselogical.ParseState{
		Msg:           &msg,
		Current:       1,
		Prev:          1,
		TokenStart:    1,
		OldKey:        false,
		CurColumnName: "username",
		CurColumnType: "string",
	}

	pr := parselogical.ParseResult{
		State:       ps,
		Transaction: "0",
		Relation:    "test.users",
		Operation:   "UPDATE",
		NoTupleData: false,
		Columns:     columns,
		OldColumns:  oldColumns,
	}

	walMessage := replication.WalMessage{
		WalStart:     1,
		ServerWalEnd: 0,
		ServerTime:   0,
		TimeBasedKey: "0-0",
		Pr:           &pr,
	}

	// Send data in to marshaller
	in <- &walMessage

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "UPDATE", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := `{"time":"1970-01-01T00:00:00Z","time_ms":0,"txn":"0-0","lsn":"0/1","table":"test.users","operation":"UPDATE","columns":{"first_name":{"new":{"q":"true","t":"string","v":"Foo"},"old":{"q":"true","t":"string","v":"Bar"}},"last_name":{"new":{"q":"true","t":"string","v":"Last"},"old":{"q":"false","t":"string","v":"null"}},"middle_name":{"new":{"q":"false","t":"string","v":"null"}}}}`
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestBasicUpdateNoOldMessage(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(true)
	go m.Start()

	// Send data in to marshaller
	in <- _basicUpdateMessage()

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "INSERT", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := "{\"time\":\"1970-01-01T00:00:00Z\",\"time_ms\":0,\"txn\":\"0-0\",\"lsn\":\"0/1\",\"table\":\"test.users\",\"operation\":\"INSERT\",\"columns\":{\"first_name\":{\"new\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Foo\"}}}}"
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestToastValue(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	bm := _basicUpdateMessage()
	toast := parselogical.ColumnValue{
		Value:  "unchanged-toast-datum",
		Type:   "string",
		Quoted: true,
	}

	bm.Pr.Columns["first_name"] = toast

	in <- bm

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "INSERT", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := "{\"time\":\"1970-01-01T00:00:00Z\",\"time_ms\":0,\"txn\":\"0-0\",\"lsn\":\"0/1\",\"table\":\"test.users\",\"operation\":\"INSERT\",\"columns\":{\"first_name\":{\"new\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Bar\"},\"old\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Bar\"}}}}"
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestToastNoOldValue(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(true)
	go m.Start()

	// Send data in to marshaller
	bm := _basicUpdateMessage()
	toast := parselogical.ColumnValue{
		Value:  "unchanged-toast-datum",
		Type:   "string",
		Quoted: true,
	}

	bm.Pr.Columns["first_name"] = toast

	in <- bm

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, uint64(1), marshalled.WalStart)
	assert.Equal(t, "INSERT", marshalled.Operation)
	assert.Equal(t, "test.users", marshalled.Table)
	assert.Equal(t, "0", marshalled.Transaction)

	expectedJson := "{\"time\":\"1970-01-01T00:00:00Z\",\"time_ms\":0,\"txn\":\"0-0\",\"lsn\":\"0/1\",\"table\":\"test.users\",\"operation\":\"INSERT\",\"columns\":{\"first_name\":{\"new\":{\"q\":\"true\",\"t\":\"string\",\"v\":\"Bar\"}}}}"
	assert.Equal(t, expectedJson, string(marshalled.Json))
}

func TestTimeBasedKey(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	in <- _basicInsertMessage()

	// Setup timer to catch issues with Marshaller reading from channel
	timeout := time.NewTimer(25 * time.Millisecond)

	var marshalled *MarshalledMessage
	var ok bool

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case m, okay := <-m.OutputChan:
		marshalled = m
		ok = okay
		break
	}

	if !ok {
		assert.Fail(t, "something went wrong.. the output channel is closed.")
	}

	assert.Equal(t, "0-0", marshalled.TimeBasedKey)
}

// TODO(#10): to test failures we need to have an interface and mock out the marshall function
// func TestFailStat(t *testing.T) {}

func TestSuccessStat(t *testing.T) {
	// Setup test
	m, in, _, s := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	in <- _basicInsertMessage()

	// Read output
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case <-timeout.C:
		assert.Fail(t, "did not get marshalled message in time")
	case <-m.OutputChan:
	}

	expectedStats := []stats.Stat{stats.NewStatCount("marshaller", "success", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestInputChannelClose(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Close input channel
	close(in)

	_, ok := <-m.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}

	_, ok = <-m.shutdownHandler.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestTerminationContextInput(t *testing.T) {
	// Setup test
	m, _, _, _ := _setupMarshaller(false)
	go m.Start()

	// Cancel
	m.shutdownHandler.CancelFunc()

	_, ok := <-m.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestTerminationContextOutput(t *testing.T) {
	// Setup test
	m, in, _, _ := _setupMarshaller(false)
	go m.Start()

	// Send data in to marshaller
	in <- _basicInsertMessage()

	time.Sleep(10 * time.Millisecond)
	m.shutdownHandler.CancelFunc()

	time.Sleep(10 * time.Millisecond)
	<-m.OutputChan          // first message
	_, ok := <-m.OutputChan // now channel is empty
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func BenchmarkMarshalWalToJson(b *testing.B) {
	msg := _basicInsertMessage()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = marshalWalToJson(msg, true)
	}
}
