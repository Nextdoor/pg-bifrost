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

package client

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/replication/client/conn/mocks"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/golang/mock/gomock"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"time"

	"testing"
)

func getPrimaryKeepaliveMessage(serverWalEnd uint64) pgproto3.BackendMessage {
	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:9], serverWalEnd)
	binary.BigEndian.PutUint64(data[9:17], uint64(0))
	data[17] = 1 // Reply Requested = true

	return &pgproto3.CopyData{Data: data}
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func timeToPgTime(unixSeconds int64) int64 {
	microsecSinceUnixEpoch := unixSeconds * 1000000
	return microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
}

func getXLogData(walData []byte, walStart uint64, serverWalEnd uint64, serverTime int64) pgproto3.BackendMessage {
	data := make([]byte, 25+len(walData))

	data[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(data[1:9], walStart)
	binary.BigEndian.PutUint64(data[9:17], serverWalEnd)
	binary.BigEndian.PutUint64(data[17:25], uint64(timeToPgTime(serverTime)))
	copy(data[25:], walData)

	return &pgproto3.CopyData{Data: data}
}

func TestGetXLogData(t *testing.T) {
	walData := []byte("foobar")
	walStart := uint64(100)
	serverWalEnd := uint64(200)
	serverTime := time.Now().Unix()

	message := getXLogData(walData, walStart, serverWalEnd, serverTime)

	cd, ok := message.(*pgproto3.CopyData)
	assert.Equal(t, true, ok, "Failed to cast message to pgproto3.CopyData")

	xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
	assert.NoError(t, err, "failed to parse message as XLogData")

	assert.Equal(t, pglogrepl.LSN(walStart), xld.WALStart)
	assert.Equal(t, pglogrepl.LSN(serverWalEnd), xld.ServerWALEnd)
	assert.Equal(t, serverTime, xld.ServerTime.Unix())
}

func getBasicTestSetup(test *testing.T) (*gomock.Controller, Replicator, chan uint64, *mocks.MockManagerInterface, *mocks.MockConn) {
	// Setup mock
	mockCtrl := gomock.NewController(test)
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 2)

	// Setup return
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).Times(1)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Times(1)

	return mockCtrl, replicator, progChan, mockManager, mockConn
}

func waitForShutdown(t *testing.T, mockManager *mocks.MockManagerInterface, handler shutdown.ShutdownHandler, stoppedChan chan struct{}) {
	// Wait for closing cleanup
	mockManager.EXPECT().Close().Times(1)
	handler.CancelFunc()

	// Add a little delay to ensure shutdown ran
	var timeout *time.Timer
	timeout = time.NewTimer(4000 * time.Millisecond)

	// Check to see if shutdown closed output channel
	select {
	case <-timeout.C:
		assert.Fail(t, "shutdown didn't close output chan in time")
	case _, ok := <-stoppedChan:
		if ok {
			assert.Fail(t, "shutdown didn't close output chan")
		}
	}
}

func TestGetBasicTestSetup(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()
	defer mockCtrl.Finish()

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(1)

	go replicator.Start(progChan)

	time.Sleep(10 * time.Millisecond)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

// TestWalMessage verifies that WaitForReplicationMessage is called and sends the ReplicationMessage on the output channel
func TestWalMessage(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockManager := mocks.NewMockManagerInterface(mockCtrl)
	mockConn := mocks.NewMockConn(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	// Setup return
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))
	cd, _ := backendMessage.(*pgproto3.CopyData)
	xld, _ := pglogrepl.ParseXLogData(cd.Data[1:])

	expected, _ := replication.XLogDataToWalMessage(xld)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(2)

	serverWalEnd := uint64(111)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(1)

	// Do test
	go replicator.Start(progChan)

	// Wait for shutdown
	time.Sleep(time.Millisecond * 5)

	select {
	case <-time.After(25 * time.Millisecond):
		assert.Fail(t, "did not get output in time")
	case message := <-replicator.GetOutputChan():
		assert.Equal(t, expected, message)
	}

	// Verify stats
	expectedStats := []stats.Stat{stats.NewStatCount("replication", "received", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

// TestReplicationError tests handling of error returned by WaitForReplicationMessage
// which continues looping to the next iteration.
func TestReplicationError(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	// Setup return
	serverWalEnd := uint64(111)

	err := errors.New("expected context error")
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, err).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)
	mockConn.EXPECT().IsClosed().Return(true).MinTimes(2)

	// Do test
	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	// Verify stats
	expectedStats := []stats.Stat{}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

// TestMsgNotCopyData tests handling of when the message is not of CopyData type.
func TestMsgNotCopyData(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)

	// Setup return
	serverWalEnd := uint64(111)

	// Unexpected data
	msg := pgproto3.AuthenticationSASL{}

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(&msg, nil).Times(1)
	mockManager.EXPECT().Close().Times(1)

	// Do test
	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	// Verify stats
	expectedStats := []stats.Stat{}
	stats.VerifyStats(t, statsChan, expectedStats)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestNilMessage(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	// Setup return
	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)

	// Do test
	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	// Verify stats
	expectedStats := []stats.Stat{}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestStartValidMessage(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	// walData.WalString:
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))

	serverWalEnd := uint64(111)
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(1)

	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		// pass
	case <-time.After(25 * time.Millisecond):
		assert.Fail(t, "a WalMessage was not received from the outputChan")
	}

	// Verify stats
	expectedStats := []stats.Stat{stats.NewStatCount("replication", "received", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestTxnsMessage(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	// Begin
	// walData.WalString:
	// BEGIN 566
	beginData, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY2")
	beginMessage := getXLogData(beginData, uint64(22210920), uint64(0), int64(0))

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateMessage := getXLogData(updateData, uint64(22210920), uint64(0), int64(0))

	// Commit
	// walData.WalString:
	// COMMIT 566
	commitData, _ := base64.StdEncoding.DecodeString("Q09NTUlUIDU2Ng==")
	commitMessage := getXLogData(commitData, uint64(22210921), uint64(0), int64(0))

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(8)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(updateMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(commitMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(updateMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(commitMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)

	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(10 * time.Millisecond)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		// pass
	case <-time.After(200 * time.Millisecond):
		assert.Fail(t, "a WalMessage was not received from the outputChan")
	}

	// Verify stats
	expectedStats := []stats.Stat{
		// Transaction 1
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // BEGIN
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // UPDATE
		stats.NewStatCount("replication", "txns", 1, time.Now().UnixNano()),     // COMMIT
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // COMMIT

		// Transaction 1 duplicate
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // BEGIN
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // UPDATE
		stats.NewStatCount("replication", "txns", 1, time.Now().UnixNano()),     // COMMIT
		stats.NewStatCount("replication", "txns_dup", 1, time.Now().UnixNano()), // COMMIT
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()), // COMMIT
	}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestNoCommit(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	// Begin
	// walData.WalString:
	// BEGIN 566
	beginData, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY3")
	beginMessage := getXLogData(beginData, uint64(22210920), uint64(0), int64(0))

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateMessage := getXLogData(updateData, uint64(22210920), uint64(0), int64(0))

	// Begin
	// walData.WalString:
	// BEGIN 567
	beginData2, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY3")
	beginMessage2 := getXLogData(beginData2, uint64(22210921), uint64(0), int64(0))

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(5)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(updateMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage2, nil).Times(1)
	mockManager.EXPECT().Close().Times(1)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)

	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(10 * time.Millisecond)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		// pass
	case <-time.After(200 * time.Millisecond):
		assert.Fail(t, "a WalMessage was not received from the outputChan")
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestDupWalStart(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	// Begin
	// walData.WalString:
	// BEGIN 566
	beginData, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY2")
	beginMessage := getXLogData(beginData, uint64(22210928), uint64(0), int64(0))

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateMessage := getXLogData(updateData, uint64(22210928), uint64(0), int64(0))

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(4)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(updateMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(3)

	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		// pass
	case <-time.After(200 * time.Millisecond):
		assert.Fail(t, "a WalMessage was not received from the outputChan")
	}

	// Verify stats
	expectedStats := []stats.Stat{
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()),
		stats.NewStatCount("replication", "received", 1, time.Now().UnixNano()),
	}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestStartInvalidWalString(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()

	// walData.WalString:
	// some invalid wal data
	walData, _ := base64.StdEncoding.DecodeString("c29tZSBpbnZhbGlkIHdhbCBkYXRh")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil)
	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	// Wait for shutdown
	time.Sleep(time.Millisecond * 5)

	select {
	case _, ok := <-out:
		if ok {
			assert.Fail(t, "a WalMessage was unexpectedly received from the outputChan")
		}
		// pass
	case <-time.After(1000 * time.Millisecond):
		assert.Fail(t, "outputChan was not closed")
	}

	_, ok := <-replicator.shutdownHandler.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}

	// Verify stats
	expectedStats := []stats.Stat{stats.NewStatCount("replication", "invalid_msg", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, statsChan, expectedStats)
}

func TestStartNilWalMessage(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)

	go replicator.Start(progChan)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		assert.Fail(t, "got a message when not expected")
	case <-time.After(25 * time.Millisecond):
		// pass
	}

	// Verify stats
	expectedStats := []stats.Stat{}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

// used to compare all fields of StandbyStatus except ClientTime
type standbyStatusMatcher struct {
	WalWritePosition pglogrepl.LSN
	WalFlushPosition pglogrepl.LSN
	WalApplyPosition pglogrepl.LSN
	ReplyRequested   bool
}

// Matches uses standbyStatusMatcher to do a specific comparison
func (s *standbyStatusMatcher) Matches(x interface{}) bool {
	ss := x.(pglogrepl.StandbyStatusUpdate)

	return ss.WALWritePosition == s.WalWritePosition &&
		ss.WALFlushPosition == s.WalFlushPosition &&
		ss.WALApplyPosition == s.WalApplyPosition &&
		ss.ReplyRequested == s.ReplyRequested
}

func (s *standbyStatusMatcher) String() string {
	return fmt.Sprintf("WalWritePosition: %d, WalFlushPosition: %d, WalApplyPosition: %d, ReplyRequested: %v",
		s.WalWritePosition, s.WalFlushPosition, s.WalApplyPosition, s.ReplyRequested)
}

// EqStatusWithoutTime is a gomock.Matcher that we can pass to matcher as arguments
func EqStatusWithoutTime(status pglogrepl.StandbyStatusUpdate) gomock.Matcher {

	return &standbyStatusMatcher{
		status.WALWritePosition,
		status.WALFlushPosition,
		status.WALApplyPosition,
		status.ReplyRequested,
	}
}

func TestStartWithSendStandbyStatus(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)
	expectChan := make(chan interface{}, 100)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(uint64(10)), nil).Times(1)

	standbyWalStart := uint64(11111111)
	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(standbyWalStart),
		ReplyRequested:   true,
	}

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(3)
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status)).MinTimes(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
			expectChan <- 1
		}).MinTimes(1)

	progChan <- standbyWalStart
	go replicator.Start(progChan)

	// Wait a little bit for replicator to process progress
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestClosedProgressChan(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Times(1)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	// Expects
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(1)
	mockManager.EXPECT().Close().Times(1)

	sh := shutdown.NewShutdownHandler()

	replicator := New(sh, statsChan, mockManager, 10)
	out := replicator.GetOutputChan()

	go replicator.Start(progChan)

	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "shutdown probably caused panic", r)
		}
	}()

	close(progChan)

	// Add a little delay to ensure shutdown ran
	var timeout *time.Timer
	timeout = time.NewTimer(100 * time.Millisecond)

	// Check to see if shutdown closed output channel
	select {
	case <-timeout.C:
		assert.Fail(t, "shutdown didn't close output chan in time")
	case _, ok := <-out:
		if ok {
			assert.Fail(t, "shutdown didn't close output chan")
		}
	}

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.shutdownHandler.TerminateCtx.Done():
		if ok {
			assert.Fail(t, "context not cancelled")
		}
	}
}

func TestStartOutputChannelFull(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 1)
	out := replicator.GetOutputChan()
	stoppedChan := replicator.GetStoppedChan()

	// walData.WalString:
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)

	initalWalStart := uint64(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(initalWalStart), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).MinTimes(1)

	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(initalWalStart),
		ReplyRequested:   true,
	}

	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status)).MinTimes(1)

	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(5 * time.Millisecond)

	select {
	case _, ok := <-out:
		if !ok {
			assert.Fail(t, "replicator was closed unexpectedly")
		}
		// pass
	case <-time.After(25 * time.Millisecond):
		assert.Fail(t, "a WalMessage was not received from the outputChan")
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)

}

func TestDeadlineExceededTwice(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup channels
	progChan := make(chan uint64, 10)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	// Setup return
	err := context.DeadlineExceeded
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(3)

	initalWalStart := uint64(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(initalWalStart), nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, err).MinTimes(2)

	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(initalWalStart),
		ReplyRequested:   true,
	}

	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status)).MinTimes(1)

	// Do test
	go replicator.Start(progChan)

	// Wait for replicator to run
	time.Sleep(20 * time.Millisecond)

	// Verify stats
	expectedStats := []stats.Stat{}
	stats.VerifyStats(t, statsChan, expectedStats)

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestWithMultipleProgress(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()

	replicator := New(sh, statsChan, mockManager, 10)
	stoppedChan := replicator.GetStoppedChan()

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Times(1)
	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}

	// Our progress
	progress1 := uint64(20)
	progress2 := uint64(30)
	progress3 := uint64(40)
	status3 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress3),
		ReplyRequested:   true,
	}

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(3)
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).MinTimes(1)
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status3)).MinTimes(1)

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil,
		context.DeadlineExceeded).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 20)
		}).MinTimes(2)

	go replicator.Start(progChan)

	// Wait for first replicator to start
	select {
	case <-time.After(100 * time.Millisecond):
	}

	progChan <- uint64(progress1)
	progChan <- uint64(progress2)
	progChan <- uint64(progress3)

	// Wait a little bit for replicator to process progress
	select {
	case <-time.After(200 * time.Millisecond):
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestDeadlineExceeded(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockConn := mocks.NewMockConn(mockCtrl)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Times(1)

	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}

	// repl messages
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil,
		context.DeadlineExceeded).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	// status
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).MinTimes(1)

	// shutdown
	mockManager.EXPECT().Close().Times(1)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)

	go replicator.Start(progChan)

	// test
	time.Sleep(10 * time.Millisecond)
	replicator.shutdownHandler.CancelFunc()

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestTerminationContext(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(1)

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil,
		nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	// cancel
	time.Sleep(5 * time.Millisecond)
	replicator.shutdownHandler.CancelFunc()

	// wait for shutdown
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestGetConnectionError(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	err := errors.New("expected error")
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(nil, err).MinTimes(1)
	mockManager.EXPECT().Close().Times(1)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)

	go replicator.Start(progChan)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestGetConnectionErrorInLoop(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, _ := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	err := errors.New("expected error")
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(nil, err).Times(1)
	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestPanic(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, _ := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	err := errors.New("expected error")
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(nil, err).Do(
		func(_ interface{}, _ interface{}) {
			time.Sleep(time.Millisecond * 1)
			panic("TestPanic")
		}).Times(1)

	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestProgressChanClosed(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockManager := mocks.NewMockManagerInterface(mockCtrl)
	mockConn := mocks.NewMockConn(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(1)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Times(1)

	mockManager.EXPECT().Close().Times(1)

	// Close progress channel before startup
	close(progChan)

	go replicator.Start(progChan)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestProgressChanClosedDeadline(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	// Close progress channel before returning of WaitForReplicationMessage
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(1)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil,
		context.DeadlineExceeded).Do(
		func(_ interface{}) {
			close(progChan)
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	mockManager.EXPECT().Close().Times(1)

	// Do test
	go replicator.Start(progChan)

	// Wait for replicator to run
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestHeartbeatRequested(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()
	defer mockCtrl.Finish()
	expectChan := make(chan interface{}, 100)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Times(2)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).Do(
		func(_ interface{}) {
			time.Sleep(1 * time.Second)
		})

	// expect to reply
	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).MinTimes(1).Do(
		func(_, _ interface{}) {
			expectChan <- 1
		})

	go replicator.Start(progChan)

	time.Sleep(10 * time.Millisecond)

	// Wait for test to run
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestHeartbeatRequestedShutdown(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	stoppedChan := replicator.GetStoppedChan()
	defer mockCtrl.Finish()
	expectChan := make(chan interface{}, 100)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(10), nil).MinTimes(6)

	// expect to reply
	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).MinTimes(1).Do(
		func(_, _ interface{}) {
			expectChan <- 1
		})
	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	// Add a little delay to ensure shutdown ran
	var timeout *time.Timer
	timeout = time.NewTimer(1000 * time.Millisecond)

	// Check to see if shutdown closed output channel
	select {
	case <-timeout.C:
		assert.Fail(t, "shutdown didn't close output chan in time")
	case _, ok := <-stoppedChan:
		if ok {
			assert.Fail(t, "shutdown not called")
		}
	}
}

func TestHeartbeatRequestedError(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(1)

	err := errors.New("expected err")
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(nil, err).Times(1)

	// server asks for heartbeat
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(10), nil).MinTimes(1)

	mockManager.EXPECT().Close().Times(1)

	go replicator.Start(progChan)

	select {
	case <-time.After(40 * time.Millisecond):
		assert.Fail(t, "channel not closed in time")
	case _, ok := <-replicator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	}
}

func TestSendKeepaliveChanFull(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()
	defer mockCtrl.Finish()
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(4)
	expectChan := make(chan interface{}, 100)

	// Setup return
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(2)

	// expect to reply
	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(10),
		ReplyRequested:   true,
	}

	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).Times(1).Do(
		func(_, _ interface{}) {
			expectChan <- 1
		})

	// Do test
	go replicator.Start(progChan)

	// Wait for expect
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestSendKeepaliveChanFullError(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	expectChan := make(chan interface{}, 1)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(3)

	// Setup return
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	backendMessage := getXLogData(walData, uint64(22210928), uint64(0), int64(0))

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).Times(2)

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(backendMessage, nil).Do(
		func(_ interface{}) {
			replicator.shutdownHandler.CancelFunc()
			time.Sleep(time.Millisecond * 1)
		}).Times(1)
	mockManager.EXPECT().Close().Times(1).Do(func() {
		expectChan <- 1
	})

	// Do test
	go replicator.Start(progChan)

	// Wait for test to run
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}
}

func TestSendStandbyStatusError(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	expectChan := make(chan interface{}, 100)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(2)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(getPrimaryKeepaliveMessage(progress0), nil).MinTimes(1)

	// expect to reply
	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}

	err := errors.New("expected error")
	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).Return(err).Times(1)
	mockManager.EXPECT().Close().Times(1).Do(func() {
		expectChan <- 1
	})

	go replicator.Start(progChan)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}
}

func TestOldOverallProgress(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	expectChan := make(chan interface{}, 1)

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).MinTimes(1)

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	progress0 := uint64(1000)
	progress1 := uint64(999)

	status0 := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(progress0),
		ReplyRequested:   true,
	}

	mockConn.EXPECT().SendStandbyStatus(gomock.Any(), EqStatusWithoutTime(status0)).Times(1).Do(func(_, _ interface{}) {
		expectChan <- 1
	})

	progChan <- progress0
	progChan <- progress1

	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()
	go replicator.Start(progChan)

	time.Sleep(10 * time.Millisecond)

	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		// pass
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)
}

func TestRecovery(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()
	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()

	// COMMIT 565
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(0)).Return(mockConn, nil).Times(1)
	commitMessage := getXLogData([]byte("COMMIT 565"), uint64(11), uint64(0), int64(0))
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(commitMessage, nil).Times(1)

	// BEGIN 566
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(11)).Return(mockConn, nil).Times(1)
	beginMessage := getXLogData([]byte("BEGIN 566"), uint64(12), uint64(0), int64(0))
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(beginMessage, nil).Times(1)

	// Replication error
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(11)).Return(mockConn, nil).Times(1)
	errResp := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "could not find pg_class entry for 16398", Detail: "", Hint: "", Position: 0, InternalPosition: 0, InternalQuery: "", Where: "", SchemaName: "", TableName: "", ColumnName: "", DataTypeName: "", ConstraintName: "", File: "relcache.c", Line: 1145, Routine: "RelationInitPhysicalAddr", UnknownFields: map[uint8]string{0x56: "ERROR"}}
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(errResp, nil).Times(1)
	mockManager.EXPECT().Close()
	mockManager.EXPECT().GetConn(gomock.Any()).Return(mockConn, nil).Times(1)

	// Recovery
	ident := pglogrepl.IdentifySystemResult{XLogPos: pglogrepl.LSN(20)}
	mockConn.EXPECT().IdentifySystem(gomock.Any()).Return(ident, nil).Times(1)
	mockManager.EXPECT().Close()
	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), uint64(20)).Return(mockConn, nil).MinTimes(1)

	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)

	go replicator.Start(progChan)
	<-replicator.GetOutputChan()           // COMMIT 565
	begin := <-replicator.GetOutputChan()  // BEGIN 566
	commit := <-replicator.GetOutputChan() // COMMIT 566

	// Add a little delay to ensure shutdown ran
	time.Sleep(40 * time.Millisecond)

	waitForShutdown(t, mockManager, sh, stoppedChan)

	assert.Equal(t, "BEGIN", begin.Pr.Operation)
	assert.Equal(t, "COMMIT", commit.Pr.Operation)
	assert.Equal(t, uint64(11), commit.WalStart)
	assert.Equal(t, uint64(11), commit.ServerWalEnd)
}

func TestRecoveryFailed(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()
	sh := replicator.shutdownHandler
	stoppedChan := replicator.GetStoppedChan()

	mockManager.EXPECT().GetConnWithStartLsn(gomock.Any(), gomock.Any()).Return(mockConn, nil).Times(1)

	// Replication error
	errResp := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "could not find pg_class entry for 16398", Detail: "", Hint: "", Position: 0, InternalPosition: 0, InternalQuery: "", Where: "", SchemaName: "", TableName: "", ColumnName: "", DataTypeName: "", ConstraintName: "", File: "relcache.c", Line: 1145, Routine: "RelationInitPhysicalAddr", UnknownFields: map[uint8]string{0x56: "ERROR"}}
	mockConn.EXPECT().ReceiveMessage(gomock.Any()).Return(errResp, nil).Times(1)

	// Recovery
	mockManager.EXPECT().Close().Do(
		func() {
			time.Sleep(time.Millisecond * 5)
		}).Times(1)
	err := errors.New("expected error")
	mockManager.EXPECT().GetConn(gomock.Any()).Return(nil, err).Times(1)

	// Run
	go replicator.Start(progChan)
	time.Sleep(5 * time.Millisecond)

	// Wait for closing cleanup
	mockManager.EXPECT().Close().Times(1)
	sh.CancelFunc()

	// Add a little delay to ensure shutdown ran
	var timeout *time.Timer
	timeout = time.NewTimer(100 * time.Millisecond)

	// Check to see if shutdown closed output channel
	select {
	case <-timeout.C:
		assert.Fail(t, "shutdown didn't close output chan in time")
	case _, ok := <-stoppedChan:
		if ok {
			assert.Fail(t, "shutdown didn't close output chan")
		}
	}
}
