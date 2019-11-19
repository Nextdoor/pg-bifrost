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
	"encoding/base64"
	"fmt"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/replication/client/conn/mocks"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func getServerHeartbeatMessage(serverWalEnd uint64) *pgx.ReplicationMessage {
	pgxServerHeartbeat := &pgx.ServerHeartbeat{
		ServerWalEnd:   serverWalEnd,
		ServerTime:     0,
		ReplyRequested: 1,
	}

	return &pgx.ReplicationMessage{
		ServerHeartbeat: pgxServerHeartbeat,
	}
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
	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(1)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).Times(1)
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1

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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	walReplMsg := &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}
	expected, _ := replication.PgxReplicationMessageToWalMessage(walReplMsg)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)

	serverWalEnd := uint64(111)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, err).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(2)
	mockConn.EXPECT().IsAlive().Return(false).MinTimes(2)

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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	walReplMsg := &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}

	serverWalEnd := uint64(111)
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	beginWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210920),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      beginData,
	}
	beginReplMsg := &pgx.ReplicationMessage{
		WalMessage: beginWalMsg,
	}

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210920),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      updateData,
	}
	updateReplMsg := &pgx.ReplicationMessage{
		WalMessage: updateWalMsg,
	}

	// Commit
	// walData.WalString:
	// COMMIT 566
	commitnData, _ := base64.StdEncoding.DecodeString("Q09NTUlUIDU2Ng==")
	commitWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210921),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      commitnData,
	}
	commitReplMsg := &pgx.ReplicationMessage{
		WalMessage: commitWalMsg,
	}

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(8)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(updateReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(commitReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(updateReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(commitReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	beginData, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY2")
	beginWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210920),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      beginData,
	}
	beginReplMsg := &pgx.ReplicationMessage{
		WalMessage: beginWalMsg,
	}

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210920),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      updateData,
	}
	updateReplMsg := &pgx.ReplicationMessage{
		WalMessage: updateWalMsg,
	}

	// Begin
	// walData.WalString:
	// BEGIN 567
	beginData2, _ := base64.StdEncoding.DecodeString("QkVHSU4gNTY3")
	beginWalMsg2 := &pgx.WalMessage{
		WalStart:     uint64(22210921),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      beginData2,
	}
	beginReplMsg2 := &pgx.ReplicationMessage{
		WalMessage: beginWalMsg2,
	}

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(5)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(updateReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg2, nil).Times(1)
	mockManager.EXPECT().Close().Times(1)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg, nil).Times(1)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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
	beginWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      beginData,
	}
	beginReplMsg := &pgx.ReplicationMessage{
		WalMessage: beginWalMsg,
	}

	// Update
	// walData.WalString:
	// table public.customers: UPDATE: id[integer]:7 first_name[text]:'Hello' last_name[text]:'World7'
	updateData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogVVBEQVRFOiBpZFtpbnRlZ2VyXTo3IGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkNyc=")
	updateWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      updateData,
	}
	updateReplMsg := &pgx.ReplicationMessage{
		WalMessage: updateWalMsg,
	}

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(4)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(beginReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(updateReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
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

func _getWalMessage(msgStr string) *pgx.ReplicationMessage {
	walData, _ := base64.StdEncoding.DecodeString(msgStr)
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	return &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}
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
	walReplMsg := _getWalMessage("c29tZSBpbnZhbGlkIHdhbCBkYXRh")

	serverWalEnd := uint64(111)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(2)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil)
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(serverWalEnd), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
		}).MinTimes(4)

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
	WalWritePosition uint64
	WalFlushPosition uint64
	WalApplyPosition uint64
	ReplyRequested   byte
}

// Matches uses standbyStatusMatcher to do a specific comparison
func (s *standbyStatusMatcher) Matches(x interface{}) bool {
	ss := x.(*pgx.StandbyStatus)

	return ss.WalWritePosition == s.WalWritePosition &&
		ss.WalFlushPosition == s.WalFlushPosition &&
		ss.WalApplyPosition == s.WalApplyPosition &&
		ss.ReplyRequested == s.ReplyRequested
}

func (s *standbyStatusMatcher) String() string {
	return fmt.Sprintf("WalWritePosition: %d, WalFlushPosition: %d, WalApplyPosition: %d, ReplyRequested: %d",
		s.WalWritePosition, s.WalFlushPosition, s.WalApplyPosition, s.ReplyRequested)
}

// EqStatusWithoutTime is a gomock.Matcher that we can pass to matcher as arguments
func EqStatusWithoutTime(status *pgx.StandbyStatus) gomock.Matcher {
	return &standbyStatusMatcher{
		status.WalWritePosition,
		status.WalFlushPosition,
		status.WalApplyPosition,
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

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(uint64(10)), nil).Times(1)

	standbyWalStart := uint64(11111111)
	status, _ := pgx.NewStandbyStatus(standbyWalStart)
	status.ReplyRequested = 1
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(3)
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status)).MinTimes(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 5)
			expectChan <- 1
		}).MinTimes(1)

	progChan <- uint64(standbyWalStart)
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
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Times(1)
	mockManager := mocks.NewMockManagerInterface(mockCtrl)

	// Setup
	progChan := make(chan uint64, 1000)
	statsChan := make(chan stats.Stat, 1000)

	// Expects
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)
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
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	walReplMsg := &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)

	initalWalStart := uint64(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(initalWalStart), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).MinTimes(1)

	status, _ := pgx.NewStandbyStatus(initalWalStart)
	status.ReplyRequested = 1
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status)).MinTimes(1)

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
	err := errors.New("context deadline exceeded")
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(3)

	initalWalStart := uint64(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(initalWalStart), nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, err).MinTimes(2)

	status, _ := pgx.NewStandbyStatus(initalWalStart)
	status.ReplyRequested = 1
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status)).MinTimes(1)

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
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).Times(1)
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1

	// Our progress
	progress1 := uint64(20)
	progress2 := uint64(30)
	progress3 := uint64(40)
	status3, _ := pgx.NewStandbyStatus(progress3)
	status3.ReplyRequested = 1

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(3)
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).MinTimes(1)
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status3)).MinTimes(1)

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil,
		errors.New("context deadline exceeded")).Do(
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).Times(1)
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1

	// repl messages
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil,
		errors.New("context deadline exceeded")).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	// status
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).MinTimes(1)

	// shutdown
	mockManager.EXPECT().Close().Times(1)

	sh := shutdown.NewShutdownHandler()
	replicator := New(sh, statsChan, mockManager, 10)

	go replicator.Start(progChan)

	// test
	time.Sleep(40 * time.Millisecond)
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
	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil,
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
	mockManager.EXPECT().GetConn().Return(nil, err).MinTimes(1)
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
	mockManager.EXPECT().GetConn().Return(nil, err).Times(1)
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
	mockManager.EXPECT().GetConn().Return(nil, err).Do(
		func(_ interface{}) {
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)

	// CommitWalStart from server
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).Times(1)

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
	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(1)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil,
		errors.New("context deadline exceeded")).Do(
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).MinTimes(1)

	// expect to reply
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).MinTimes(1).Do(
		func(_ interface{}) {
			expectChan <- 1
		})

	go replicator.Start(progChan)

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

func TestHeartbeatRequestedError(t *testing.T) {
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()
	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(1)

	err := errors.New("error")
	mockManager.EXPECT().GetConn().Return(nil, err).Times(1)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).MinTimes(1)

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
	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(4)
	expectChan := make(chan interface{}, 100)

	// Setup return
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	walReplMsg := &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(2)

	// expect to reply
	progress0 := uint64(10)
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).Times(1).Do(
		func(_ interface{}) {
			expectChan <- 1
		})

	// Do test
	go replicator.Start(progChan)

	// Wait for expect
	select {
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "did not pass test in time")
	case <-expectChan:
		replicator.shutdownHandler.CancelFunc()
	}

	// Wait for shutdown
	waitForShutdown(t, mockManager, sh, stoppedChan)

}

func TestSendKeepaliveChanFullError(t *testing.T) {
	// Setup mock
	mockCtrl, replicator, progChan, mockManager, mockConn := getBasicTestSetup(t)
	defer mockCtrl.Finish()

	expectChan := make(chan interface{}, 1)

	mockManager.EXPECT().GetConn().Return(mockConn, nil).Times(3)

	// Setup return
	// table public.customers: INSERT: id[integer]:1 first_name[text]:'Hello' last_name[text]:'World'
	walData, _ := base64.StdEncoding.DecodeString("dGFibGUgcHVibGljLmN1c3RvbWVyczogSU5TRVJUOiBpZFtpbnRlZ2VyXToxIGZpcnN0X25hbWVbdGV4dF06J0hlbGxvJyBsYXN0X25hbWVbdGV4dF06J1dvcmxkJw==")
	pgxWalMsg := &pgx.WalMessage{
		WalStart:     uint64(22210928),
		ServerWalEnd: uint64(0),
		ServerTime:   uint64(0),
		WalData:      walData,
	}
	walReplMsg := &pgx.ReplicationMessage{
		WalMessage: pgxWalMsg,
	}

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).Times(2)

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(walReplMsg, nil).Do(
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(2)

	// server asks for heartbeat
	progress0 := uint64(10)
	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(getServerHeartbeatMessage(progress0), nil).MinTimes(1)

	// expect to reply
	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1

	err := errors.New("expected error")
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).Return(err).Times(1)
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

	mockManager.EXPECT().GetConn().Return(mockConn, nil).MinTimes(1)

	mockConn.EXPECT().WaitForReplicationMessage(gomock.Any()).Return(nil, nil).Do(
		func(_ interface{}) {
			time.Sleep(time.Millisecond * 1)
		}).MinTimes(1)

	progress0 := uint64(1000)
	progress1 := uint64(999)

	status0, _ := pgx.NewStandbyStatus(progress0)
	status0.ReplyRequested = 1
	mockConn.EXPECT().SendStandbyStatus(EqStatusWithoutTime(status0)).Times(1).Do(
		func(_ interface{}) {
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

func TestFoo(t *testing.T) {
	time.Sleep(5 * time.Second)
}