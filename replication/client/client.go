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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Nextdoor/parselogical"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/replication/client/conn"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	logger              = logrus.New()
	log                 = logger.WithField("package", "client")
	logProgressInterval = int64(30 * time.Second)

	// Settings for exponential sleep time. This prevents spinning when
	// there is a backlog.
	initialSleep = 10 * time.Millisecond
	maxSleep     = 2 * time.Second
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Replicator struct {
	shutdownHandler  shutdown.ShutdownHandler
	connManager      conn.ManagerInterface
	statsChan        chan stats.Stat
	progressChan     <-chan uint64
	overallProgress  uint64
	outputChan       chan *replication.WalMessage
	progressLastSent int64
	stoppedChan      chan struct{}

	// handlePrimaryKeepaliveMessage
	lastClientHeartbeatRequestTime time.Time
	heartbeatRequestDeltaTime      time.Duration
	heartbeatRequestCounter        int

	// handleXLogData
	transaction     string
	timeBasedKey    string
	highestWalStart uint64
	firstIteration  bool
	sawCommit       bool
}

// New a simple constructor to create a replication.client with postgres configurations.
func New(shutdownHandler shutdown.ShutdownHandler,
	statsChan chan stats.Stat,
	connManager conn.ManagerInterface,
	clientBufferSize int) Replicator {

	return Replicator{
		shutdownHandler:  shutdownHandler,
		connManager:      connManager,
		statsChan:        statsChan,
		overallProgress:  0,
		outputChan:       make(chan *replication.WalMessage, clientBufferSize),
		progressLastSent: int64(0),
		stoppedChan:      make(chan struct{}),

		// handlePrimaryKeepaliveMessage
		lastClientHeartbeatRequestTime: time.Now(),
		heartbeatRequestCounter:        0,
		heartbeatRequestDeltaTime:      0,

		// handleXLogData
		firstIteration: true,
		sawCommit:      false,
	}
}

// shutdown idempotently closes the output channels and cancels the termination context
func (c *Replicator) shutdown() {
	log.Info("shutting down")
	c.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	log.Debug("closing replication connection")
	c.connManager.Close()

	log.Debug("closing output channel")

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()
	close(c.outputChan)

	// Close stopped channel to signal stop
	close(c.stoppedChan)
}

// sendProgressStatus sends a StandbyStatus (heartbeat) to postgres which lets it know where it can trim off
// the wal replication slot, based on the overallProgress that this struct maintains.
func (c *Replicator) sendProgressStatus(ctx context.Context) error {
	var err error

	lsn := pglogrepl.LSN(c.overallProgress)
	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		ReplyRequested:   true,
	}

	var pgConn conn.Conn
	pgConn, err = c.connManager.GetConnWithStartLsn(ctx, c.highestWalStart)

	if err != nil {
		return err
	}

	err = pgConn.SendStandbyStatus(ctx, status)
	if err != nil {
		return err
	}

	// Only log out progress at most every logProgressInterval
	if time.Now().UnixNano()-logProgressInterval > c.progressLastSent {
		c.progressLastSent = time.Now().UnixNano()
		log.Infof("sent progress LSN: %s / %d",
			lsn,
			c.overallProgress)
	}

	return nil
}

// handleProgress read all progress off progress channel and send to Postgres
func (c *Replicator) handleProgress(force bool) error {

	var progressUpdated bool

	// Read all progress off the channel
	err := func() error {
		for {
			select {
			case latestProgress, ok := <-c.progressChan:
				if !ok {
					return errors.New("progress channel closed")
				}

				// Ensure new progress is older than the overall progress
				if c.overallProgress >= latestProgress {
					log.Warn("overall progress is newer than or same as latest reported progress, skipping")
					continue
				}

				// We're updating progress since what we got is newer
				if latestProgress > c.overallProgress {
					c.overallProgress = latestProgress
					progressUpdated = true
				}
			default:
				return nil
			}
		}
	}()

	// Progress channel was closed
	if err != nil {
		return err
	}

	// If we had updated the progress then send the new one, else if we want to
	// force send the progress (whether we updated or not) then do it. This ensures
	// we are always sending something to postgres (this functions as a
	// connection keepalive).
	if progressUpdated || force {
		// Check for shutdown state before sending progress.
		select {
		case <-c.shutdownHandler.TerminateCtx.Done():
			return errors.New("received terminateCtx cancellation")
		default:
		}

		// Send the progress
		if err := c.sendProgressStatus(c.shutdownHandler.TerminateCtx); err != nil {
			return err
		}
	}

	return nil
}

// Start loops on serially updating overallProgress from the progress channel, reading in a pglogrepl.XLogData
// from the API, and sending out converted replication.WalMessages on the output Channel. Additionally it sends back a
// StandbyStatus when requested from postgres.
// Start needs to be provided a progressChan because we need to initialize all the modules first to produce a
// progressChan needed for the replication.client
func (c *Replicator) Start(progressChan <-chan uint64) {
	defer c.shutdown()

	log.Info("Starting")
	c.progressChan = progressChan

	var pgConn conn.Conn
	var message pgproto3.BackendMessage
	var rplErr error

	// Get a connection
	pgConn, rplErr = c.connManager.GetConnWithStartLsn(c.shutdownHandler.TerminateCtx, 0)
	if rplErr != nil {
		log.Error(rplErr.Error())
		return
	}

	// First replication message will be a ServerHeartbeat which contains
	// the current wal end location. This is our starting location.
	func() {
		replicationCtx, cancelFn := context.WithTimeout(c.shutdownHandler.TerminateCtx, 5*time.Second)
		defer cancelFn()

		message, rplErr = pgConn.ReceiveMessage(replicationCtx)
	}()

	cd, ok := message.(*pgproto3.CopyData)
	if !ok {
		log.Errorf("Received unexpected message: %#v", message)
		return
	}

	if cd.Data[0] != pglogrepl.PrimaryKeepaliveMessageByteID {
		log.Error("server did not send a heartbeat as the first message")
		return
	}

	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
	if err != nil {
		log.WithError(err).Error("unable to parse keepalive")
	}

	// Set overall status to that of server's location
	log.Infof("replication slot LSN: %s / %d",
		pkm.ServerWALEnd,
		uint64(pkm.ServerWALEnd))
	c.overallProgress = uint64(pkm.ServerWALEnd)

	// Ticker to force update of progress
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Loop reading replication messages
	for {
		// Check to see if a termination is initiated
		select {
		case <-c.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
		}

		// If the ticker ticked then do a force progress send. This way we are always
		// reporting progress regardless of downstream.
		forceProgress := false
		select {
		case <-ticker.C:
			forceProgress = true
		default:
			forceProgress = false
		}

		// Check progress channel and only send an update if there is one
		if err := c.handleProgress(forceProgress); err != nil {
			log.Error(err)
			return
		}

		// Get a connection
		pgConn, rplErr = c.connManager.GetConnWithStartLsn(c.shutdownHandler.TerminateCtx, c.highestWalStart)
		if rplErr != nil {
			log.WithError(rplErr).Error("fatal")
			return
		}

		// Setup a timeout and then read from replication slot on server
		func() {
			replicationCtx, cancelFn := context.WithTimeout(c.shutdownHandler.TerminateCtx, 5*time.Second)
			defer cancelFn()

			message, rplErr = pgConn.ReceiveMessage(replicationCtx)
		}()

		// Check for error and connection status
		if rplErr != nil {
			if pgconn.Timeout(rplErr) || errors.Is(rplErr, context.DeadlineExceeded) {
				if err := c.handleProgress(true); err != nil {
					log.Error(err)
					return
				}

				continue
			}

			log.WithError(rplErr).Warn("handling replication error")

			// TODO(#8): what about other types of errors?
			if pgConn.IsClosed() {
				log.Warn("connection was closed")
				continue
			}

			return
		}

		// Begin reading message
		if message == nil {
			log.Debug("message was nil")
			continue
		}

		// Handle different types of messages
		switch t := message.(type) {
		case *pgproto3.CopyData:
			switch t.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				err = c.handlePrimaryKeepaliveMessage(t.Data[1:])
			case pglogrepl.XLogDataByteID:
				err = c.handleXLogData(t.Data[1:])
			default:
				continue
			}
		case *pgproto3.ParameterStatus:
			continue
		case *pgproto3.ParameterDescription:
			continue
		case *pgproto3.ErrorResponse:
			log.Errorf("Attempting to recover after error: %#v", t)

			err := c.recoverFromErrorResponse()
			if err != nil {
				log.WithError(err).Error("unable to recover...")
				return
			}

			continue
		default:
			log.Errorf("Received unexpected message: %#v", message)
			return
		}

		if err != nil {
			log.WithError(err).Error("fatal")
			return
		}
	}
}

// recoverFromErrorResponse attempts to recover from fatal replication errors by
// advancing the replication to the current LSN the server is on. Note that this
// will cause a gap in data if new data has come in after the replication error
// but before we've had a chance to recover.
func (c *Replicator) recoverFromErrorResponse() error {
	// Emitting fake COMMIT wal message to close out this transaction so
	// that there are no gaps in data from the ledger's perspective because
	// at this point we have already seen a BEGIN before the corrupt data.
	log.Warnf("Emitting fake COMMIT wal message to close out transaction %s", c.transaction)
	pr := parselogical.ParseResult{Transaction: c.transaction, Operation: "COMMIT"}
	msg := replication.WalMessage{Pr: &pr, TimeBasedKey: c.timeBasedKey, ServerWalEnd: c.highestWalStart, WalStart: c.highestWalStart}
	c.outputChan <- &msg

	// Close the old connection first because it's broken at this point
	c.connManager.Close()

	// Request a new connection that will be used just during recovery
	tmpConn, err := c.connManager.GetConn(c.shutdownHandler.TerminateCtx)

	if err != nil {
		return err
	}

	// Calling IdentifySystem tells us what the LSN the server is on
	sysident, err := tmpConn.IdentifySystem(c.shutdownHandler.TerminateCtx)
	if err != nil {
		return err
	}

	log.Warnf("Advancing WAL location from %s to %s",
		pglogrepl.LSN(c.highestWalStart),
		sysident.XLogPos)

	c.highestWalStart = uint64(sysident.XLogPos)
	c.sawCommit = false
	c.firstIteration = true

	// It's critical to close the connection provided by the connection manager because
	// we don't want to have it re-used by the replication portion of the client.
	c.connManager.Close()

	return nil
}

func (c *Replicator) handlePrimaryKeepaliveMessage(data []byte) error {
	// Handle ServerHeartbeat and send keepalive
	log.Debug("server sent a heartbeat")

	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		log.WithError(err).Error("unable to parse keepalive")
		return err
	}

	// If server isn't asking for a reply it's likely sending us a heartbeat
	// that we requested. In this case we can ignore any heartbeats that
	// don't ask for a reply.
	if !pkm.ReplyRequested {
		return nil
	}

	log.Debug("server asked for a heartbeat")
	if err := c.handleProgress(true); err != nil {
		return err
	}

	// Track when the last requested client heartbeat from server was. If the server
	// asks for heartbeats rapidly assume this is a request to shutdown.
	//
	// Shutdown if server asks for heartbeat more than 5 times with less than 100ms
	// between all requests.
	now := time.Now()
	c.heartbeatRequestDeltaTime += now.Sub(c.lastClientHeartbeatRequestTime)
	c.heartbeatRequestCounter++

	if c.heartbeatRequestDeltaTime < time.Millisecond*100 && c.heartbeatRequestCounter > 5 {
		return errors.New("Server asked for heartbeat rapidly, assuming request to shutdown...")
	}

	if c.heartbeatRequestCounter > 5 {
		c.heartbeatRequestCounter = 0
		c.heartbeatRequestDeltaTime = 0
	}
	c.lastClientHeartbeatRequestTime = now

	return nil
}

func (c *Replicator) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		log.Error(err)
		c.statsChan <- stats.NewStatCount("replication", "invalid_msg", 1, time.Now().UnixNano())
		return nil
	}

	wal, err := replication.XLogDataToWalMessage(xld)
	if err != nil {
		c.statsChan <- stats.NewStatCount("replication", "invalid_msg", 1, time.Now().UnixNano())
		return err
	}

	// Keep track of the latest seen CommitWalStart of the COMMITs. This should be increasing
	// but in the case postgres re-sends data this will tell us number of duplicate
	// transactions it has sent.
	if wal.Pr.Operation == "COMMIT" {
		c.statsChan <- stats.NewStatCount("replication", "txns", 1, time.Now().UnixNano())
		if c.highestWalStart < wal.WalStart {
			c.highestWalStart = wal.WalStart
		} else {
			c.statsChan <- stats.NewStatCount("replication", "txns_dup", 1, time.Now().UnixNano())
		}

		c.sawCommit = true
	}

	// It's possible that we can see the same transaction (even partially) twice from postgres.
	//
	// Transaction ids are only sent on BEGIN/COMMIT. Here we keep track of the
	// latest BEGIN and set the transaction id for the subsequent messages to
	// that of the BEGIN.
	//
	// Additionally, we stamp a timeBasedKey on all messages in any given transaction
	// from the nanosecond time of the BEGIN so that we can identify temporally different
	// instances of the same transaction. This allows us to see later down the line whether
	// we are receiving the same transaction again so that we can update our ledger to the
	// latest instance of the transaction, and ignore the ledger entries from the old instance.
	if wal.Pr.Operation == "BEGIN" {
		// Update the transaction marker
		c.transaction = wal.Pr.Transaction

		// Update the time marker
		var strs []string
		strs = append(strs, c.transaction)
		strs = append(strs, "-")
		strs = append(strs, strconv.FormatInt(time.Now().UnixNano(), 10))
		c.timeBasedKey = strings.Join(strs, "")

		// If we are at a BEGIN ensure that a COMMIT was seen. This ensures that we never
		// progress reading until a transaction is fully "closed" on our end by having seen
		// both BEGIN and COMMIT.
		if !c.sawCommit && !c.firstIteration {
			log.Errorf("Saw a BEGIN but no associated commit. Highest COMMIT lsn seen was %s / %d",
				pglogrepl.LSN(c.highestWalStart),
				c.highestWalStart)

			// Closing the connection will make postgres resend everything that hs not been
			// acknowledged.
			c.connManager.Close()

			c.sawCommit = false
			c.firstIteration = true
			return nil
		}

		// Reset state to look for next commit
		c.sawCommit = false

		// After the first begin is seen then we are no longer on the first iteration
		c.firstIteration = false
	}

	wal.Pr.Transaction = c.transaction
	wal.TimeBasedKey = c.timeBasedKey

	// Attempt to write to channel. If full then send a keepalive and attempt
	// to write to channel again.
	err = func() error {
		var curSleep = initialSleep

		for {
			select {
			case c.outputChan <- wal:
				// Break out to read the next message
				return nil
			default:
				// Send keepalive if channel is full
				if err := c.handleProgress(true); err != nil {
					// propagate error
					return err
				}

				if curSleep > maxSleep {
					curSleep = initialSleep
				}

				// Sleep here to prevent spinning.
				time.Sleep(curSleep)
				curSleep = curSleep * 2
			}
		}
	}()

	if err != nil {
		return err
	}

	c.statsChan <- stats.NewStatCount("replication", "received", 1, time.Now().UnixNano())
	return nil
}

// GetOutputChan returns the outputChan
func (c *Replicator) GetOutputChan() chan *replication.WalMessage {
	return c.outputChan
}

// GetStoppedChan returns stoppedChan
func (c *Replicator) GetStoppedChan() chan struct{} {
	return c.stoppedChan
}
