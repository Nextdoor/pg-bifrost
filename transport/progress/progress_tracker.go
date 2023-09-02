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

package progress

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/utils"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "progress_tracker")
)

const (
	outputChanSize = 1000
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Progress struct {
	WorkerNum int    // worker id
	WalStart  uint64 // wal offset of the latest _contiguous_ record processed by worker
}

type Seen struct {
	Transaction    string // transaction id of the transaction
	TimeBasedKey   string // time based key of the transaction (composite key of txn id and time)
	TotalMsgs      int    // total number of messages in transaction
	CommitWalStart uint64 // WalStart of the commit
}

type Written struct {
	Transaction  string // transaction id of the transaction
	TimeBasedKey string // time based key of the transaction (composite key of txn id and time)
	Count        int    // number of messages written (number of messages that were in the batch for this transaction)
}

type ProgressTracker struct {
	shutdownHandler shutdown.ShutdownHandler

	txnSeenChan <-chan []*Seen
	txnsWritten <-chan *ordered_map.OrderedMap // <transaction:progress.written>
	statsChan   chan stats.Stat
	OutputChan  chan uint64 // channel to send overall (youngest) progress on

	ledger Ledger // ordered map which contains batches seen & written per transaction

	stopChan chan struct{} // helper channel to stop mutating the ledger during tests
}

// New creates a progress "table" based on the number of workers (inputChans) and returns a ProgressTracker
// txnsSeen MUST be an unbuffered channel
func New(shutdownHandler shutdown.ShutdownHandler,
	txnSeenChan <-chan []*Seen,
	txnsWritten <-chan *ordered_map.OrderedMap,
	statsChan chan stats.Stat) ProgressTracker {

	outputChan := make(chan uint64, outputChanSize)
	stopChan := make(chan struct{})
	ledger := NewLedger()

	return ProgressTracker{shutdownHandler, txnSeenChan, txnsWritten, statsChan, outputChan, ledger, stopChan}
}

// shutdown idempotently closes the output channel
func (p ProgressTracker) shutdown() {
	log.Info("shutting down")
	p.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	log.Debug("closing output channel")
	close(p.OutputChan)
}

// updateSeen adds an entire transaction that was seen by the batcher to the ledger
func (p *ProgressTracker) updateSeen(seen []*Seen) error {
	for _, s := range seen {
		err := p.ledger.updateSeen(s)

		if err != nil {
			return err
		}
	}

	return nil
}

// updateSeen adds a written batch-transaction to the ledger as reported by the transporter
func (p *ProgressTracker) updateWritten(writtenMap *ordered_map.OrderedMap) error {
	batchTransactionsIter := writtenMap.IterFunc()
	for kv, ok := batchTransactionsIter(); ok; kv, ok = batchTransactionsIter() {
		written := kv.Value.(*Written)
		err := p.ledger.updateWritten(written)
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper struct to pack timeBasedKey and walStart together for easy deletion from the ledger later
type contiguousTuple struct {
	timeBasedKey string
	walStart     uint64
	transaction  string
}

// emitProgress sends on the ProgressTracker's output channels the latest contiguous CommitWalStart with all batches
// written and with a COMMIT sent.
func (p *ProgressTracker) emitProgress() {
	ledgerIter := p.ledger.items.IterFunc()
	contiguousTxnsWalStart := []contiguousTuple{}

	for kv, ok := ledgerIter(); ok; kv, ok = ledgerIter() {
		// Unpack
		timeBasedKey := kv.Key.(string)
		ledgerEntry := kv.Value.(*LedgerEntry)
		count := ledgerEntry.Count
		total := ledgerEntry.TotalMsgs
		walStart := ledgerEntry.CommitWalStart
		transaction := ledgerEntry.Transaction

		// When total and count match then the transaction has been fully written out
		if walStart != uint64(0) && count == total {
			// Store walStarts which can be acked on, and the timeBasedKey to know what to delete from the ledger
			contiguousTxnsWalStart = append(
				contiguousTxnsWalStart,
				contiguousTuple{timeBasedKey, walStart, transaction})
		} else {
			break
		}
	}

	// Get the latest contiguous walStart written and emit it as progress
	if len(contiguousTxnsWalStart) > 0 {
		latestWalWritten := contiguousTxnsWalStart[len(contiguousTxnsWalStart)-1].walStart

		p.OutputChan <- latestWalWritten

		// Delete the transactions written from the ledger
		for _, c := range contiguousTxnsWalStart {
			p.ledger.remove(c.timeBasedKey)
		}
	}
}

// readProgress reads everything from the txnSeenChan and txnsWritten channel and updates the ledger
func (p *ProgressTracker) readProgress(tickerDuration time.Duration) error {
	var deadline = time.After(tickerDuration)

ReadProgressLoop:
	for {
		select {
		case txnSeen, ok := <-p.txnSeenChan:
			// Update entries from the Batcher as "seen"
			if !ok {
				return errors.New("txnSeenChan input channel is closed")
			}

			err := p.updateSeen(txnSeen)
			if err != nil {
				panic(err.Error())
			}

			// Ensure we do check for deadline
			select {
			case <-deadline:
				break ReadProgressLoop
			default:
				continue
			}
		case batchTransactions, ok := <-p.txnsWritten:
			// Update entries from the Batcher as successfully written
			if !ok {
				return errors.New("txnsWritten input channel is closed")
			}

			err := p.updateWritten(batchTransactions)
			if err != nil {
				panic(err.Error())
			}

			// Ensure we do check for deadline
			select {
			case <-deadline:
				break ReadProgressLoop
			default:
				continue
			}
		case <-deadline:
			break ReadProgressLoop
		}
	}

	// Ensure we've read txnsWritten since the above loop does not guarantee
	// we do so since "select" is non-deterministic
	select {
	case batchTransactions, ok := <-p.txnsWritten:
		if !ok {
			return errors.New("txnsWritten input channel is closed")
		}

		err := p.updateWritten(batchTransactions)
		if err != nil {
			panic(err.Error())
		}
	default:
		break
	}

	return nil
}

// Start begins progress tracking which emits progress on a timer or otherwise updates the ledger
// when transactions ares seen from the batcher or successfully written out by the transporter
func (p *ProgressTracker) Start(tickerDuration time.Duration) {
	log.Info("starting")

	// Ticker for emitting progress
	var ticker time.Ticker
	if tickerDuration != 0 {
		ticker = *time.NewTicker(tickerDuration)
		defer ticker.Stop()
	} else {
		log.Warn("progress_tracker ticker is disabled!")
	}

	// Signal handler channel to dump out ledger
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGIO)

	defer p.shutdown()
	for {
		select {
		case <-p.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		case <-p.stopChan: // TODO(#2): is this needed now?
			log.Info("stopping")
			break
		case <-ticker.C:
			// Prioritize emitProgress if the timer has ticked
			log.Debug("ticker ticked in progress tracker")

			// Emit progress of the updated ledger
			initialLedgerSize := p.ledger.items.Len()
			p.emitProgress()
			finalLedgerSize := p.ledger.items.Len()

			p.statsChan <- stats.NewStatHistogram("progress_tracker", "ledger_size", int64(finalLedgerSize), time.Now().UnixNano(), "count")
			p.statsChan <- stats.NewStatHistogram("progress_tracker", "ledger_flushed", int64(initialLedgerSize-finalLedgerSize), time.Now().UnixNano(), "count")

		case sig := <-sigchan:
			log.Infof("Got a signal %d (%s)", sig, sig.String())

			if sig == syscall.SIGIO {
				// Dump out any ledger entries on SIGIO signal
				ledgerDump := utils.OrderedMapToStrings(p.ledger.items)

				if len(ledgerDump) > 0 {
					for _, entry := range ledgerDump {
						fmt.Println("entry: ", entry)
					}
				} else {
					log.Info("No ledger entries to dump")
				}
			}
		default:
			// Flush seen and written channel so the ledger is up to date before emitting
			if err := p.readProgress(tickerDuration); err != nil {
				log.Warn(err.Error())
				return
			}
		}
	}
}

// stop is a function only used in unit tests to stop mutating on the ledger before asserting on it's data
func (p ProgressTracker) stop() {
	p.stopChan <- struct{}{}
}
