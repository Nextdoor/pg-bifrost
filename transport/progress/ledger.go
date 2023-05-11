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
	"fmt"

	"github.com/cevaris/ordered_map"
)

// LedgerEntry are used by ledger to keep track of the current count of wal messages per
// transaction that have been both seen and written.
// It uses WalTuple to back-reference to the transactionToTimeBasedKey.
type LedgerEntry struct {
	Transaction    string // transaction id of the transaction
	TimeBasedKey   string // time based key of the transaction (composite key of txn id and time)
	CommitWalStart uint64 // WalStart of the commit
	Count          int    // number of messages written (number of messages that were in the batch for this transaction)
	TotalMsgs      int    // total number of messages in transaction
}

// Ledger keeps track of ledger entries in order. When any given ledger entry count is 0 and
// it has a non-nil CommitWalStart, the WalsStart is emitted and the entry is cleared.
type Ledger struct {
	items                     *ordered_map.OrderedMap // string => LedgerEntry
	transactionToTimeBasedKey map[string]string
}

func NewLedger() Ledger {
	omap := ordered_map.NewOrderedMap()
	t := map[string]string{}
	return Ledger{omap, t}
}

func (l *Ledger) updateSeen(seen *Seen) error {
	if val, ok := l.transactionToTimeBasedKey[seen.Transaction]; ok {
		if val != seen.TimeBasedKey {
			// If you see a progress again with the same transaction but different timeBasedKey,
			// its outdated and must be deleted as there is a new temporally unique instance of
			// a transaction to keep track of.
			l.items.Delete(val)

			// We also need to delete it from our transactionToTimeBasedKey because the default
			// logic below will re-add it.
			delete(l.transactionToTimeBasedKey, seen.Transaction)
		}
	}

	// Add or update the ledger
	if val, ok := l.items.Get(seen.TimeBasedKey); !ok {
		// Transaction not in ledger (writes haven't happened yet)
		entry := LedgerEntry{
			seen.Transaction,
			seen.TimeBasedKey,
			seen.CommitWalStart,
			0,
			seen.TotalMsgs}

		// Create new ledger entry
		l.items.Set(seen.TimeBasedKey, &entry)

		// Update our transaction helper-ledger
		l.transactionToTimeBasedKey[seen.Transaction] = seen.TimeBasedKey
	} else {
		// Transaction is in ledger (writes did happen)

		// Update ledger entry with the total count and commit LSN
		ledgerEntry := val.(*LedgerEntry)

		// Sanity check. Don't allow overwriting ledger entries. Duplicates should have been removed above.
		if ledgerEntry.CommitWalStart != 0 {
			return fmt.Errorf("transaction ID %s CommitWalStart was not 0", seen.TimeBasedKey)
		}

		ledgerEntry.TotalMsgs = seen.TotalMsgs
		ledgerEntry.CommitWalStart = seen.CommitWalStart
	}

	return nil
}

func (l *Ledger) updateWritten(written *Written) error {

	// check to see if transaction is in the ledger and verify time based key matches
	if val, ok := l.transactionToTimeBasedKey[written.Transaction]; ok {

		// if time based key does not match then remove from ledger
		if val != written.TimeBasedKey {
			// If you see a progress again with the same transaction but different timeBasedKey,
			// its outdated and must be deleted as there is a new temporally unique instance of
			// a transaction to keep track of.
			l.items.Delete(val)

			// We also need to delete it from our transactionToTimeBasedKey because the default
			// logic below will re-add it.
			delete(l.transactionToTimeBasedKey, written.Transaction)
		}
	}

	// get ledger entry from the supplied timeBasedKey
	if val, ok := l.items.Get(written.TimeBasedKey); !ok {
		// Could've been removed but it was an outdated transaction that was deleted

		entry := LedgerEntry{
			written.Transaction,
			written.TimeBasedKey,
			0,
			written.Count,
			0,
		}
		// Create a new ledger entry
		l.items.Set(written.TimeBasedKey, &entry)

		// Update our transaction helper-ledger
		l.transactionToTimeBasedKey[written.Transaction] = written.TimeBasedKey

	} else {
		// Add count to ledger
		ledgerEntry := val.(*LedgerEntry)

		ledgerEntry.Count += written.Count
	}

	return nil
}

// remove deletes a transaction from our whole ledger
func (l *Ledger) remove(timeBasedKey string) {
	val, ok := l.items.Get(timeBasedKey)
	if !ok {
		// Did not find an outdated tranaction, passing...
		return
	}

	ledgerEntry := val.(*LedgerEntry)

	delete(l.transactionToTimeBasedKey, ledgerEntry.Transaction)

	l.items.Delete(timeBasedKey)
}

// ledgerChanIter returns a simple iterator for our ledger
func (l *Ledger) ledgerChanIter() <-chan *ordered_map.KVPair {
	// TODO(#4): Iter() method is deprecated!. Use IterFunc() instead.
	return l.items.Iter()
}
