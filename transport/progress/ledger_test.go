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
	"github.com/stretchr/testify/assert"
	"testing"
)

func _compareLedgerToSeen(seen *Seen, ledger Ledger, t *testing.T) {
	le, ok := ledger.items.Get(seen.TimeBasedKey)
	if !ok {
		assert.Fail(t, "seen time based key not in ledger", seen.TimeBasedKey)
	}

	ledgerEntry := le.(*LedgerEntry)

	assert.Equal(t, seen.Transaction, ledgerEntry.Transaction)
	assert.Equal(t, seen.TimeBasedKey, ledgerEntry.TimeBasedKey)
	assert.Equal(t, seen.CommitWalStart, ledgerEntry.CommitWalStart)
	assert.Equal(t, seen.TotalMsgs, ledgerEntry.TotalMsgs)
}

func _compareLedgerToWritten(written *Written, ledger Ledger, t *testing.T, expectedCount int) {
	le, ok := ledger.items.Get(written.TimeBasedKey)
	if !ok {
		assert.Fail(t, "written time based key not in ledger", written.TimeBasedKey)
	}

	ledgerEntry := le.(*LedgerEntry)

	assert.Equal(t, written.Transaction, ledgerEntry.Transaction)
	assert.Equal(t, written.TimeBasedKey, ledgerEntry.TimeBasedKey)
	assert.Equal(t, expectedCount, ledgerEntry.Count)
}

func TestUpdateSeenDifferentTimeBasedKey(t *testing.T) {
	l := NewLedger()
	var err error

	seenFirst := Seen{"1", "1-1", 1, 111}
	seenSecond := Seen{"1", "1-2", 4, 111}

	err = l.updateSeen(&seenFirst)
	if err != nil {
		assert.Fail(t, "error adding seen", err)
	}

	err = l.updateSeen(&seenSecond)
	if err != nil {
		assert.Fail(t, "error adding seen", err)
	}

	// Verify first seen is not in the ledger
	if _, ok := l.items.Get(seenFirst.TimeBasedKey); ok {
		assert.Fail(t, "not expecting first time based key to be in ledger ", seenFirst.TimeBasedKey)
	}

	// Verify second seen is in the ledger
	if _, ok := l.items.Get(seenSecond.TimeBasedKey); !ok {
		assert.Fail(t, "expecting second time based key to be in ledger ", seenSecond.TimeBasedKey)
	}

	if val, ok := l.transactionToTimeBasedKey[seenFirst.Transaction]; ok {
		assert.Equal(t, seenSecond.TimeBasedKey, val)
	} else {
		assert.Fail(t, "expecting second time based key to be in transactionToTimeBasedKey ", seenSecond.TimeBasedKey)
	}

	// Verify nothing else is in the ledger
	assert.Equal(t, 1, l.items.Len())
	assert.Equal(t, 1, len(l.transactionToTimeBasedKey))

	_compareLedgerToSeen(&seenSecond, l, t)
}

func TestUpdateSeenDuplicateTimeBasedKey(t *testing.T) {
	l := NewLedger()
	var err error

	seenFirst := Seen{"1", "1-1", 1, 111}
	seenSecond := Seen{"1", "1-1", 1, 111}

	err = l.updateSeen(&seenFirst)
	assert.Equal(t, nil, err)

	err = l.updateSeen(&seenSecond)
	if err != nil {
		assert.Error(t, err, "Transaction ID 1-1 CommitWalStart was not 0")
	}
}

func TestUpdateSeenAfterWritten(t *testing.T) {
	l := NewLedger()
	var err error

	written := Written{"1", "1-1", 5}
	seen := Seen{"1", "1-1", 1, 111}

	err = l.updateSeen(&seen)
	assert.Equal(t, nil, err)

	err = l.updateWritten(&written)
	assert.Equal(t, nil, err)

	// Verify contents of ledger
	assert.Equal(t, 1, l.items.Len())
	assert.Equal(t, 1, len(l.transactionToTimeBasedKey))

	_compareLedgerToSeen(&seen, l, t)
	_compareLedgerToWritten(&written, l, t, 5)
}

func TestUpdateWritten(t *testing.T) {
	l := NewLedger()
	var err error

	written := Written{"1", "1-1", 5}

	err = l.updateWritten(&written)
	assert.Equal(t, nil, err)

	// Verify contents of ledger
	assert.Equal(t, 1, l.items.Len())
	assert.Equal(t, 1, len(l.transactionToTimeBasedKey))

	_compareLedgerToWritten(&written, l, t, 5)
}

func TestUpdateWrittenTwice(t *testing.T) {
	l := NewLedger()
	var err error

	writtenFirst := Written{"1", "1-1", 5}
	writtenSecond := Written{"1", "1-1", 4}

	err = l.updateWritten(&writtenFirst)
	assert.Equal(t, nil, err)

	err = l.updateWritten(&writtenSecond)
	assert.Equal(t, nil, err)

	// Verify contents of ledger
	assert.Equal(t, 1, l.items.Len())
	assert.Equal(t, 1, len(l.transactionToTimeBasedKey))

	_compareLedgerToWritten(&writtenSecond, l, t, 9)
}

func TestUpdateWrittenDifferentTimeBasedKey(t *testing.T) {
	l := NewLedger()
	var err error

	writtenFirst := Written{"1", "1-1", 5}
	writtenSecond := Written{"1", "1-2", 8}

	err = l.updateWritten(&writtenFirst)
	assert.Equal(t, nil, err)

	err = l.updateWritten(&writtenSecond)
	assert.Equal(t, nil, err)

	// Verify contents of ledger
	assert.Equal(t, 1, l.items.Len())
	assert.Equal(t, 1, len(l.transactionToTimeBasedKey))

	_compareLedgerToWritten(&writtenSecond, l, t, 8)
}

func TestRemoveMissing(t *testing.T) {
	l := NewLedger()
	l.remove("foo")
}

func TestRemove(t *testing.T) {
	l := NewLedger()
	var err error

	written := Written{"1", "1-1", 5}

	err = l.updateWritten(&written)
	assert.Equal(t, nil, err)

	l.remove(written.TimeBasedKey)

	// Verify contents of ledger
	assert.Equal(t, 0, l.items.Len())
	assert.Equal(t, 0, len(l.transactionToTimeBasedKey))
}

func TestLedgerChanIter(t *testing.T) {
	l := NewLedger()
	var err error

	written := Written{"1", "1-1", 5}
	seen := Seen{"1", "1-1", 6, 111}

	err = l.updateWritten(&written)
	assert.Equal(t, nil, err)

	err = l.updateSeen(&seen)
	assert.Equal(t, nil, err)

	item := <-l.ledgerChanIter()
	val := item.Value
	entry := val.(*LedgerEntry)

	expected := LedgerEntry{"1", "1-1", 111, 5, 6}
	assert.Equal(t, "1-1", item.Key)
	assert.Equal(t, &expected, entry)
}
