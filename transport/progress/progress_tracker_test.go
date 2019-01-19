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
	"reflect"
	"testing"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetLevel(logrus.InfoLevel)
}

type testCase struct {
	action     string
	writtenMap *ordered_map.OrderedMap
	seen       *Seen
}

type testExpected struct {
	transaction    string
	timeBasedKey   string
	commitWalStart uint64
	count          int
	totalMsgs      int
}
type testExpecteds struct {
	progressWalStart uint64
	entries          []testExpected
}

func compareLedger(a Ledger, b Ledger) bool {
	if a.items.Len() != b.items.Len() {
		return false
	}

	iterA := a.items.IterFunc()

	for kv, ok := iterA(); ok; kv, ok = iterA() {
		key := kv.Key.(string)
		value := kv.Value.(*LedgerEntry)

		fmt.Println(value)

		if val, ok := b.items.Get(key); ok == true {
			if !reflect.DeepEqual(value, val) {
				// false if same key's value isnt the same
				return false
			}
		} else {
			// false if key doesn't exist in other omap
			return false
		}
	}

	return true
}

func getProgressTracker() (ProgressTracker, chan *Seen, chan *ordered_map.OrderedMap) {
	seen := make(chan *Seen)
	written := make(chan *ordered_map.OrderedMap, 1000)
	sh := shutdown.NewShutdownHandler()

	p := New(sh, seen, written)

	return p, seen, written
}

func testRunner(t *testing.T, cases []testCase, expecteds testExpecteds, tickerDuration time.Duration) {
	p, seenChan, writtenChan := getProgressTracker()

	go p.Start(tickerDuration)

	for _, c := range cases {

		if c.action == "seen" {
			seenChan <- c.seen
		}

		if c.action == "written" {
			writtenChan <- c.writtenMap
		}

		time.Sleep(25 * time.Millisecond)
	}

	expected := NewLedger()

	for _, e := range expecteds.entries {
		expectedEntry := LedgerEntry{
			e.transaction,
			e.timeBasedKey,
			e.commitWalStart,
			e.count,
			e.totalMsgs,
		}

		expected.items.Set(e.timeBasedKey, &expectedEntry)
	}

	p.stop()
	assert.Equal(t, true, compareLedger(expected, p.ledger), "The ledger should have the correct updates at the end.")

	if expecteds.progressWalStart == 0 {
		select {
		case _, ok := <-p.OutputChan:
			if !ok {
				assert.Fail(t, "Channel was unexpectedly closed.")
			} else {
				assert.Fail(t, "Received a progress but did not expect it.")
			}
		default:
		}
	} else {
		fmt.Println("before")
		resultProgress := <-p.OutputChan
		fmt.Println("after")
		assert.Equal(t, expecteds.progressWalStart, resultProgress, "The correct progress should be emitted.")
	}

	time.Sleep(20 * time.Millisecond)
	fmt.Println("END: ", p.ledger.items)
}

func TestSingleSeenEntry(t *testing.T) {
	omap := ordered_map.NewOrderedMap()

	//uint64(0), "1"
	omap.Set("1-1", &LedgerEntry{"1", "1-1", uint64(111), 0, 1})

	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 1, 111}},
	}
	entries := []testExpected{
		testExpected{"1","1-1", 111, 0,  1},
	}
	expecteds := testExpecteds{
		entries: entries,
	}

	testRunner(t, cases, expecteds, 0)
}


// TODO(#9): add a test case to panic if you see two COMMITs with the same timeBasedKey
//func TestDoubleSeenEntry(t *testing.T) {
//	omap1 := ordered_map.NewOrderedMap()
//	omap1.Set("1-1", WalTuple{uint64(0), "1"})
//	omap2 := ordered_map.NewOrderedMap()
//	omap2.Set("1-1", WalTuple{uint64(0), "1"})
//
//	cases := []testCase{
//		testCase{"seen", omap1},
//		testCase{"seen", omap2},
//	}
//	entries := []testExpected{
//		testExpected{"1-1", 2, 0, "1"},
//	}
//	expecteds := testExpecteds{
//		entries: entries,
//	}
//
//	testRunner(t, cases, expecteds, time.Millisecond*5)
//}

func TestTwoDistinctSeenEntry(t *testing.T) {
	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 1, 111}},
		testCase{"seen", nil, &Seen{"2", "2-1", 1, 222}},
	}
	entries := []testExpected{
		testExpected{"1","1-1", 111, 0,  1},
		testExpected{"2","2-1", 222, 0,  1},
	}
	expecteds := testExpecteds{
		entries: entries,
	}

	testRunner(t, cases, expecteds, 0)
}

func TestSingleSeenAndWrittenEntryWithoutTicker(t *testing.T) {
	omap2 := ordered_map.NewOrderedMap()
	omap2.Set("1-1", &Written{"1", "1-1", 1})

	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 1, 111}},
		testCase{"written", omap2, nil},
	}

	entries := []testExpected{
		testExpected{"1", "1-1", 111, 1, 1},
	}
	expecteds := testExpecteds{
		entries: entries,
	}

	testRunner(t, cases, expecteds, 0)
}

func TestSingleSeenAndWrittenEmitted(t *testing.T) {
	omap2 := ordered_map.NewOrderedMap()
	omap2.Set("1-1", &Written{"1", "1-1", 1})

	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 1, 999}},
		testCase{"written", omap2, nil},
	}

	expecteds := testExpecteds{
		progressWalStart: 999,
	}

	testRunner(t, cases, expecteds, time.Millisecond*5)
}

func TestMultipleSeenAndWrittenEmitted(t *testing.T) {
	omap1 := ordered_map.NewOrderedMap()
	omap1.Set("1-1", &Written{"1", "1-1", 1})
	omap1.Set("2-1", &Written{"2", "2-1", 1})
	omap2 := ordered_map.NewOrderedMap()
	omap2.Set("1-1", &Written{"1", "1-1", 1})
	omap3 := ordered_map.NewOrderedMap()
	omap3.Set("1-1", &Written{"1", "1-1", 1})
	omap4 := ordered_map.NewOrderedMap()
	omap4.Set("1-1", &Written{"1", "1-1", 2})

	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 5, 888}},
		testCase{"seen", nil, &Seen{"2", "2-1", 2, 999}},
		testCase{"written", omap1, nil},
		testCase{"written", omap2, nil},
		testCase{"written", omap3, nil},
		testCase{"written", omap4, nil},
	}
	entries := []testExpected{
		testExpected{"2", "2-1", 999, 1, 2},
	}
	expecteds := testExpecteds{
		progressWalStart: 888,
		entries:          entries,
	}

	testRunner(t, cases, expecteds, time.Millisecond*5)
}

func TestSeenAndSeenAgain(t *testing.T) {
	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 1, 999}},
		testCase{"seen", nil, &Seen{"1", "1-2", 1, 999}},
	}
	entries := []testExpected{
		testExpected{"1", "1-2", 999, 0, 1},
	}
	expecteds := testExpecteds{
		entries: entries,
	}

	testRunner(t, cases, expecteds, time.Millisecond*5)
}

func TestSeenAndSeenAgainThenWritten(t *testing.T) {
	omap1 := ordered_map.NewOrderedMap()
	omap1.Set("1-1", &Written{"1", "1-1", 5})

	omap2 := ordered_map.NewOrderedMap()
	omap2.Set("1-2", &Written{"1", "1-2", 2})

	omap3 := ordered_map.NewOrderedMap()
	omap3.Set("1-2", &Written{"1", "1-2", 5})

	omap4 := ordered_map.NewOrderedMap()
	omap4.Set("1-2", &Written{"1", "1-2", 3})

	cases := []testCase{
		testCase{"seen", nil, &Seen{"1", "1-1", 10, 999}},
		testCase{"written", omap1, nil},

		testCase{"seen", nil, &Seen{"1", "1-2", 10, 999}},
		testCase{"written", omap2, nil},
		testCase{"written", omap3, nil},
		testCase{"written", omap4, nil},
	}

	expecteds := testExpecteds{
		progressWalStart: 999,
		entries:          []testExpected{},
	}

	testRunner(t, cases, expecteds, time.Millisecond*5)
}

func TestCloseInputChannel(t *testing.T) {
	p, seen, written := getProgressTracker()

	go p.Start(time.Millisecond * 5)

	// Close input channels
	close(seen)
	close(written)

	// Verify output channel gets closed
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-p.OutputChan:
		if ok {
			assert.Fail(t, "Output channel not closed")
		}
	case <-timeout.C:
		assert.Fail(t, "Output channel not closed")
	}

	_, ok2 := <-p.shutdownHandler.TerminateCtx.Done()
	if ok2 {
		assert.Fail(t, "context not cancelled")
	}
}

func TestDualShutdown(t *testing.T) {
	p, seen, written := getProgressTracker()

	go p.Start(time.Millisecond * 5)

	// Close input channels
	close(seen)
	close(written)

	// Give time for ProgressTracker to handle shutdown
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case <-timeout.C:
	}

	// Catch panic if it does happen
	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "shutdown probably caused panic", r)
		}
	}()

	// should not panic
	p.shutdown()
}

func TestTerminationHandling(t *testing.T) {
	p, _, _ := getProgressTracker()

	go p.Start(time.Millisecond * 5)

	p.shutdownHandler.CancelFunc()

	// Verify output channel gets closed
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-p.OutputChan:
		if ok {
			assert.Fail(t, "Output channel not closed")
		}
	case <-timeout.C:
		assert.Fail(t, "Output channel not closed")
	}
}

func TestTxnsSeenClosed(t *testing.T) {
	p, seen, _ := getProgressTracker()

	go p.Start(time.Millisecond * 5)

	close(seen)

	// Verify output channel gets closed
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-p.OutputChan:
		if ok {
			assert.Fail(t, "Output channel not closed")
		}
	case <-timeout.C:
		assert.Fail(t, "Output channel not closed")
	}
}
