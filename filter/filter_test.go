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

package filter

import (
	"github.com/Nextdoor/pg-bifrost.git/replication"

	"testing"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/parselogical"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/stretchr/testify/assert"
)

func _testMessage(relation string, operation string) *replication.WalMessage {
	pr := parselogical.ParseResult{
		State:       parselogical.ParseState{},
		Transaction: "0",
		Relation:    relation,
		Operation:   operation,
		NoTupleData: false,
		Columns:     nil,
		OldColumns:  nil,
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

func _setupFilter(whitelist, regex bool, tablelist []string) (Filter, chan *replication.WalMessage, chan error, chan stats.Stat) {
	sh := shutdown.NewShutdownHandler()
	in := make(chan *replication.WalMessage)
	errorChan := make(chan error)
	statsChan := make(chan stats.Stat, 1000)

	f := New(sh, in, statsChan, whitelist, regex, tablelist)

	return f, in, errorChan, statsChan
}

func _collectOutput(outputChan chan *replication.WalMessage) []*replication.WalMessage {
	actual := make([]*replication.WalMessage, 0)

	func() {
		for {
			select {
			case <-time.After(5 * time.Millisecond):
				// Got all the stats
				return
			case m := <-outputChan:
				actual = append(actual, m)
			}
		}
	}()

	return actual
}

func TestWhitelist(t *testing.T) {
	// settings
	whitelist := true
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	msgBar := _testMessage("bar", "INSERT")
	in <- msgBar
	msgFoo := _testMessage("foo", "INSERT")
	in <- msgFoo

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{msgFoo}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "filtered", 1, time.Now().UnixNano()),
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestWhitelistRegex(t *testing.T) {
	// settings
	whitelist := true
	regex := true
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	msgBar := _testMessage("bar", "INSERT")
	in <- msgBar
	msgFoo := _testMessage("foo-1234", "INSERT")
	in <- msgFoo

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{msgFoo}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "filtered", 1, time.Now().UnixNano()),
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestBlacklist(t *testing.T) {
	// settings
	whitelist := false
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	msgFoo := _testMessage("foo", "INSERT")
	in <- msgFoo
	msgBar := _testMessage("bar", "INSERT")
	in <- msgBar

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{msgBar}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "filtered", 1, time.Now().UnixNano()),
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano()),
	}
	stats.VerifyStats(t, s, expectedStats)
}

func TestBlacklistRegex(t *testing.T) {
	// settings
	whitelist := false
	regex := true
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	msgFoo := _testMessage("foo-12342", "INSERT")
	in <- msgFoo
	msgBar := _testMessage("bar", "INSERT")
	in <- msgBar

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{msgBar}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "filtered", 1, time.Now().UnixNano()),
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano()),
	}
	stats.VerifyStats(t, s, expectedStats)
}
func TestBlacklistPassThrough(t *testing.T) {
	// settings
	whitelist := false
	regex := false

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, []string{})

	go f.Start()

	msgFoo := _testMessage("foo", "INSERT")
	in <- msgFoo

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{msgFoo}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestBeginPass(t *testing.T) {
	// settings
	whitelist := false
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	begin := _testMessage("bar", "BEGIN")
	in <- begin

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{begin}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestCommitPass(t *testing.T) {
	// settings
	whitelist := false
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, s := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	commit := _testMessage("bar", "COMMIT")
	in <- commit

	actual := _collectOutput(f.OutputChan)
	expected := []*replication.WalMessage{commit}

	assert.Equal(t, expected, actual)

	expectedStats := []stats.Stat{
		stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, s, expectedStats)
}

func TestInputChannelClose(t *testing.T) {
	// Setup test
	f, in, _, _ := _setupFilter(true, false, []string{})
	go f.Start()

	// Close input channel
	close(in)

	_, ok := <-f.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}

	_, ok = <-f.shutdownHandler.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestNilMessage(t *testing.T) {
	// settings
	whitelist := false
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, _ := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	in <- nil

	_, ok := <-f.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestTerminationContextInput(t *testing.T) {
	// settings
	whitelist := false
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, _, _, _ := _setupFilter(whitelist, regex, tablelist)

	go f.Start()
	time.Sleep(10 * time.Millisecond)
	f.shutdownHandler.CancelFunc()

	time.Sleep(10 * time.Millisecond)
	_, ok := <-f.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestTerminationContextOutput(t *testing.T) {
	// settings
	whitelist := true
	regex := false
	tablelist := []string{"foo"}

	// setup filter
	f, in, _, _ := _setupFilter(whitelist, regex, tablelist)

	go f.Start()

	msgFoo := _testMessage("foo", "INSERT")
	in <- msgFoo

	time.Sleep(10 * time.Millisecond)
	f.shutdownHandler.CancelFunc()

	time.Sleep(10 * time.Millisecond)
	_, ok := <-f.OutputChan
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}
