package aggregator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Test case struct that our test function will iterate over.
type testStat struct {
	statName  string
	statType  stats.StatType
	timeSent  int64
	timestamp int64
	value     int64
	unit      string
}

type testResult struct {
	statName  string
	timestamp int64
	value     int64
}

// currentTime is a time object that we can override in our test cases.
var (
	currentTime   time.Time
	muCurrentTime = &sync.Mutex{}
)

func _resetCurrentTime() {
	muCurrentTime.Lock()
	defer muCurrentTime.Unlock()
	currentTime = time.Now()
}

func _updateNowTo(t int64, n int64) {
	muCurrentTime.Lock()
	defer muCurrentTime.Unlock()
	currentTime = time.Unix(t, n)
}

func init() {
	logger.SetLevel(logrus.InfoLevel)
}

func makeKey(stat stats.Stat) string {
	return fmt.Sprintf("%v", stat)
}

func _testStart(t *testing.T, sts []testStat, expecteds []testResult) {
	// Setup
	_resetCurrentTime()
	defer _resetCurrentTime()

	now := func() time.Time {
		muCurrentTime.Lock()
		defer muCurrentTime.Unlock()
		return currentTime
	}

	in := make(chan stats.Stat, 1000)
	out := make(chan stats.Stat, 1000)
	aggregates := map[int64]map[string]*aggregate{}

	sh := shutdown.NewShutdownHandler()
	statsAggregator := NewConfigureAggregates(sh, &sync.Mutex{}, in, out, now, int64(time.Second*10), 10, aggregates)

	go statsAggregator.Start()

	// Run
	for _, input := range sts {
		_updateNowTo(input.timeSent, 0)

		var data stats.Stat

		switch input.statType {
		case stats.Count:
			data = stats.NewStatCount("rplclient",
				input.statName,
				input.value,
				input.timestamp)
		case stats.Histogram:
			data = stats.NewStatHistogram("rplclient",
				input.statName,
				input.value,
				input.timestamp,
				input.unit)
		default:
			panic("unrecognized statType")
		}

		in <- data

		// The sleep in this test are needed so that processStatsMessagesWorker has time to read in
		// inputs before fast-forwarding the time to read off the out channel input, in order.
		time.Sleep(250 * time.Millisecond)
	}

	// Test
	// Move time forward
	_updateNowTo(946684800, 0)

	// Gather the results from the outbound channel
	results := map[string]stats.Stat{}
	for i := 0; i < len(expecteds); i++ {
		result := <-out
		results[makeKey(result)] = result
	}

	// Compare the respective result and expected elements
	for _, expected := range expecteds {
		var expectedStat stats.Stat
		expectedStat = stats.NewStatCount("rplclient",
			expected.statName,
			expected.value,
			expected.timestamp,
		)

		assert.Equal(t, expectedStat, results[makeKey(expectedStat)], "The two stats should be the same.")
	}
}

func TestStartWithSingleStat(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(1), 1, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value}, // TODO should stats[0].timestamp for 1 be 0?
	}

	_testStart(t, sts, expecteds)
}

func TestStartWithDoubleStatsSameBucket(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(time.Millisecond), 1, ""},
		{"foo", stats.Count, 11, int64(time.Millisecond * 9999), 2, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value + sts[1].value},
	}

	_testStart(t, sts, expecteds)
}

func TestStartWithDoubleStatsDiffBucket(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(time.Millisecond), 1, ""},
		{"foo", stats.Count, 15, int64(time.Millisecond * 14999), 2, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value},
	}

	_testStart(t, sts, expecteds)
}

func TestStartWithSingleStatSecondBucket(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(time.Millisecond), 1, ""},
		{"foo", stats.Count, 19, int64(time.Millisecond * 19000), 2, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value},
		{"foo", int64(10 * time.Second), sts[1].value},
	}

	_testStart(t, sts, expecteds)
}

func TestStartWithDoubleStatSecondBucket(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(time.Millisecond), 1, ""},
		{"foo", stats.Count, 15, int64(time.Millisecond * 14000), 2, ""},
		{"foo", stats.Count, 16, int64(time.Millisecond * 15000), 3, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value},
		{"foo", int64(10 * time.Second), sts[1].value + sts[2].value},
	}

	_testStart(t, sts, expecteds)
}

func TestStartWithDroppedStat(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 1, int64(time.Millisecond), 1, ""},
		{"foo", stats.Count, 15, int64(time.Millisecond * 9999), 2, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value},
	}

	_testStart(t, sts, expecteds)
}

func TestBucketExistsKeyDoesNot(t *testing.T) {
	sts := []testStat{
		{"foo", stats.Count, 9, int64(time.Millisecond * 9998), 98, ""},
		{"bar", stats.Count, 9, int64(time.Millisecond * 9999), 99, ""},
	}
	expecteds := []testResult{
		{"foo", 0, sts[0].value},
		{"bar", 0, sts[1].value},
	}

	_testStart(t, sts, expecteds)
}

func TestCloseInputChannel(t *testing.T) {
	// Setup
	_resetCurrentTime()
	defer _resetCurrentTime()

	now := func() time.Time {
		muCurrentTime.Lock()
		defer muCurrentTime.Unlock()
		return currentTime
	}

	in := make(chan stats.Stat, 1000)
	out := make(chan stats.Stat, 1000)
	aggregates := map[int64]map[string]*aggregate{}

	sh := shutdown.NewShutdownHandler()
	statsAggregator := NewConfigureAggregates(sh, &sync.Mutex{}, in, out, now, int64(time.Second*10), 10, aggregates)

	go statsAggregator.Start()

	// Wait to ensure routine started
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case <-timeout.C:
	}

	// Close input
	close(in)

	// Verify output gets closed
	timeoutVerify := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-statsAggregator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	case <-timeoutVerify.C:
		assert.Fail(t, "output channel not closed in time")
	}
}

func TestTerminationContext(t *testing.T) {
	// Setup
	_resetCurrentTime()
	defer _resetCurrentTime()

	now := func() time.Time {
		muCurrentTime.Lock()
		defer muCurrentTime.Unlock()
		return currentTime
	}

	in := make(chan stats.Stat, 1000)
	out := make(chan stats.Stat, 1000)
	aggregates := map[int64]map[string]*aggregate{}

	sh := shutdown.NewShutdownHandler()
	statsAggregator := NewConfigureAggregates(sh, &sync.Mutex{}, in, out, now, int64(time.Second*10), 10, aggregates)

	go statsAggregator.Start()

	// Wait to ensure routine started
	time.Sleep(20 * time.Millisecond)

	sh.CancelFunc()

	// Verify output gets closed
	timeoutVerify := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-statsAggregator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	case <-timeoutVerify.C:
		assert.Fail(t, "output channel not closed in time")
	}
}

func TestTerminationContextInSend(t *testing.T) {
	// Setup
	_resetCurrentTime()
	defer _resetCurrentTime()

	now := func() time.Time {
		muCurrentTime.Lock()
		defer muCurrentTime.Unlock()
		return currentTime
	}

	in := make(chan stats.Stat, 1000)
	out := make(chan stats.Stat, 1)
	aggregates := map[int64]map[string]*aggregate{}

	sh := shutdown.NewShutdownHandler()
	statsAggregator := NewConfigureAggregates(sh, &sync.Mutex{}, in, out, now, int64(time.Second*10), 10, aggregates)

	go statsAggregator.Start()

	_updateNowTo(1, 0)
	in <- stats.NewStatCount("rplclient",
		"foo",
		1,
		int64(time.Millisecond*1))

	_updateNowTo(10, 0)
	in <- stats.NewStatCount("rplclient",
		"foo",
		1,
		int64(time.Millisecond*11))

	time.Sleep(250 * time.Millisecond)
	_updateNowTo(946684800, 0)

	sh.CancelFunc()

	time.Sleep(250 * time.Millisecond)

	// Verify output gets closed
	timeoutVerify := time.NewTimer(10 * time.Millisecond)

	select {
	case _, ok := <-statsAggregator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	case <-timeoutVerify.C:
		assert.Fail(t, "output channel not closed in time")
	}
}

func TestPanicHandling(t *testing.T) {
	// Setup
	_resetCurrentTime()
	defer _resetCurrentTime()

	now := func() time.Time {
		muCurrentTime.Lock()
		defer muCurrentTime.Unlock()
		return currentTime
	}

	in := make(chan stats.Stat, 1000)
	out := make(chan stats.Stat, 1)
	aggregates := map[int64]map[string]*aggregate{}

	sh := shutdown.NewShutdownHandler()
	statsAggregator := NewConfigureAggregates(sh, &sync.Mutex{}, in, out, now, int64(time.Second*10), 10, aggregates)

	go statsAggregator.Start()

	close(out)

	_updateNowTo(1, 0)
	in <- stats.NewStatCount("rplclient",
		"foo",
		1,
		int64(time.Millisecond*1))

	time.Sleep(250 * time.Millisecond)
	_updateNowTo(946684800, 0)

	time.Sleep(250 * time.Millisecond)

	// Verify output gets closed
	timeoutVerify := time.NewTimer(10 * time.Millisecond)

	select {
	case _, ok := <-statsAggregator.outputChan:
		if ok {
			assert.Fail(t, "output channel not properly closed")
		}
	case <-timeoutVerify.C:
		assert.Fail(t, "output channel not closed in time")
	}
}

func TestRealTime(t *testing.T) {
	sh := shutdown.NewShutdownHandler()
	in := make(chan stats.Stat, 1000)

	statsAggregator := New(sh, in)
	statsAggregator.aggregateTimeNano = int64(100 * time.Millisecond)
	statsAggregator.Start()

	startTime := time.Now().UnixNano()

	time.Sleep(150 * time.Millisecond)

	in <- stats.NewStatCount("rplclient",
		"foo",
		1,
		time.Now().UnixNano())

	timeoutVerify := time.NewTimer(2000 * time.Millisecond)

	var stat stats.Stat
	var ok bool

	select {
	case stat, ok = <-statsAggregator.GetOutputChan():
		if !ok {
			assert.Fail(t, "output channel was closed")
		}
	case <-timeoutVerify.C:
		assert.Fail(t, "didn't get output in time")
	}

	isBetween := false
	endTime := time.Now().UnixNano()
	if stat.Timestamp >= startTime && stat.Timestamp <= endTime {
		isBetween = true
	}

	assert.Equal(t, true, isBetween, "time bucket was %v but should be between %v and %v", stat.Timestamp, startTime, endTime)
}
