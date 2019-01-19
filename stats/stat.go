package stats

import (
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

type StatType string

const (
	Count     StatType = "count"
	Histogram StatType = "histogram"
)

// Stat type which gets externally and internally constructed which contains a Stat and metadata.
// TODO(#5): How can we do tags for Stat? Such as table name.
type Stat struct {
	// What we aggregate on as a unique Stat.
	Component string   // module from which the message is sent (e.g., filter)
	StatType  StatType // (e.g., count, timeLength)
	StatName  string   // what Stat is being reported (e.g., filtered_count)
	Unit      string   // (e.g., count, milliseconds)

	// Statistics
	Value int64 //

	// Metadata
	Timestamp int64 // timestamp in nanoseconds since epoch of Stat being recorded
}

// NewStatCount returns a Stat struct with statType set accordingly.
func NewStatCount(component string, statName string, value int64, timestamp int64) Stat {
	return Stat{StatType: Count,
		Component: component,
		StatName:  statName,
		Unit:      "count",
		Value:     value,
		Timestamp: timestamp}
}

// NewStatHistogram returns a Stat struct with statType set accordingly.
func NewStatHistogram(component string, statName string, value int64, timestamp int64, unit string) Stat {
	return Stat{StatType: Histogram,
		Component: component,
		StatName:  statName,
		Unit:      unit,
		Value:     value,
		Timestamp: timestamp}
}

// Equal is a equality helper for stat
func (s Stat) Equal(other Stat) bool {
	if s.Component != other.Component {
		return false
	}

	if s.StatType != other.StatType {
		return false
	}

	if s.StatName != other.StatName {
		return false
	}

	if s.Unit != other.Unit {
		return false
	}

	if s.Value != other.Value {
		return false
	}

	return true
}

func VerifyStats(t *testing.T, statsChan chan Stat, expected []Stat) {
	actual := make([]Stat, 0)

	func() {
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				// Got all the stats
				return
			case s := <-statsChan:
				actual = append(actual, s)
			}
		}
	}()

	eq := cmp.Equal(expected, actual, cmp.AllowUnexported(Stat{}))
	if !eq {
		t.Error(
			"expected\n", expected,
			"got\n", actual)
	}
}
