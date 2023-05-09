package aggregator

import (
	"fmt"
	"math"

	"github.com/Nextdoor/pg-bifrost.git/stats"
)

// aggregate type which keeps a unique Stat's metadata, a running count of it's value
// and calculated statistics.
type aggregate struct {
	// What we aggregate on as a unique Stat.
	component string         // module from which the message is sent (e.g., filter)
	statType  stats.StatType // (e.g., count, timeLength)
	statName  string         // what Stat is being reported (e.g., filtered_count)
	unit      string         // (e.g., count, milliseconds)

	// Running Statistics
	value int64 // total of individual values received to form this aggregate
	count int64 // number of Stat times received to form this aggregate
	min   int64
	max   int64
	avg   float64

	// Metadata
	timestamp int64 // timestamp in nanoseconds since epoch of when the aggregation occurred
}

// newAggregate returns an aggregate with certain initializations such as min and max.
func newAggregate(s stats.Stat, t int64) aggregate {
	return aggregate{
		component: s.Component,
		statType:  s.StatType,
		statName:  s.StatName,
		unit:      s.Unit,
		min:       math.MaxInt64,
		max:       math.MinInt64,
		timestamp: t,
	}
}

// createStat is a helper function to create a new stat based on this aggregate with a specific
// name and value.
func (a *aggregate) createStat(name string, value int64) stats.Stat {
	return stats.Stat{
		StatType:  a.statType,
		Component: a.component,
		StatName:  name,
		Unit:      a.unit,
		Value:     value,
		Timestamp: a.timestamp}
}

// toStats turns an aggregate to a slice of stats that can be directly reported on.
// For stats which occur over a  period of time, an aggregate is multiplexed into multiple stats,
// namespaced by statName.
func (a *aggregate) toStats() []stats.Stat {
	sts := []stats.Stat{}
	sts = append(sts, stats.Stat{
		StatType:  a.statType,
		Component: a.component,
		StatName:  a.statName,
		Unit:      a.unit,
		Value:     a.value,
		Timestamp: a.timestamp})

	switch t := a.statType; t {
	case stats.Count:
		// Do nothing special.
	case stats.Histogram:
		sts = append(sts, a.createStat(fmt.Sprintf("%s_avg", a.statName), int64(a.avg)))
		sts = append(sts, a.createStat(fmt.Sprintf("%s_max", a.statName), int64(a.max)))
		sts = append(sts, a.createStat(fmt.Sprintf("%s_min", a.statName), int64(a.min)))
	default:
		panic("unrecognized statType")
	}

	return sts
}

// update function updates a running aggregate based on the type of Stat it is.
func (a *aggregate) update(s stats.Stat) {
	a.value += s.Value
	a.count += 1

	switch t := a.statType; t {
	case stats.Count:
		// Do nothing special.
	case stats.Histogram:
		if s.Value < a.min {
			a.min = s.Value
		}

		if s.Value > a.max {
			a.max = s.Value
		}

		a.avg = float64(a.value) / float64(a.count)
	default:
		panic("unrecognized statType")
	}
}
