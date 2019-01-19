package aggregator

import (
	"testing"

	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// TODO(#6): make these tests less declarative
func TestToStatsWithCountStat(t *testing.T) {
	data := aggregate{
		component: "test_filter",
		statType:  stats.Count,
		statName:  "filtered_count",
		unit:      "count",
		value:     123,
		count:     123,
		timestamp: 1234500000,
	}
	expected := []stats.Stat{stats.Stat{
		Component: "test_filter",
		StatType:  stats.Count,
		StatName:  "filtered_count",
		Unit:      "count",
		Value:     123,
		Timestamp: 1234500000,
	}}

	// Run
	result := data.toStats()

	// Test
	assert.Equal(t, expected, result, "The two stats[] should be the same.")
}

func TestToStatsWithTimeLengthStat(t *testing.T) {
	data := aggregate{
		component: "test_replication_slot_lag",
		statType:  stats.Histogram,
		statName:  "replication_slot_lag_milli",
		unit:      "milli",
		value:     123,
		count:     123,
		timestamp: 1234500000,
	}
	expected := []stats.Stat{
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_avg",
			Unit:      "milli",
			Value:     0, // no update() has ran so value will be 0 for calculated values
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_max",
			Unit:      "milli",
			Value:     0,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_min",
			Unit:      "milli",
			Value:     0,
			Timestamp: 1234500000},
	}

	// Run
	result := data.toStats()

	// Test
	assert.Equal(t, expected, result, "The two stats[] should be the same.")
}

func TestUpdateWithSingleTimeLengthStat(t *testing.T) {
	data := stats.NewStatHistogram("test_replication_slot_lag",
		"replication_slot_lag_milli",
		123,
		1234500000,
		"milli")
	agg := newAggregate(data, 1234500000)

	expected := []stats.Stat{
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_avg",
			Unit:      "milli",
			Value:     123, // no update() has ran so value will be 0 for calculated values
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_max",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_min",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
	}

	// Run
	agg.update(data)
	result := agg.toStats()

	// Test
	assert.Equal(t, expected, result, "The two stats[] should be the same.")
}

func TestUpdateWithDoubleTimeLengthStat(t *testing.T) {
	data := stats.NewStatHistogram("test_replication_slot_lag",
		"replication_slot_lag_milli",
		123,
		1234500000,
		"milli")
	agg := newAggregate(data, 1234500000)

	expected := []stats.Stat{
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli",
			Unit:      "milli",
			Value:     246,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_avg",
			Unit:      "milli",
			Value:     123, // no update() has ran so value will be 0 for calculated values
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_max",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_min",
			Unit:      "milli",
			Value:     123,
			Timestamp: 1234500000},
	}

	// Run
	agg.update(data)
	agg.update(data)
	result := agg.toStats()

	// Test
	assert.Equal(t, expected, result, "The two stats[] should be the same.")
}

func TestUpdateWithTwoTimeLengthStat(t *testing.T) {
	dataOne := stats.NewStatHistogram("test_replication_slot_lag",
		"replication_slot_lag_milli",
		100,
		1234500000,
		"milli")
	dataTwo := stats.NewStatHistogram("test_replication_slot_lag",
		"replication_slot_lag_milli",
		200,
		1257894001,
		"milli")
	agg := newAggregate(dataOne, 1234500000)

	expected := []stats.Stat{
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli",
			Unit:      "milli",
			Value:     300,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_avg",
			Unit:      "milli",
			Value:     150, // no update() has ran so value will be 0 for calculated values
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_max",
			Unit:      "milli",
			Value:     200,
			Timestamp: 1234500000},
		{Component: "test_replication_slot_lag",
			StatType:  stats.Histogram,
			StatName:  "replication_slot_lag_milli_min",
			Unit:      "milli",
			Value:     100,
			Timestamp: 1234500000},
	}

	// Run
	agg.update(dataOne)
	agg.update(dataTwo)
	result := agg.toStats()

	// Test
	eq := cmp.Equal(expected, result)
	if !eq {
		t.Error(
			"For\n", dataOne, dataTwo,
			"expected\n", expected,
			"got\n", result)
	}
}
