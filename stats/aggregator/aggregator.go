// Receives stats messages from components, aggregates them pushes to the a stats reporter.
//
// Example Usage:
//
//	import "github.com/Nextdoor/pg-bifrost.git/stats/Aggregator"
//
//	// Setup
//	in_stats := make(chan Stat, 1000)
//	statsAggregator = Aggregator.New(in_stats, time.Now)
//	go statsAggregator.StartTransporting()
//	out_stats := statsAggregator.outputChan
//
//	// send a Stat
//	Stat = NewStatCount("rplclient",
//			"bufferedMessages",
//			1,
//			timeNow)
//	in_stats <- Stat
//
//	// receive aggregated stats
//	aggregated_stat := <-out_stats
//
package aggregator

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "aggregator")
)

const (
	outputChanSize                = 1000
	reportGraceNano               = int64(time.Second)     // (1s) how long after a bucket time has passed to allow stats
	reportAggregatesSleepDuration = time.Millisecond * 250 // how long the reportAggregatesWorker sleeps between runs

	defaultAggregateTimeNano   = int64(time.Second * 60)
	defaultAggregateMaxBuckets = 60
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

// Aggregator type that holds send/receive channels and map of Stat aggregates
type Aggregator struct {
	shutdownHandler shutdown.ShutdownHandler
	timeNow         func() time.Time

	inputChan  <-chan stats.Stat
	outputChan chan stats.Stat // when exposing this chan, it needs to be both send/receive

	aggregateTimeNano   int64 // time in nanoseconds to aggregate stats for. (default: 60s)
	aggregateMaxBuckets uint  // max number of buckets to collect before dropping stats

	muAggregates *sync.Mutex                     // guards aggregates
	aggregates   map[int64]map[string]*aggregate // Collected aggregates of the Aggregator
}

// New returns an Aggregator object with defaults.
func New(shutdownHandler shutdown.ShutdownHandler, inputChan <-chan stats.Stat) Aggregator {
	muAggregates := &sync.Mutex{}
	outputChan := make(chan stats.Stat, outputChanSize)
	aggregates := map[int64]map[string]*aggregate{}

	return NewConfigureAggregates(shutdownHandler, muAggregates, inputChan, outputChan, time.Now, defaultAggregateTimeNano, defaultAggregateMaxBuckets, aggregates)
}

// NewConfigureAggregates returns an Aggregator object primarily for use in tests and lets
// us configure Aggregator parameters and provide a time.Now function for time manipulation.
func NewConfigureAggregates(shutdownHandler shutdown.ShutdownHandler,
	muAggregates *sync.Mutex,
	inputChan <-chan stats.Stat,
	outputChan chan stats.Stat,
	timeNow func() time.Time,
	aggregateTimeNano int64,
	aggregateMaxBuckets uint,
	aggregates map[int64]map[string]*aggregate) Aggregator {

	return Aggregator{
		shutdownHandler,
		timeNow,
		inputChan,
		outputChan,
		aggregateTimeNano,
		aggregateMaxBuckets,
		muAggregates,
		aggregates,
	}
}

// shutdown idempotently closes the output channel
func (a *Aggregator) shutdown() {
	log.Info("shutting down")
	a.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Warn("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	log.Debug("closing output channel")
	close(a.outputChan)
}

// sendAggregate emits an aggregate to the outbound channel as individual stats.
func (a *Aggregator) sendAggregate(agg aggregate) {
	stats := agg.toStats()

	for _, stat := range stats {
		// If processStatsMessagesWorker closed the output channel then don't attempt to write
		select {
		case <-a.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
		}
		a.outputChan <- stat
	}
}

// processStatsMessagesWorker loops now the input channel, collects stats and writes to our aggregates map.
func (a *Aggregator) processStatsMessagesWorker() {
	localLog := log.WithField("routine", "processStatsMessagesWorker")
	localLog.Info("starting")
	defer a.shutdown()

	var s stats.Stat
	var ok bool

	for {
		localLog.Debug("-waiting for Stat-")

		select {
		case s, ok = <-a.inputChan:
			if !ok {
				log.Warn("input channel is closed")
				return
			}
		default:
			// Check to see if a terminate was initiated before proceeding
			select {
			case <-a.shutdownHandler.TerminateCtx.Done():
				log.Debug("received terminateCtx cancellation")
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		// The unique key to identify this statistic
		aggregateKey := computeAggregateKey(s)

		// The bucket time of this aggregation (floor of the aggregateTimeMilli)
		// TODO should this bucket timestamp here and for the aggregate be the ceiling?
		bucketTime := a.aggregateTimeNano * (s.Timestamp / a.aggregateTimeNano)
		localLog.Debugf(`s.timestamp: %d => bucketTime: %d`, s.Timestamp, bucketTime)

		if a.isBtimeExpired(bucketTime) {
			// drop the Stat because it will never have a bucket
			logStatDropped(s, localLog)
			continue
		}

		func() {
			a.muAggregates.Lock()
			defer a.muAggregates.Unlock()

			if _, ok := a.aggregates[bucketTime]; !ok {
				// bucket nor key do NOT exist, create the bucket and key
				localLog.Debug("new bucket")

				a.aggregates[bucketTime] = map[string]*aggregate{}

				agg := newAggregate(s, bucketTime)
				aggPtr := &agg
				a.aggregates[bucketTime][aggregateKey] = aggPtr

				aggPtr.update(s)
				return
			}

			if aggPtr, ok := a.aggregates[bucketTime][aggregateKey]; ok {
				// bucket and key exist
				localLog.Debug("existing bucket and aggregate")
				aggPtr.update(s)
			} else {
				// bucket exists, key does NOT exist
				localLog.Debug("existing bucket only")
				agg := newAggregate(s, bucketTime)
				aggPtr := &agg
				a.aggregates[bucketTime][aggregateKey] = aggPtr

				aggPtr.update(s)
			}
		}()
	}
}

// reportAggregatesWorker runs sendAggregate for any time bucket which is past due which
// reports its aggregates to the outbound channel as individual stats.
// Then, it deletes the bucket from the map.
func (a *Aggregator) reportAggregatesWorker() {
	localLog := log.WithField("routine", "reportAggregatesWorker")
	localLog.Info("starting")
	defer a.shutdown()

	for {
		reportedBucketBtimes := map[int64]bool{} // bucket reported marker

		func() {
			a.muAggregates.Lock()
			defer a.muAggregates.Unlock()

			for bucketTime, bucket := range a.aggregates {
				localLog.Debug("reportWorker - ")
				if a.isBtimeExpired(bucketTime) {
					// report out bucket
					for _, agg := range bucket {
						a.sendAggregate(*agg)
					}

					reportedBucketBtimes[bucketTime] = true
				}
			}

			if len(reportedBucketBtimes) != 0 {
				for bucketTime, _ := range reportedBucketBtimes {
					delete(a.aggregates, bucketTime)
				}
			}
		}()

		time.Sleep(reportAggregatesSleepDuration)
	}
}

// StartTransporting loops on the input channel and aggregates stats from it.
func (a *Aggregator) Start() {
	go a.processStatsMessagesWorker()
	go a.reportAggregatesWorker()
}

// GetOutputChan returns the channel of aggregates for use by a reporter.
func (a *Aggregator) GetOutputChan() chan stats.Stat {
	return a.outputChan
}

// computeAggregateKey gives us a "key" that we can use for unique stats.
func computeAggregateKey(s stats.Stat) string {
	var sb strings.Builder

	sb.WriteString(s.Component)
	sb.WriteString(s.StatName)
	sb.WriteString(string(s.StatType))
	sb.WriteString(s.Unit)

	return sb.String()
}

// isBtimeExpired returns true if the current time is past the bucket time with a grace period
func (a *Aggregator) isBtimeExpired(bucketTime int64) bool {
	timeNow := a.timeNow().UnixNano()
	log.Debugf(`%s > %s ?`, strconv.FormatInt(timeNow, 10),
		strconv.FormatInt(bucketTime+a.aggregateTimeNano+reportGraceNano, 10))
	return timeNow > bucketTime+a.aggregateTimeNano+reportGraceNano
}

// logStatDropped makes sure we know via logging and/or metrics of dropped stats
// TODO(nehalrp) How to log? How to report to stdout?
func logStatDropped(s stats.Stat, localLog *logrus.Entry) {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	localLog.Debugf(` dropping %s !`, string(out))
}
