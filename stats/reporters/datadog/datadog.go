package datadog

import (
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "datadog")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type DataDogReporter struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan chan stats.Stat
	client    *statsd.Client
}

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan chan stats.Stat,
	client *statsd.Client) reporters.Reporter {

	return &DataDogReporter{shutdownHandler: shutdownHandler, inputChan: inputChan, client: client}
}

func (r *DataDogReporter) shutdown() {
	log.Info("shutting down")
	r.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well
	r.client.Close()
}

func (r *DataDogReporter) Start() {
	defer r.shutdown()

	for {
		s, ok := <-r.inputChan

		if !ok {
			return
		}

		// Check to see if a terminate was initiated before proceeding
		select {
		case <-r.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
		}

		statName := fmt.Sprintf("bifrost.%s.%s.%s", s.Component, s.StatName, s.Unit)

		switch s.StatType {
		case stats.Count:
			r.client.Count(statName, s.Value, nil, 1)
			break
		case stats.Histogram:
			r.client.Gauge(statName, float64(s.Value), nil, 1)
		}

		err := r.client.Flush()

		if err != nil {
			log.Errorf("unable to send logs to datadog %s", err)
		}
	}
}
