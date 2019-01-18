package stdout

import (
	"fmt"
	"os"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "stdout")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type StdoutReporter struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan chan stats.Stat
}

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan chan stats.Stat) reporters.Reporter {

	return &StdoutReporter{shutdownHandler: shutdownHandler, inputChan: inputChan}
}

func (r *StdoutReporter) shutdown() {
	log.Info("shutting down")
	r.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well
}

func (r *StdoutReporter) Start() {
	log.Info("starting reporter")
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

		ts := time.Unix(s.Timestamp/int64(time.Second), 0)

		fmt.Printf("%s %s %s %s %d\n", ts, s.Component, s.StatName, s.Unit, s.Value)
	}
}
