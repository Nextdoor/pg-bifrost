package factory

import (
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters/datadog"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters/stdout"
	"github.com/DataDog/datadog-go/statsd"
)

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan chan stats.Stat,
	reporterType reporters.ReporterType,
	reporterConfig map[string]interface{}) (reporters.Reporter, error) {

	var r reporters.Reporter
	switch reporterType {
	case reporters.STDOUT:
		r = stdout.New(shutdownHandler, inputChan)
		break
	case reporters.DATADOG:
		// TODO(#3): make this configurable
		c, err := statsd.New("127.0.0.1:8125")
		if err != nil {
			return nil, err
		}

		r = datadog.New(shutdownHandler, inputChan, c)
		break
	}

	return r, nil
}
