package factory

import (
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/Nextdoor/pg-bifrost.git/app/config"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters/datadog"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters/stdout"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "reporters")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan chan stats.Stat,
	reporterType reporters.ReporterType,
	reporterConfig map[string]interface{}) (reporters.Reporter, error) {

	// TODO(#3): make this configurable
	var r reporters.Reporter
	switch reporterType {
	case reporters.STDOUT:
		r = stdout.New(shutdownHandler, inputChan)
		break
	case reporters.DATADOG:
		addr, ok := reporterConfig[config.VAR_NAME_DD_HOST].(string)
		if !ok {
			log.Panic("Wrong type assertion")
		}
		log.Info("addr=", addr)

		tags, ok := reporterConfig[config.VAR_NAME_DD_TAGS].([]string)
		if !ok {
			log.Panic("Wrong type assertion")
		}
		log.Info("tags=", tags)

		c, err := statsd.New(addr,
			statsd.WithTags(tags),
		)

		if err != nil {
			return nil, err
		}

		r = datadog.New(shutdownHandler, inputChan, c)
		break
	}

	return r, nil
}
