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

package rabbitmq

import (
	"net/url"
	"os"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq/transporter"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "rabbitmq")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

// New creates a rabbitmq transport which has rabbitmq batching and rabbitmq transporting, connected by channels
func New(shutdownHandler shutdown.ShutdownHandler,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	workers int,
	inputChans []<-chan transport.Batch,
	transportConfig map[string]interface{}) []*transport.Transporter {

	exchangeNameVar := transportConfig[ConfVarExchangeName]
	exchangeName, ok := exchangeNameVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarExchangeName, "string")
	}

	usernameVar := transportConfig[ConfVarUsername]
	var username string
	if usernameVar != "" {
		username, ok = usernameVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarUsername, "string")
		}
	}

	passwordVar := transportConfig[ConfVarPassword]
	var password string
	if passwordVar != "" {
		password, ok = passwordVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarPassword, "string")
		}
	}

	hostVar := transportConfig[ConfVarHost]
	var host string
	if hostVar != "" {
		host, ok = hostVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarHost, "string")
		}
	}

	virtualHostVar := transportConfig[ConfVarVirtualHost]
	var virtualHost string
	if virtualHostVar != "" {
		virtualHost, ok = virtualHostVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarVirtualHost, "string")
		}
	}
	amqpURL := &url.URL{
		Scheme: "amqp",
		Host:   host,
		User:   url.UserPassword(username, password),
		Path:   virtualHost,
	}

	transports := make([]*transport.Transporter, workers)

	// Make and link transporters to the batcher's output channels
	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(shutdownHandler, inputChans[i], txnsWritten, statsChan, *log, i, amqpURL, exchangeName)
		transports[i] = &t
	}

	return transports
}
