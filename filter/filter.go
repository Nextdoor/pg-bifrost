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

package filter

import (
	"os"
	"regexp"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "filter")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Filter struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan  <-chan *replication.WalMessage
	OutputChan chan *replication.WalMessage

	statsChan chan stats.Stat

	passthrough bool
	whitelist   bool
	regex       bool
	tablelist   []string
	regexlist   []*regexp.Regexp
}

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *replication.WalMessage,
	statsChan chan stats.Stat,
	whitelist bool,
	regex bool,
	tablelist []string,
) Filter {
	outputChan := make(chan *replication.WalMessage)

	// if in blacklist mode and list is empty then pass everything through
	passthrough := false
	if !whitelist && len(tablelist) == 0 {
		passthrough = true
	}

	regexlist := make([]*regexp.Regexp, len(tablelist))
	for i, item := range tablelist {
		regex, err := regexp.Compile(item)
		if err != nil {
			log.WithError(err).WithField("regexp", item).Warn("Problem compiling regular expression.")
		}
		regexlist[i] = regex
	}

	return Filter{shutdownHandler,
		inputChan,
		outputChan,
		statsChan,
		passthrough,
		whitelist,
		regex,
		tablelist,
		regexlist,
	}
}

// shutdown idempotently closes the output channel and cancels the termination context
func (f *Filter) shutdown() {
	log.Info("shutting down")
	f.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	log.Debug("closing output channel")
	close(f.OutputChan)
}

func (f *Filter) Start() {
	log.Info("starting")

	var op string
	if f.whitelist {
		op = "whitelist"
	} else {
		op = "blacklist"
	}

	log.Infof("%s (regex: %v) filter on tables %v", op, f.regex, f.tablelist)

	defer f.shutdown()

	var msg *replication.WalMessage
	var ok bool

	for {
		select {
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return

		case msg, ok = <-f.inputChan:
			// pass
		}

		select {
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
			// pass
		}

		if !ok {
			log.Debug("input channel is closed")
			return
		}

		if msg == nil {
			log.Error("message was nil")
		}

		// if in blacklist mode and list is empty then pass everything through
		if f.passthrough {
			f.OutputChan <- msg
			f.statsChan <- stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())
			continue
		}

		// pass begins and commits through
		if msg.Pr.Operation == "BEGIN" || msg.Pr.Operation == "COMMIT" {
			f.OutputChan <- msg
			f.statsChan <- stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())
			continue
		}

		// check to see if relation (table name) is found in the lsit
		found := false
		if f.regex {
			var matched bool
			for _, item := range f.regexlist {
				if matched = item.MatchString(msg.Pr.Relation); matched {
					found = true
					break
				}
			}
		} else {
			for _, item := range f.tablelist {
				if msg.Pr.Relation == item {
					found = true
					break
				}
			}
		}

		/*
			Filter logic:

			whitelist:
					found: pass through
				not found: drop

			blacklist:
					found: drop
				not found: pass through

		*/
		filtered := true
		if f.whitelist {
			if found {
				filtered = false
			}
		} else {
			if !found {
				filtered = false
			}
		}

		if filtered {
			f.statsChan <- stats.NewStatCount("filter", "filtered", 1, time.Now().UnixNano())
			continue
		}

		select {
		case f.OutputChan <- msg:
			// pass
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		}

		f.statsChan <- stats.NewStatCount("filter", "passed", 1, time.Now().UnixNano())
	}
}
