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

package kinesis

import (
	"os"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/app/config"
	"github.com/Nextdoor/pg-bifrost.git/partitioner"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/utils"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/transporter"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "kinesis")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

func New(
	shutdownHandler shutdown.ShutdownHandler,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	workers int,
	inputChans []<-chan transport.Batch,
	transportConfig map[string]interface{}) []*transport.Transporter {

	// Get configs
	streamNameVar := transportConfig[ConfVarStreamName]
	streamName, ok := streamNameVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarStreamName, "string")
	}

	awsRegion := transportConfig[ConfVarAwsRegion]
	var awsRegionVar string
	if awsRegion != "" {
		awsRegionVar, ok = awsRegion.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarAwsRegion, "string")
		}
	}

	awsAccessKeyId := transportConfig[ConfVarAwsAccessKeyId]
	var awsAccessKeyIdVar string
	if awsAccessKeyId != "" {
		awsAccessKeyIdVar, ok = awsAccessKeyId.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarAwsAccessKeyId, "string")
		}
	}

	awsSecretAccessKey := transportConfig[ConfVarAwsSecretAccessKey]
	var awsSecretAccessKeyVar string
	if awsSecretAccessKey != "" {
		awsSecretAccessKeyVar, ok = awsSecretAccessKey.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarAwsSecretAccessKey, "string")
		}
	}

	endpoint := transportConfig[ConfVarEndpoint]
	var endpointVar string
	var endpointVarPtr *string = nil
	if endpoint != "" {
		endpointVar, ok = endpoint.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarEndpoint, "string")
		}
		endpointVarPtr = &endpointVar
	}

	transports := make([]*transport.Transporter, workers)

	// Make and link transporters to the batcher's output channels
	for i := 0; i < workers; i++ {
		// One retry policy per worker is required because the policy
		// is not thread safe.
		retryPolicy := &backoff.ExponentialBackOff{
			InitialInterval:     1500 * time.Millisecond,
			RandomizationFactor: 0.5,
			Multiplier:          1.2,
			MaxInterval:         5 * time.Second,
			MaxElapsedTime:      time.Duration(time.Minute * 5),
			Clock:               backoff.SystemClock,
		}

		t := transporter.NewTransporter(shutdownHandler,
			inputChans[i],
			txnsWritten,
			statsChan,
			*log,
			i,
			streamName,
			retryPolicy,
			&awsRegionVar,
			&awsAccessKeyIdVar,
			&awsSecretAccessKeyVar,
			endpointVarPtr,
		)
		transports[i] = &t
	}

	return transports
}

type KinesisBatchFactory struct {
	partitionMethod utils.KinesisPartitionMethod
}

func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	partMethod, ok := transportConfig[config.VAR_NAME_PARTITION_METHOD]
	if !ok {
		log.Fatal("Expected ", config.VAR_NAME_PARTITION_METHOD, " to be in config")
	}

	// If partitioning is not set then use wal-start. Otherwise the user wants each
	// batch to go to the same shard.
	var kinesisPartMethod utils.KinesisPartitionMethod
	if partMethod == partitioner.PART_METHOD_NONE {
		kinesisPartMethod = utils.KINESIS_PART_WALSTART
	} else {
		kinesisPartMethod = utils.KINESIS_PART_BATCH
	}

	return KinesisBatchFactory{kinesisPartMethod}
}

func (f KinesisBatchFactory) NewBatch(partitionKey string) transport.Batch {
	return batch.NewKinesisBatch(partitionKey, f.partitionMethod)
}
