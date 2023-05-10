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

package s3

import (
	"os"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/s3/transporter"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "s3")
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
	bucketNameVar := transportConfig[ConfVarBucketName]
	bucketName, ok := bucketNameVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarBucketName, "string")
	}

	keySpaceVar := transportConfig[ConfVarKeySpace]
	keySpace, ok := keySpaceVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarKeySpace, "string")
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
			bucketName,
			keySpace,
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

// NewBatchFactory returns a GenericBatchFactory configured for S3
func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	batchSizeVar := transportConfig[ConfVarPutBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarPutBatchSize, "int")
	}

	return batch.NewGenericBatchFactory(batchSize)
}
