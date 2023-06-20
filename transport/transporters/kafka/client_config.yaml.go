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

package kafka

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

func producerConfig(kafkaTLS bool, clusterCA, clientPrivateKey, clientPublicKey string, maxMessageBytes int) (*sarama.Config, error) {
	sarama.Logger = clientLogger(true)

	// We don't have a registry set up, so might as well disable these.
	metrics.UseNilMetrics = true

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0

	// Internal channel buffer size.
	config.ChannelBufferSize = 256

	// Default value is 30 seconds, which is too high. We haven't actively tried to optimize these values.
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	// This produces reasonably efficient batching.
	config.Producer.Flush.Bytes = 262144

	// The best-effort frequency of flushes.
	config.Producer.Flush.Frequency = 2500 * time.Millisecond

	// Required to be set to true for producer to function.
	config.Producer.Return.Successes = true

	// Required to be set to true for producer to function.
	config.Producer.Return.Errors = true

	// Message compression type
	config.Producer.Compression = sarama.CompressionSnappy

	// The maximum permitted size of a message
	config.Producer.MaxMessageBytes = maxMessageBytes

	// During cluster restarts, we're at risk of hitting the max retry limit for a
	// partition if we retry too quickly.
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	// Reduce memory usage by only holding onto the metadata needed for PG-Bifrost's topic(s).
	config.Metadata.Full = false

	// By default, Kafka closes idle connections after 10 minutes.
	// For brokers that don't have any PG-Bifrost partitions, the connection is idle other
	// than metadata refreshing, so at times the client will try to use a connection
	// that the broker has closed.
	config.Metadata.RefreshFrequency = 5 * time.Minute

	// Timeout for fetching metadata.
	config.Metadata.Timeout = 20 * time.Second

	config.Metadata.Retry.Max = 10

	config.Metadata.Retry.BackoffFunc = metadataBackoff

	var err error
	if kafkaTLS {
		config, err = configureTLS(config, clusterCA, clientPrivateKey, clientPublicKey)
	}
	return config, err
}

// Logarithmic backoff with jitter
func metadataBackoff(retries, maxRetries int) time.Duration {
	const BASE = 150
	duration := time.Duration(math.Log2(float64(retries+2))*BASE) * time.Millisecond
	r, _ := rand.Int(rand.Reader, big.NewInt((10000 * time.Microsecond).Microseconds()))
	jitter := time.Duration(r.Int64()) * time.Microsecond

	return duration + jitter
}

func configureTLS(config *sarama.Config, clusterCA, clientPrivateKey, clientPublicKey string) (*sarama.Config, error) {
	clientCert, err := tls.LoadX509KeyPair(clientPublicKey, clientPrivateKey)
	if err != nil {
		return nil, err
	}

	clusterCert, err := os.ReadFile(clusterCA)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(clusterCert)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
	}

	config.Net.TLS.Config = tlsConfig
	config.Net.TLS.Enable = true
	return config, nil
}

type saramaLogger struct {
	prefix string
	logFn  func(args ...interface{})
}

func clientLogger(debug bool) sarama.StdLogger {
	prefix := "Sarama: "
	if debug {
		return &saramaLogger{prefix, log.Debug}
	}
	return &saramaLogger{prefix, log.Info}
}

func (s *saramaLogger) Printf(format string, v ...interface{}) {
	s.logFn(fmt.Sprintf(strings.Replace(s.prefix+format, "\n", "", -1), v...))
}

func (s *saramaLogger) Println(v ...interface{}) {
	s.logFn(fmt.Sprintf(strings.Replace(s.prefix+"%v", "\n", "", -1), v))
}

func (s *saramaLogger) Print(v ...interface{}) {
	s.logFn(fmt.Sprintf(strings.Replace(s.prefix+"%v", "\n", "", -1), v))
}
