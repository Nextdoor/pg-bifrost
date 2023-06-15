package kafka

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math"
	"math/big"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

func producerConfig(kafkaTLS bool, clusterCA, clientPrivateKey, clientPublicKey string) (*sarama.Config, error) {
	// TODO: Set up a custom logger for Sarama debug logging.
	//sarama.Logger = clientLogger(true)

	// We don't have a registry set up, so might as well disable these.
	metrics.UseNilMetrics = true

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0

	// Default client value. This should be kept lower, since we don't have control over
	// Sarama's internal buffering. Having too high of a value may cause shutdown to be
	// blocked for too long.
	config.ChannelBufferSize = 256

	// Default value is 30 seconds, which is too high. We haven't actively tried to optimize these values.
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	// This produces reasonably efficient batching, but could be tuned more.
	config.Producer.Flush.Bytes = 262144

	// Should be less than `closeDelay` to make a best-effort attempt at flushing
	// all messages when shutting down.
	config.Producer.Flush.Frequency = 2500 * time.Millisecond

	// This is required to track metrics for successful writes.
	config.Producer.Return.Successes = true

	// This is required to track any errors.
	config.Producer.Return.Errors = true

	// We previously used ztsd, which produced a better compression ratio, but the version used by
	// Sarama 1.29 and 1.30 would use way too much memory over time: https://github.com/Shopify/sarama/issues/1831.
	config.Producer.Compression = sarama.CompressionSnappy

	// Default client value.
	config.Producer.MaxMessageBytes = 1000000

	// Default value is 100ms. During cluster restarts, we're at risk of hitting the max retry limit
	// for a partition if we retry too quickly. This value was increased when debugging buffer flushing
	// issues, but hasn't been actively tuned / optimized.
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	// Reduce memory usage by only holding onto the metadata needed for PG-Bifrost's topic(s).
	config.Metadata.Full = false

	// By default, Kafka closes idle connections after 10 minutes.
	// For brokers that don't have any PG-Bifrost partitions, the connection is idle other
	// than metadata refreshing, so at times the client will try to use a connection
	// that the broker has closed.
	config.Metadata.RefreshFrequency = 5 * time.Minute

	// Timeout for fetching metadata. The default configuration may cause
	// requests to fail too slowly, depending on how other settings are configured.
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
