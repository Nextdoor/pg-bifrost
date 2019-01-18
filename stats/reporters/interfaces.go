package reporters

type ReporterType string

const (
	STDOUT  ReporterType = "stdout"
	DATADOG ReporterType = "datadog"
)

type Reporter interface {
	Start() // Run as routine to start reading from stats aggregator output channel
}
