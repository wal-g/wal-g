package internal

import (
	"fmt"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
)

type metrics struct {
	uploadedFilesTotal       prometheus.Counter
	uploadedFilesFailedTotal prometheus.Counter
}

var (
	WalgMetricsPrefix = "walg_"

	WalgMetrics = metrics{
		uploadedFilesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: WalgMetricsPrefix + "uploader_uploaded_files_total",
				Help: "Number of uploaded files.",
			},
		),

		uploadedFilesFailedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: WalgMetricsPrefix + "uploader_uploaded_files_failed_total",
				Help: "Number of file upload failures.",
			},
		),
	}
)

func init() {
	// unregister prometheus collectors
	// https://github.com/prometheus/client_golang/blob/8dfa334295e85f9b1e48ce862fae5f337faa6d2f/prometheus/registry.go#L62-L63
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.Unregister(collectors.NewGoCollector())

	prometheus.MustRegister(WalgMetrics.uploadedFilesTotal)
	prometheus.MustRegister(WalgMetrics.uploadedFilesFailedTotal)
}

func PushMetrics() {
	address := viper.GetString(StatsdAddressSetting)
	if address == "" {
		return
	}

	err := pushMetrics(address)
	if err != nil {
		tracelog.WarningLogger.Printf("Pushing metrics failed: %v", err)
	}
}

func pushMetrics(address string) error {
	config := &statsd.ClientConfig{
		Address:       address,
		UseBuffered:   true,
		FlushInterval: 10 * time.Second,
		TagFormat:     statsd.InfixComma,
	}

	client, err := statsd.NewClientWithConfig(config)
	if err != nil {
		return err
	}
	defer client.Close()

	tracelog.DebugLogger.Printf("Sending metrics to statsd")

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return err
	}
	for _, mf := range mfs {
		if err := writeMetricFamilyToStatsd(client, mf); err != nil {
			return err
		}
	}

	return nil
}

func writeMetricFamilyToStatsd(client statsd.Statter, in *dto.MetricFamily) error {
	name := in.GetName()
	metricType := in.GetType()

	for _, metric := range in.Metric {
		var tags []statsd.Tag
		for _, lp := range metric.Label {
			tags = append(tags, statsd.Tag{lp.GetName(), lp.GetValue()})
		}

		switch metricType {
		case dto.MetricType_COUNTER:
			if metric.Counter == nil {
				return fmt.Errorf("expected counter in metric %s %s", name, metric)
			}
			err := client.Inc(name, int64(metric.Counter.GetValue()), 1.0, tags...)
			if err != nil {
				return err
			}
		case dto.MetricType_GAUGE:
			if metric.Gauge == nil {
				return fmt.Errorf("expected gauge in metric %s %s", name, metric)
			}
			err := client.Gauge(name, int64(metric.Gauge.GetValue()), 1.0, tags...)
			if err != nil {
				return err
			}
		case dto.MetricType_UNTYPED:
			return fmt.Errorf("expected untyped in metric %s %s", name, metric)
		case dto.MetricType_SUMMARY:
			return fmt.Errorf("expected summary in metric %s %s", name, metric)
		case dto.MetricType_HISTOGRAM:
			return fmt.Errorf("expected histogram in metric %s %s", name, metric)
		default:
			return fmt.Errorf("unexpected type in metric %s %s", name, metric)
		}
	}

	return nil
}
