package statistics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
)

type metrics struct {
	UploadedFilesTotal       prometheus.Counter
	UploadedFilesFailedTotal prometheus.Counter

	S3Codes        prometheus.GaugeVec
	S3BytesWritten prometheus.Gauge
	S3BytesRead    prometheus.Gauge
}

var (
	WalgMetricsPrefix = "walg_"

	WalgMetrics = metrics{
		UploadedFilesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: WalgMetricsPrefix + "uploader_uploaded_files_total",
				Help: "Number of uploaded files.",
			},
		),
		UploadedFilesFailedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: WalgMetricsPrefix + "uploader_uploaded_files_failed_total",
				Help: "Number of file upload failures.",
			},
		),
		S3Codes: *prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: WalgMetricsPrefix + "s3_response_",
				Help: "S3 response codes.",
			},
			[]string{"code"},
		),
		S3BytesWritten: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: WalgMetricsPrefix + "s3_bytes_written",
				Help: "Amount of bytes written to S3.",
			},
		),
		S3BytesRead: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: WalgMetricsPrefix + "s3_bytes_read",
				Help: "Amount of bytes read from S3.",
			},
		),
	}
)

func init() {
	// unregister prometheus collectors
	// https://github.com/prometheus/client_golang/blob/8dfa334295e85f9b1e48ce862fae5f337faa6d2f/prometheus/registry.go#L62-L63
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.Unregister(collectors.NewGoCollector())

	prometheus.MustRegister(WalgMetrics.UploadedFilesTotal)
	prometheus.MustRegister(WalgMetrics.UploadedFilesFailedTotal)
	prometheus.MustRegister(WalgMetrics.S3Codes)
	prometheus.MustRegister(WalgMetrics.S3BytesWritten)
	prometheus.MustRegister(WalgMetrics.S3BytesRead)
}

func PushMetrics() {
	address := viper.GetString(conf.StatsdAddressSetting)
	if address == "" {
		return
	}

	extraTags := viper.GetStringMapString(conf.StatsdExtraTagsSetting)

	err := pushMetrics(address, extraTags)
	if err != nil {
		tracelog.WarningLogger.Printf("Pushing metrics failed: %v", err)
	}
}

func WriteStatusCodeMetric(code int) {
	WalgMetrics.S3Codes.WithLabelValues(strconv.Itoa(code)).Inc()
}

func pushMetrics(address string, extraTags map[string]string) error {
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
		if err := writeMetricFamilyToStatsd(client, mf, extraTags); err != nil {
			return err
		}
	}

	return nil
}

func writeMetricFamilyToStatsd(client statsd.Statter, in *dto.MetricFamily, extraTags map[string]string) error {
	name := in.GetName()
	metricType := in.GetType()

	for _, metric := range in.Metric {
		tags := make([]statsd.Tag, 0, len(metric.Label)+len(extraTags))
		for _, lp := range metric.Label {
			tags = append(tags, statsd.Tag{lp.GetName(), lp.GetValue()})
		}
		for k, v := range extraTags {
			tags = append(tags, statsd.Tag{k, v})
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
			tracelog.DebugLogger.Printf("writing metric: %s", metric.String())
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
