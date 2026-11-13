package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddr            = flag.String("web.listen-address", ":9352", "Address to listen on for web interface and telemetry.")
	metricsPath           = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	walgPath              = flag.String("walg.path", "wal-g", "Path to the wal-g binary.")
	walgConfigPath        = flag.String("walg.config-path", "", "Path to the wal-g config file.")
	storageScrapeInterval = flag.Duration("storage.scrape.interval", 30*time.Second,
		"Interval between storage aliveness checks.")
	walgScrapeInterval = flag.Duration("walg.scrape.interval", 60*time.Second,
		"Interval between WAL-G backup and binlog scrapes.")
)

// metricPrefix returns the prefix of the metric names of the database.
func metricPrefix(database string) (string, error) {
	switch database {
	case "mysql", "mariadb":
		return "walg_" + database, nil
	}
	return "", fmt.Errorf("unknown database %q, must be \"mysql\" or \"mariadb\"", database)
}

// newHandler returns the HTTP handler of the exporter: the metrics, a landing page and the liveness
// (/health) and readiness (/ready) probes.
func newHandler(exporter *Exporter, metricsHandler http.Handler, metricsPath string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(metricsPath, metricsHandler)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html>
<head><title>WAL-G MySQL/MariaDB Prometheus Exporter</title></head>
<body>
<h1>WAL-G MySQL/MariaDB Prometheus Exporter</h1>
<p><a href="%s">Metrics</a></p>
</body>
</html>`, metricsPath)
	})

	// /health: the process is alive (liveness probe)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"healthy"}`)
	})

	// /ready: at least one successful scrape has completed (readiness probe)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if exporter.IsReady() {
			fmt.Fprint(w, `{"status":"ready"}`)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"not_ready","reason":"no successful scrape completed yet"}`)
	})

	return mux
}

func main() {
	database := flag.String("database", "mysql",
		`Database the exporter serves, "mysql" or "mariadb". It selects the prefix of the metric names: walg_mysql or walg_mariadb.`)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	prefix, err := metricPrefix(*database)
	if err != nil {
		logger.Error("Invalid --database", "error", err)
		os.Exit(2)
	}

	logger.Info("Starting WAL-G MySQL/MariaDB Prometheus Exporter",
		"listen_address", *listenAddr,
		"metrics_path", *metricsPath,
		"database", *database,
		"walg_path", *walgPath,
		"walg_config", *walgConfigPath,
		slog.Group("intervals",
			slog.Duration("storage_scrape", *storageScrapeInterval),
			slog.Duration("walg_scrape", *walgScrapeInterval),
		),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exporter := NewExporter(logger, Config{
		MetricPrefix:          prefix,
		WalgPath:              *walgPath,
		WalgConfigPath:        *walgConfigPath,
		StorageScrapeInterval: *storageScrapeInterval,
		WalgScrapeInterval:    *walgScrapeInterval,
	})
	prometheus.MustRegister(exporter)

	go exporter.Start(ctx)

	server := &http.Server{
		Addr:         *listenAddr,
		Handler:      newHandler(exporter, promhttp.Handler(), *metricsPath),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Starting HTTP server", "addr", *listenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	logger.Info("Received shutdown signal", "signal", sig.String())
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("Exporter shutdown complete")
}
