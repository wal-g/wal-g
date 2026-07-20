package main

import (
	"context"
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
	backupScrapeInterval  = flag.Duration("backup-list.scrape-interval", 60*time.Second, "Interval between backup-list scrapes.")
	binlogScrapeInterval  = flag.Duration("binlog-list.scrape-interval", 30*time.Second, "Interval between binlog-list scrapes.")
	storageScrapeInterval = flag.Duration("storage-check.scrape-interval", 30*time.Second, "Interval between storage connectivity checks.")
)

func main() {
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logger.Info("Starting WAL-G MySQL/MariaDB Prometheus exporter",
		"listen_address", *listenAddr,
		"metrics_path", *metricsPath,
		"walg_path", *walgPath,
		"walg_config", *walgConfigPath,
		slog.Group("intervals",
			slog.Duration("backup", *backupScrapeInterval),
			slog.Duration("binlog", *binlogScrapeInterval),
			slog.Duration("storage", *storageScrapeInterval),
		),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exporter, err := NewExporter(
		logger,
		*walgPath,
		*backupScrapeInterval,
		*binlogScrapeInterval,
		*storageScrapeInterval,
		*walgConfigPath,
	)
	if err != nil {
		logger.Error("Failed to create exporter", "error", err)
		os.Exit(1)
	}

	prometheus.MustRegister(exporter)

	go exporter.Start(ctx)

	mux := http.NewServeMux()
	mux.Handle(*metricsPath, promhttp.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html>
<head><title>WAL-G MySQL/MariaDB Exporter</title></head>
<body>
<h1>WAL-G MySQL/MariaDB Exporter</h1>
<p><a href="%s">Metrics</a></p>
</body>
</html>`, *metricsPath)
	})

	server := &http.Server{
		Addr:    *listenAddr,
		Handler: mux,
	}

	go func() {
		logger.Info("HTTP server listening", "addr", *listenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
