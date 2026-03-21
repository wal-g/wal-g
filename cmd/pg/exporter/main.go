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
	listenAddr            = flag.String("web.listen-address", ":9351", "Address to listen on for web interface and telemetry.")
	metricsPath           = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	walgPath              = flag.String("walg.path", "wal-g", "Path to the wal-g binary.")
	backupScrapeInterval  = flag.Duration("backup-list.scrape-interval", 60*time.Second, "Interval between backup-list scrapes.")
	verifyScrapeInterval  = flag.Duration("wal-verify.scrape-interval", 5*time.Minute, "Interval between wal-verify scrapes.")
	storageScrapeInterval = flag.Duration("storage-check.scrape-interval", 30*time.Second, "Interval between storage scrapes.")
	walgConfigPath        = flag.String("walg.config-path", "", "Path to the wal-g config file.")
)

func main() {
	flag.Parse()

	// Initialize structured logger
	// Using JSONHandler for production-ready structured logs
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logger.Info("Starting WAL-G Prometheus exporter",
		"listen_address", *listenAddr,
		"metrics_path", *metricsPath,
		"walg_path", *walgPath,
		"walg_config", *walgConfigPath,
		slog.Group("intervals",
			slog.Duration("backup", *backupScrapeInterval),
			slog.Duration("verify", *verifyScrapeInterval),
			slog.Duration("storage", *storageScrapeInterval),
		),
	)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unsupportedEnvs := []string{
		"WALG_LOG_LEVEL",
		"S3_LOG_LEVEL",
	}

	for _, env := range unsupportedEnvs {
		if val := os.Getenv(env); val != "" {
			logger.Warn("Clearing unsupported environment variable", "key", env, "value", val)
			os.Unsetenv(env)
		}
	}

	// Create and register the exporter
	exporter, err := NewWalgExporter(*walgPath, *backupScrapeInterval, *verifyScrapeInterval, *storageScrapeInterval, *walgConfigPath)
	if err != nil {
		logger.Error("Failed to create exporter", "error", err)
		os.Exit(1)
	}

	prometheus.MustRegister(exporter)

	// Start the exporter in a goroutine
	go exporter.Start(ctx)

	// Set up HTTP server
	mux := http.NewServeMux()
	mux.Handle(*metricsPath, promhttp.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html>
<head><title>WAL-G Prometheus Exporter</title></head>
<body>
<h1>WAL-G Prometheus Exporter</h1>
<p><a href="%s">Metrics</a></p>
</body>
</html>`, *metricsPath)
	})

	server := &http.Server{
		Addr:    *listenAddr,
		Handler: mux,
	}

	// Start HTTP server in a goroutine
	go func() {
		logger.Info("Starting HTTP server", "addr", *listenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	logger.Info("Received shutdown signal", "signal", sig.String())

	// Cancel context to stop exporter
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("Exporter shutdown complete")
}