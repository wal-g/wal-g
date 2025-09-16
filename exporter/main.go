package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddr     = flag.String("web.listen-address", ":9351", "Address to listen on for web interface and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	walgPath       = flag.String("walg.path", "wal-g", "Path to the wal-g binary.")
	scrapeInterval = flag.Duration("scrape.interval", 60*time.Second, "Interval between scrapes.")
)

func main() {
	flag.Parse()

	log.Printf("Starting WAL-G Prometheus exporter")
	log.Printf("Listen address: %s", *listenAddr)
	log.Printf("Metrics path: %s", *metricsPath)
	log.Printf("WAL-G path: %s", *walgPath)
	log.Printf("Scrape interval: %v", *scrapeInterval)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and register the exporter
	exporter, err := NewWalgExporter(*walgPath, *scrapeInterval)
	if err != nil {
		log.Fatalf("Failed to create exporter: %v", err)
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
		log.Printf("Starting HTTP server on %s", *listenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Received shutdown signal, shutting down gracefully...")

	// Cancel context to stop exporter
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Println("Exporter shutdown complete")
}
