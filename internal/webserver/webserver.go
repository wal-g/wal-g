package webserver

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
)

// WebServer defines web-server interface.
type WebServer interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
	Serve() error
	Shutdown(ctx context.Context) error
}

var DefaultWebServer WebServer

// SimpleWebServer is naive implementation of WebServer.
// is not thread-safe
type SimpleWebServer struct {
	*http.Server
	*http.ServeMux
	running bool
}

// NewSimpleWebServer builds SimpleWebServer.
func NewSimpleWebServer(addr string) *SimpleWebServer {
	mux := http.NewServeMux()
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return &SimpleWebServer{srv, mux, false}
}

// Serve starts server.
// TODO: handle errors properly
func (sw *SimpleWebServer) Serve() error {
	if sw.running {
		return fmt.Errorf("already running")
	}
	sw.running = true
	go func() {
		_ = sw.ListenAndServe()
	}()

	return nil
}

// Shutdown stops running server.
func (sw *SimpleWebServer) Shutdown(ctx context.Context) error {
	if !sw.running {
		return fmt.Errorf("not started")
	}
	sw.running = false
	return sw.Server.Shutdown(ctx)
}

// EnablePprofEndpoints exposes pprof http endpoints.
func EnablePprofEndpoints(ws WebServer) {
	ws.HandleFunc("/debug/pprof/", pprof.Index)
	ws.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	ws.HandleFunc("/debug/pprof/profile", pprof.Profile)
	ws.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	ws.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// EnableExpVarEndpoints exposes expvar http endpoints.
func EnableExpVarEndpoints(ws WebServer) {
	ws.HandleFunc("/debug/vars", expvar.Handler().ServeHTTP)
}

// SetDefaultWebServer sets default server instance
// is not thread-safe
func SetDefaultWebServer(ws WebServer) error {
	if DefaultWebServer != nil {
		return fmt.Errorf("default web server has been already configured")
	}
	DefaultWebServer = ws
	return nil
}