package s3

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSecondarySessionIsolation(t *testing.T) {
	original := http.DefaultClient
	http.DefaultClient = &http.Client{}
	t.Cleanup(func() { http.DefaultClient.CloseIdleConnections(); http.DefaultClient = original })
	cfg := &Config{Secrets: &Secrets{}, Region: "us-east-1", Endpoint: "https://primary.invalid",
		EndpointSource: "http://discovery.invalid"}
	primary, err := createSession(cfg)
	require.NoError(t, err)
	primaryTransport := primary.Config.HTTPClient.Transport
	require.Same(t, http.DefaultClient, primary.Config.HTTPClient)
	for _, source := range []string{"", "http://secondary-discovery.invalid"} {
		cfg := *cfg
		cfg.Endpoint, cfg.EndpointSource, cfg.IsolateHTTPClient = "https://secondary.invalid", source, true
		secondary, err := createSession(&cfg)
		require.NoError(t, err)
		defer secondary.Config.HTTPClient.CloseIdleConnections()
		require.NotSame(t, primary.Config.HTTPClient, secondary.Config.HTTPClient)
		require.Same(t, primaryTransport, primary.Config.HTTPClient.Transport)
		transport := secondary.Config.HTTPClient.Transport.(*loggingTransport).underlying.(*http.Transport)
		if source == "" {
			if transport.TLSClientConfig != nil {
				require.Empty(t, transport.TLSClientConfig.ServerName)
			}
		} else {
			require.Equal(t, "secondary.invalid", transport.TLSClientConfig.ServerName)
		}
	}
	require.Equal(t, "primary.invalid", primaryTransport.(*loggingTransport).underlying.(*http.Transport).TLSClientConfig.ServerName)
}

func TestSecondaryIsolationPreservesConfigHash(t *testing.T) {
	cfg := &Config{EndpointSource: "http://discovery.invalid"}
	before, err := storage.ComputeConfigHash("s3", cfg)
	require.NoError(t, err)
	cfg.IsolateHTTPClient = true
	after, err := storage.ComputeConfigHash("s3", cfg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestEndpointSourceTransportIsolation(t *testing.T) {
	for _, isolate := range []bool{false, true} {
		t.Run(fmt.Sprintf("isolate=%t", isolate), func(t *testing.T) {
			original := http.DefaultTransport
			transport := original.(*http.Transport).Clone()
			transport.DisableKeepAlives = false
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !r.Close {
					t.Error("discovery must close connections, including after redirects")
				}
				if r.URL.Path == "/redirect" {
					http.Redirect(w, r, "/node", http.StatusFound)
					return
				}
				_, _ = io.WriteString(w, "127.0.0.1")
			}))
			defer source.Close()
			var client *http.Client
			if isolate {
				client = newEndpointSourceClient()
				defer client.CloseIdleConnections()
			}
			got := requestEndpointFromSource(t.Context(), source.URL+"/redirect", "443", client)
			require.Equal(t, "127.0.0.1:443", aws.StringValue(got))
			require.Equal(t, !isolate, transport.DisableKeepAlives)
		})
	}
}
