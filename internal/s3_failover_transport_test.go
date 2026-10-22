package internal_test

import (
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestS3PrimaryDiscoveryUnchangedWithFailover(t *testing.T) {
	for _, mode := range []string{"absent", "empty", "configured", "configured-source"} {
		t.Run(mode, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			original := http.DefaultTransport
			transport := original.(*http.Transport).Clone()
			transport.Proxy = nil
			transport.DisableKeepAlives = false
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
			var primaryPuts, secondaryPuts, discoveries, secondaryDiscoveries atomic.Int32
			server := func(counter *atomic.Int32) *httptest.Server {
				srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPut && r.Method != http.MethodHead {
						t.Errorf("unexpected request: %s", r.Method)
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if r.Method == http.MethodPut {
						counter.Add(1)
					}
				}))
				t.Cleanup(srv.Close)
				return srv
			}
			primary, secondary := server(&primaryPuts), server(&secondaryPuts)
			host, port, err := net.SplitHostPort(primary.Listener.Addr().String())
			require.NoError(t, err)
			_, secondaryPort, err := net.SplitHostPort(secondary.Listener.Addr().String())
			require.NoError(t, err)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/secondary" {
					secondaryDiscoveries.Add(1)
				} else {
					discoveries.Add(1)
				}
				_, _ = io.WriteString(w, host)
			}))
			defer source.Close()
			caFile := filepath.Join(t.TempDir(), "ca.pem")
			require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
				Type: "CERTIFICATE", Bytes: primary.Certificate().Raw,
			}), 0600))
			// Discovery returns the same endpoint: this test deliberately does not change
			// or depend on the separate SDK v2 endpoint-rewrite ordering bug.
			for key, value := range map[string]string{
				"WALG_S3_PREFIX": "s3://primary/archive", "AWS_ENDPOINT": primary.URL,
				"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
				"AWS_S3_FORCE_PATH_STYLE": "true", "S3_CA_CERT_FILE": caFile,
				"S3_ENDPOINT_SOURCE": source.URL, "S3_ENDPOINT_PORT": port,
				"S3_SKIP_VALIDATION": "true", "S3_MAX_RETRIES": "0", "UPLOAD_CONCURRENCY": "1",
			} {
				viper.Set(key, value)
			}
			if mode == "empty" {
				viper.Set("WALG_FAILOVER_STORAGES", map[string]any{})
			} else if strings.HasPrefix(mode, "configured") {
				settings := map[string]any{
					"WALG_S3_PREFIX": "s3://secondary/archive", "AWS_ENDPOINT": secondary.URL,
					"S3_ENDPOINT_SOURCE": "",
				}
				if mode == "configured-source" {
					settings["S3_ENDPOINT_SOURCE"] = source.URL + "/secondary"
					settings["S3_ENDPOINT_PORT"] = secondaryPort
				}
				viper.Set("WALG_FAILOVER_STORAGES", map[string]any{"secondary": settings})
			}
			first, err := internal.ConfigureStorage(t.Context())
			require.NoError(t, err)
			failovers, err := internal.ConfigureFailoverStorages(t.Context())
			require.NoError(t, err)
			if len(failovers) > 0 {
				// A secondary probe before activation must not change primary transport settings.
				_, err := failovers["secondary"].RootFolder().Exists(t.Context(), "wal")
				require.NoError(t, err)
			}
			require.False(t, transport.DisableKeepAlives)
			require.NoError(t, first.RootFolder().PutObject(t.Context(), "wal", strings.NewReader("payload")))
			require.True(t, transport.DisableKeepAlives, "primary must retain its original discovery behavior")
			if len(failovers) > 0 {
				require.NoError(t, failovers["secondary"].RootFolder().PutObject(t.Context(), "wal", strings.NewReader("payload")))
				require.EqualValues(t, 1, secondaryPuts.Load())
			} else {
				require.Empty(t, failovers)
			}
			require.NoError(t, first.RootFolder().PutObject(t.Context(), "wal2", strings.NewReader("payload")))
			require.EqualValues(t, 2, primaryPuts.Load())
			require.EqualValues(t, 2, discoveries.Load(), "secondary must not inherit primary discovery")
			if mode == "configured-source" {
				require.EqualValues(t, 2, secondaryDiscoveries.Load())
			} else {
				require.Zero(t, secondaryDiscoveries.Load())
			}
		})
	}
}
