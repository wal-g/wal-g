package s3

import (
	"context"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestS3MaxIdleConnsPerHostSetting(t *testing.T) {
	defaultMaxIdleConns := http.DefaultTransport.(*http.Transport).MaxIdleConns
	for _, tc := range []struct {
		name             string
		settings         map[string]string
		perHost, maxIdle int
	}{
		{"default", nil, 256, 256},
		{"below the total limit", map[string]string{maxIdleConnsPerHostSetting: "2"}, 2, defaultMaxIdleConns},
		{"above the total limit", map[string]string{maxIdleConnsPerHostSetting: "512"}, 512, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateAWSEnv(t)
			settings := map[string]string{endpointSetting: "http://s3.example.test"}
			maps.Copy(settings, tc.settings)
			folder := configureTestStorage(t, settings)
			client, ok := folder.s3API.(*s3.Client)
			require.True(t, ok)
			transport := underlyingTransport(t, client.Options().HTTPClient)
			require.Equal(t, tc.perHost, transport.MaxIdleConnsPerHost)
			require.Equal(t, tc.maxIdle, transport.MaxIdleConns)
		})
	}
}

func TestS3RejectsInvalidMaxIdleConnsPerHost(t *testing.T) {
	for _, value := range []string{"0", "-1", "1.5", "many"} {
		t.Run(value, func(t *testing.T) {
			settings := map[string]string{
				regionSetting:              "us-east-1",
				uploadConcurrencySetting:   "1",
				maxIdleConnsPerHostSetting: value,
			}
			_, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", settings)
			require.ErrorContains(t, err, maxIdleConnsPerHostSetting)
		})
	}
}

func TestS3MaxIdleConnsPerHostPreservesConfigHash(t *testing.T) {
	isolateAWSEnv(t)
	settings := map[string]string{
		regionSetting:            "us-east-1",
		endpointSetting:          "http://s3.example.test",
		uploadConcurrencySetting: "1",
	}
	unset, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", settings)
	require.NoError(t, err)
	settings[maxIdleConnsPerHostSetting] = "2"
	set, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", settings)
	require.NoError(t, err)
	require.Equal(t, unset.ConfigHash(), set.ConfigHash())
}

func TestS3ConnectionReuseAcrossConcurrentRounds(t *testing.T) {
	const concurrency, rounds = 16, 4
	for _, tc := range []struct {
		name     string
		settings map[string]string
		opened   int
	}{
		{"default", nil, concurrency},
		{"two idle connections", map[string]string{maxIdleConnsPerHostSetting: "2"}, concurrency + (rounds-1)*(concurrency-2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.opened, countOpenedConnections(t, tc.settings, concurrency, rounds))
		})
	}
}

type requestBarrier struct {
	arrived chan struct{}
	release chan struct{}
}

func countOpenedConnections(t *testing.T, settings map[string]string, concurrency, rounds int) int {
	t.Helper()
	var opened atomic.Int32
	var barrier atomic.Pointer[requestBarrier]
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b := barrier.Load()
		b.arrived <- struct{}{}
		select {
		case <-b.release:
		case <-r.Context().Done():
		}
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	isolateAWSEnv(t)
	all := map[string]string{
		endpointSetting:        server.URL,
		forcePathStyleSetting:  "true",
		maxRetriesSetting:      "0",
		accessKeyIDSetting:     "key",
		secretAccessKeySetting: "secret",
	}
	maps.Copy(all, settings)
	folder := configureTestStorage(t, all)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for range rounds {
		b := &requestBarrier{arrived: make(chan struct{}, concurrency), release: make(chan struct{})}
		barrier.Store(b)
		var requests sync.WaitGroup
		for range concurrency {
			requests.Go(func() {
				released := make(chan struct{})
				trace := &httptrace.ClientTrace{PutIdleConn: func(error) { close(released) }}
				if _, err := folder.Exists(httptrace.WithClientTrace(ctx, trace), "object"); err != nil {
					t.Error(err)
					return
				}
				select {
				case <-released:
				case <-ctx.Done():
					t.Error("connection was not returned to the pool or closed")
				}
			})
		}
		for range concurrency {
			select {
			case <-b.arrived:
			case <-ctx.Done():
				t.Error("not all requests of the round reached the server")
			}
		}
		close(b.release)
		requests.Wait()
		if t.Failed() {
			t.FailNow()
		}
	}
	return int(opened.Load())
}
