package testutils

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/webserver"
)

func RequireServesPprof(t *testing.T, start func()) {
	t.Helper()
	require.Nil(t, webserver.DefaultWebServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	for key, value := range map[string]any{conf.HTTPListen: addr, conf.HTTPExposePprof: true} {
		previous := viper.Get(key)
		viper.Set(key, value)
		defer viper.Set(key, previous)
	}
	defer func() {
		if webserver.DefaultWebServer != nil {
			_ = webserver.DefaultWebServer.Shutdown(t.Context())
			webserver.DefaultWebServer = nil
		}
	}()

	start()

	require.NotNil(t, webserver.DefaultWebServer, "web server is not started")
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	require.Eventually(t, func() bool {
		resp, err := client.Get("http://" + addr + "/debug/pprof/")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond)
}
