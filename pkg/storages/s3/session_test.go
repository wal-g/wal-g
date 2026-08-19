package s3

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateSessionWithHTTPSEndpointSourceTwice(t *testing.T) {
	originalTransport := http.DefaultClient.Transport
	t.Cleanup(func() {
		http.DefaultClient.Transport = originalTransport
	})

	config := &Config{
		Secrets:          &Secrets{},
		Region:           "us-east-1",
		Endpoint:         "https://s3.example.com",
		EndpointSource:   "https://endpoint-source.example.com",
		EndpointPort:     "443",
		EndpointProtocol: "https",
	}

	first, err := createSession(config)
	require.NoError(t, err)
	second, err := createSession(config)
	require.NoError(t, err)

	require.NotSame(t, first.Config.HTTPClient, second.Config.HTTPClient)
	assertSessionTransport(t, first.Config.HTTPClient.Transport, "s3.example.com")
	assertSessionTransport(t, second.Config.HTTPClient.Transport, "s3.example.com")
}

func assertSessionTransport(t *testing.T, roundTripper http.RoundTripper, serverName string) {
	t.Helper()

	logging, ok := roundTripper.(*loggingTransport)
	require.True(t, ok)
	transport, ok := logging.underlying.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.TLSClientConfig)
	require.Equal(t, serverName, transport.TLSClientConfig.ServerName)
}
