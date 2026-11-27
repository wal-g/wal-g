package statistics

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
)

func stubFatal(t *testing.T) *[]error {
	t.Helper()
	var calls []error
	orig := fatalfOnError
	fatalfOnError = func(_ string, err error) { calls = append(calls, err) }
	t.Cleanup(func() { fatalfOnError = orig })
	return &calls
}

func TestFatalOnErrorPushesMetricsBeforeExit(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer conn.Close()

	viper.Set(conf.StatsdAddressSetting, conn.LocalAddr().String())
	t.Cleanup(func() { viper.Set(conf.StatsdAddressSetting, "") })

	calls := stubFatal(t)
	WalgMetrics.UploadedFilesFailedTotal.Inc()

	uploadErr := errors.New("upload failed")
	FatalOnError(uploadErr)
	require.Equal(t, []error{uploadErr}, *calls)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	var received strings.Builder
	buf := make([]byte, 65536)
	for !strings.Contains(received.String(), "walg_uploader_uploaded_files_failed_total:") {
		n, _, err := conn.ReadFrom(buf)
		require.NoError(t, err, "metrics received so far: %q", received.String())
		received.Write(buf[:n])
	}
}

func TestFatalOnErrorNilDoesNothing(t *testing.T) {
	calls := stubFatal(t)
	FatalOnError(nil)
	FatalfOnError("failed: %v", nil)
	require.Empty(t, *calls)
}
