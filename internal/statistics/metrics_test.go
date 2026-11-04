package statistics

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const skippedMetric = "walg_wal_push_skipped_total"

func listenStatsd(t *testing.T) net.PacketConn {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// receiveLines returns everything already delivered: pushMetrics flushes before returning.
func receiveLines(t *testing.T, conn net.PacketConn) []string {
	t.Helper()
	var lines []string
	buf := make([]byte, 65536)
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return lines
		}
		lines = append(lines, strings.Split(string(buf[:n]), "\n")...)
	}
}

func pushAndReceive(t *testing.T, conn net.PacketConn) []string {
	t.Helper()
	require.NoError(t, pushMetrics(conn.LocalAddr().String(), nil))
	return receiveLines(t, conn)
}

func TestPushMetricsSendsCounterIncrements(t *testing.T) {
	conn := listenStatsd(t)
	pushAndReceive(t, conn)

	WalgMetrics.WalPushSkippedTotal.Inc()
	assert.Contains(t, pushAndReceive(t, conn), skippedMetric+":1|c")

	WalgMetrics.WalPushSkippedTotal.Add(2)
	assert.Contains(t, pushAndReceive(t, conn), skippedMetric+":2|c")

	assert.Contains(t, pushAndReceive(t, conn), skippedMetric+":0|c")
}

func TestPushMetricsTracksCounterLabelsSeparately(t *testing.T) {
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "walg_test_labelled_total"}, []string{"kind"})
	prometheus.MustRegister(counter)
	defer prometheus.Unregister(counter)
	conn := listenStatsd(t)

	counter.WithLabelValues("first").Add(2)
	counter.WithLabelValues("second").Add(1)
	pushAndReceive(t, conn)

	counter.WithLabelValues("second").Add(3)
	assert.Contains(t, pushAndReceive(t, conn), "walg_test_labelled_total,kind=second:3|c")
}

func TestPushMetricsSendsEachUploadTimeOnce(t *testing.T) {
	const timer = "walg_s3_upload_time"
	conn := listenStatsd(t)
	WalgMetrics.S3UploadTime.Set(0)
	delete(pushedValues, timer)
	for _, line := range pushAndReceive(t, conn) {
		assert.NotContains(t, line, timer, "no upload has happened yet")
	}

	WriteS3UploadTimeMetric(5 * time.Millisecond)
	assert.Contains(t, pushAndReceive(t, conn), timer+":5|ms")

	for _, line := range pushAndReceive(t, conn) {
		assert.NotContains(t, line, timer, "no upload happened since the previous push")
	}

	WriteS3UploadTimeMetric(7 * time.Millisecond)
	assert.Contains(t, pushAndReceive(t, conn), timer+":7|ms")
}

func TestPushMetricsResendsGauges(t *testing.T) {
	conn := listenStatsd(t)
	WalgMetrics.S3BytesWritten.Set(10)
	assert.Contains(t, pushAndReceive(t, conn), "walg_s3_bytes_written:10|g")
	assert.Contains(t, pushAndReceive(t, conn), "walg_s3_bytes_written:10|g")
}

func TestPushMetricsDoesNotQueueBehindARunningPush(t *testing.T) {
	conn := listenStatsd(t)
	pushAndReceive(t, conn)
	WalgMetrics.WalPushSkippedTotal.Inc()

	pushMu.Lock()
	done := make(chan error, 1)
	go func() { done <- pushMetrics(conn.LocalAddr().String(), nil) }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Error("the push waits for the running one")
	}
	pushMu.Unlock()
	assert.Empty(t, receiveLines(t, conn))

	assert.Contains(t, pushAndReceive(t, conn), skippedMetric+":1|c", "the next push sends the increment")
}

func TestPushMetricsCountsEachIncrementOnceWhenConcurrent(t *testing.T) {
	const pushes = 20
	conn := listenStatsd(t)
	pushAndReceive(t, conn)

	var wg sync.WaitGroup
	for range pushes {
		wg.Go(func() {
			WalgMetrics.WalPushSkippedTotal.Inc()
			assert.NoError(t, pushMetrics(conn.LocalAddr().String(), nil))
		})
	}
	wg.Wait()
	require.NoError(t, pushMetrics(conn.LocalAddr().String(), nil)) // whatever overlapping pushes skipped

	total := 0
	for _, line := range receiveLines(t, conn) {
		if value, ok := strings.CutPrefix(line, skippedMetric+":"); ok {
			increment, err := strconv.Atoi(strings.TrimSuffix(value, "|c"))
			require.NoError(t, err)
			total += increment
		}
	}
	assert.Equal(t, pushes, total)
}
