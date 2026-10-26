package progress

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests catch lost bytes (including reads returning data and an error),
// animation corrupting logs, and background output surviving cancellation.
func TestTrackReaderCountsConcurrentReadsAndPreservesErrors(t *testing.T) {
	p := newTracker(io.Discard, "backup-push", 120)
	ctx := WithTracker(context.Background(), p)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader("12345")))
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	wantErr := errors.New("read failed")
	reader := TrackReader(ctx, &lastRead{err: wantErr})
	n, err := reader.Read(make([]byte, 10))
	assert.Equal(t, 3, n)
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, int64(43), p.bytes.Load())
}

type lastRead struct{ err error }

func (r *lastRead) Read(b []byte) (int, error) {
	return copy(b, "abc"), r.err
}

func TestDisabledTrackingPreservesReader(t *testing.T) {
	r := strings.NewReader("payload")
	assert.Same(t, r, TrackReader(context.Background(), r))
	// io.ReadCloser implementations need not be comparable interface values.
	var closer io.ReadCloser = sliceReadCloser("abc")
	data, err := io.ReadAll(TrackReadCloser(context.Background(), closer))
	require.NoError(t, err)
	assert.Equal(t, "abc", string(data))
}

type sliceReadCloser []byte

func (r sliceReadCloser) Read(b []byte) (int, error) { return copy(b, r), io.EOF }
func (r sliceReadCloser) Close() error               { return nil }

func TestReadCloserForwardsClose(t *testing.T) {
	p := newTracker(io.Discard, "backup-fetch", 120)
	wantErr := errors.New("close failed")
	r := &closeReader{Reader: strings.NewReader("payload"), err: wantErr}
	tracked := TrackReadCloser(WithTracker(context.Background(), p), r)
	data, err := io.ReadAll(tracked)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(data))
	assert.Equal(t, int64(7), p.bytes.Load())
	assert.ErrorIs(t, tracked.Close(), wantErr)
	assert.True(t, r.closed)
}

type closeReader struct {
	io.Reader
	err    error
	closed bool
}

func (r *closeReader) Close() error { r.closed = true; return r.err }

func TestRenderShowsRateAndIdleWithoutInventingPercentage(t *testing.T) {
	var out bytes.Buffer
	p := newTracker(&out, "backup-push", 160)
	p.bytes.Store(2 * 1024 * 1024)
	p.render(p.started.Add(time.Second))
	frame := out.String()
	for _, want := range []string{"backup-push", "2.0 MiB", "2.0 MiB/s", "1s"} {
		assert.Contains(t, frame, want)
	}
	assert.NotContains(t, frame, "%")
	assert.NotContains(t, frame, "ETA")
	out.Reset()
	p.render(p.started.Add(11 * time.Second))
	assert.Contains(t, out.String(), "0 B/s")
	assert.Contains(t, out.String(), "idle 10s")
}

func TestIdleKeepsCurrentStageVisible(t *testing.T) {
	for _, stage := range []string{
		"stopping backup/waiting for WAL",
		"restoring delta base_0000000100000000000000A2_D_0000000100000000000000A1",
	} {
		t.Run(stage, func(t *testing.T) {
			var out bytes.Buffer
			p := newTracker(&out, "backup-fetch", 80)
			ctx := WithTracker(context.Background(), p)
			SetStage(ctx, stage)
			p.render(p.started.Add(10 * time.Second))
			frame := strings.TrimPrefix(out.String(), "\r\x1b[2K")
			assert.Contains(t, frame, " (idle 10s)")
			assert.Less(t, len([]rune(frame)), 80)
			if stage == "stopping backup/waiting for WAL" {
				assert.Contains(t, frame, "stopping backup/waiting for WAL (idle 10s)")
			} else {
				assert.Contains(t, frame, "restoring delta base_")
			}

			out.Reset()
			SetStage(ctx, "uploading metadata")
			p.render(p.started.Add(11 * time.Second))
			assert.Contains(t, out.String(), "uploading metadata (idle 11s)")
			assert.NotContains(t, out.String(), "restoring delta")
			assert.NotContains(t, out.String(), "waiting for WAL")

			out.Reset()
			p.bytes.Add(1024)
			p.render(p.started.Add(12 * time.Second))
			assert.Contains(t, out.String(), "uploading metadata")
			assert.NotContains(t, out.String(), "idle")
		})
	}
}

func TestLogWriteClearsProgressBeforeMessage(t *testing.T) {
	var out bytes.Buffer
	p := newTracker(&out, "backup-fetch", 80)
	p.render(p.started.Add(time.Second))
	out.Reset()
	message := []byte("WARNING: retrying download\n")
	n, err := p.Write(message)
	require.NoError(t, err)
	assert.Equal(t, len(message), n)
	assert.Equal(t, "\r\x1b[2KWARNING: retrying download\n", out.String())
}

func TestResultOutputClearsSharedTerminalWithoutChangingJSON(t *testing.T) {
	for _, sharedTerminal := range []bool{false, true} {
		var stderr, stdout bytes.Buffer
		p := newTracker(&stderr, "backup-push", 80)
		p.render(p.started)
		stderr.Reset()
		result := &stdout
		if sharedTerminal {
			result = &stderr
		}
		const json = "{\"Name\":\"backup\"}\n"
		ctx := WithTracker(context.Background(), p)
		_, err := io.WriteString(Output(ctx, result), json)
		require.NoError(t, err)
		if sharedTerminal {
			assert.Equal(t, "\r\x1b[2K"+json, result.String())
		} else {
			assert.Equal(t, json, stdout.String())
			assert.Equal(t, "\r\x1b[2K", stderr.String())
		}
	}
}

func TestFramesFitTerminalAndSanitizeStage(t *testing.T) {
	var out bytes.Buffer
	p := newTracker(&out, "backup-push", 40)
	ctx := WithTracker(context.Background(), p)
	SetStage(ctx, "uploading\n\x1b[31m")
	p.render(p.started.Add(time.Second))
	frame := strings.TrimPrefix(out.String(), "\r\x1b[2K")
	assert.Less(t, len(frame), 40)
	assert.False(t, strings.ContainsAny(frame, "\r\n\x1b"), "unsafe frame: %q", frame)
}

func TestStopAndCancellationFinishOutput(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		var out bytes.Buffer
		ctx, cancel := context.WithCancel(context.Background())
		p := Start(ctx, &out, "backup-fetch", 80)
		if cancelFirst {
			cancel()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Fatal("renderer did not stop on cancellation")
			}
		}
		p.Stop()
		p.Stop()
		cancel()
		assert.True(t, strings.HasSuffix(out.String(), "\r\x1b[2K"), "progress line was not cleared: %q", out.String())
		out.Reset()
		_, _ = p.Write([]byte("done\n"))
		assert.Equal(t, "done\n", out.String())
	}
}
