package progress

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadETAAndStageReset(t *testing.T) {
	var out bytes.Buffer
	p := newTracker(&out, "backup-fetch", 160)
	ctx := WithTracker(context.Background(), p)
	p.bytes.Store(4096) // Earlier metadata/transfers must not count toward this backup.
	SetStage(ctx, "restoring full base_1")
	BeginDownload(ctx, 4096)
	started := p.download.started
	p.render(started)
	assert.Contains(t, out.String(), "0 B/4.0 KiB | ETA --")
	out.Reset()
	p.bytes.Add(1024)
	p.render(started.Add(time.Second))
	assert.Contains(t, out.String(), "1.0 KiB/4.0 KiB | ETA ~3s")
	out.Reset()
	p.render(started.Add(6 * time.Second))
	assert.Contains(t, out.String(), "(idle 5s)")
	assert.Contains(t, out.String(), "ETA --")
	out.Reset()
	p.bytes.Add(1024)
	p.render(started.Add(7 * time.Second))
	assert.NotContains(t, out.String(), "idle")
	assert.Contains(t, out.String(), "2.0 KiB/4.0 KiB | ETA ~2s")
	out.Reset()
	p.bytes.Add(2048)
	p.render(started.Add(8 * time.Second))
	assert.Contains(t, out.String(), "4.0 KiB/4.0 KiB | ETA 0s")
	out.Reset()
	SetStage(ctx, "reading backup metadata: base_2")
	p.render(started.Add(9 * time.Second))
	assert.NotContains(t, out.String(), "ETA")
	BeginDownload(ctx, 2048)
	out.Reset()
	p.render(p.download.started)
	assert.Contains(t, out.String(), "0 B/2.0 KiB | ETA --")
}

func TestDownloadETAUnknownSizesRetriesAndOverrun(t *testing.T) {
	for _, total := range []int64{0, -1} {
		p := newTracker(io.Discard, "backup-fetch", 160)
		BeginDownload(WithTracker(context.Background(), p), total)
		assert.NotContains(t, p.formatLine(p.started.Add(time.Second), 160), "ETA")
	}
	for _, retry := range []bool{false, true} {
		var out bytes.Buffer
		p := newTracker(&out, "backup-fetch", 160)
		ctx := WithTracker(context.Background(), p)
		BeginDownload(ctx, 4)
		read := func(name, data string) {
			r := TrackDownload(ctx, name, io.NopCloser(strings.NewReader(data)))
			_, err := io.Copy(io.Discard, r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
		}
		read("a", "ab")
		if retry {
			read("a", "ab")
		} else {
			read("b", "cde")
		}
		p.render(p.started.Add(time.Second))
		assert.NotContains(t, out.String(), "ETA", "must not estimate after retry or size mismatch")
	}
	r := &closeReader{Reader: strings.NewReader("payload")}
	assert.Same(t, r, TrackDownload(context.Background(), "part", r))
}

func TestDownloadETAFitsWithStageAndIdle(t *testing.T) {
	var out bytes.Buffer
	p := newTracker(&out, "backup-fetch", 80)
	ctx := WithTracker(context.Background(), p)
	SetStage(ctx, "restoring delta base_0000000100000000000000A2_D_0000000100000000000000A1")
	BeginDownload(ctx, 4096)
	started := p.download.started
	p.bytes.Add(1024)
	p.render(started.Add(time.Second))
	frame := strings.TrimPrefix(out.String(), "\r\x1b[2K")
	assert.Less(t, len([]rune(frame)), 80)
	assert.Contains(t, frame, "restoring delta")
	assert.Contains(t, frame, "1.0 KiB/4.0 KiB | ETA ~3s")
	out.Reset()
	p.render(started.Add(6 * time.Second))
	frame = strings.TrimPrefix(out.String(), "\r\x1b[2K")
	assert.Less(t, len([]rune(frame)), 80)
	assert.Contains(t, frame, "restoring delta")
	assert.Contains(t, frame, " (idle 5s)")
	assert.Contains(t, frame, "ETA --")
}
