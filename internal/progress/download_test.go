package progress

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadETAAndStageReset(t *testing.T) {
	p := newTracker(io.Discard, "backup-fetch", 160)
	ctx := WithTracker(t.Context(), p)
	p.bytes.Store(4096) // Reads from earlier backups are excluded from this total.
	SetStage(ctx, "restoring full base_1")
	BeginDownload(ctx, 4096)
	started := p.download.started
	for _, step := range []struct {
		name                   string
		after                  time.Duration
		added, downloaded, eta int64 // Negative ETA means unknown.
		idle                   float64
	}{
		{"start", 0, 0, 0, -1, 0},
		{"transfer", time.Second, 1024, 1024, 3, 0},
		{"idle", 6 * time.Second, 0, 1024, -1, 5},
		{"resume", 7 * time.Second, 1024, 2048, 2, 0},
		{"read complete", 8 * time.Second, 2048, 4096, 0, 0},
	} {
		p.bytes.Add(step.added)
		s := p.snapshot(started.Add(step.after))
		require.NotNil(t, s.DownloadedBytes, step.name)
		assert.Equal(t, step.downloaded, *s.DownloadedBytes, step.name)
		assert.Equal(t, step.idle, s.IdleSeconds, step.name)
		if step.eta < 0 {
			assert.Nil(t, s.ETASeconds, step.name)
		} else {
			require.NotNil(t, s.ETASeconds, step.name)
			assert.Equal(t, step.eta, *s.ETASeconds, step.name)
		}
	}
	SetStage(ctx, "reading backup metadata: base_2")
	assert.Nil(t, p.snapshot(started.Add(9*time.Second)).TotalBytes)
	BeginDownload(ctx, 2048)
	s := p.snapshot(p.download.started)
	require.NotNil(t, s.DownloadedBytes)
	require.NotNil(t, s.TotalBytes)
	assert.Zero(t, *s.DownloadedBytes)
	assert.Equal(t, int64(2048), *s.TotalBytes)
	assert.Nil(t, s.ETASeconds)
}

func TestDownloadETAUnknownSizesRetriesAndOverrun(t *testing.T) {
	for _, total := range []int64{0, -1} {
		p := newTracker(io.Discard, "backup-fetch", 160)
		BeginDownload(WithTracker(t.Context(), p), total)
		assert.NotContains(t, p.formatLine(p.started.Add(time.Second), 160), "ETA")
	}
	for _, retry := range []bool{false, true} {
		var out bytes.Buffer
		p := newTracker(&out, "backup-fetch", 160)
		ctx := WithTracker(t.Context(), p)
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
	assert.Same(t, r, TrackDownload(t.Context(), "part", r))
}
