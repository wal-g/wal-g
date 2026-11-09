package progress

import (
	"context"
	"io"
	"math"
	"time"
)

// download tracks one backup's selected archives. All fields are protected by
// Tracker.mu; stream reads still only increment the shared atomic byte counter.
type download struct {
	total, baseline, lastBytes int64
	started, sampled           time.Time
	rate                       float64
	opened                     map[string]struct{}
}

// BeginDownload starts an estimate for the current backup, excluding prior reads.
// A nonpositive total disables the estimate. Call after selecting archives and
// before opening their readers; no additional storage requests are needed.
func BeginDownload(ctx context.Context, total int64) {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.download = nil
		if total > 0 {
			now, current := time.Now(), p.bytes.Load()
			p.download = &download{
				total: total, baseline: current, lastBytes: current,
				started: now, sampled: now, opened: make(map[string]struct{}),
			}
		}
	}
}

// TrackDownload counts a storage reader and detects repeated opens within the
// current backup. Retried bytes cannot be distinguished by the shared counter,
// so a retry falls back to unbounded progress until the next BeginDownload.
func TrackDownload(ctx context.Context, name string, r io.ReadCloser) io.ReadCloser {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		p.mu.Lock()
		if d := p.download; d != nil {
			if _, repeated := d.opened[name]; repeated {
				p.download = nil
			} else {
				d.opened[name] = struct{}{}
			}
		}
		p.mu.Unlock()
	}
	return TrackReadCloser(ctx, r)
}

// estimateDownload samples the transfer rate while Tracker.mu is held. A
// five-second exponential average smooths bursty reads without a sample buffer.
type downloadEstimate struct {
	transferred, total int64
	etaSeconds         *int64
}

func (p *Tracker) estimateDownload(now time.Time, current int64, idle bool) *downloadEstimate {
	d := p.download
	if d == nil {
		return nil
	}
	transferred := current - d.baseline
	if transferred < 0 || transferred > d.total {
		p.download = nil // Storage size changed or reads did not match the selected total.
		return nil
	}
	if seconds := now.Sub(d.sampled).Seconds(); seconds > 0 {
		rate := float64(current-d.lastBytes) / seconds
		if d.rate == 0 {
			d.rate = rate
		} else {
			d.rate += (rate - d.rate) * (1 - math.Exp(-seconds/5))
		}
		d.sampled, d.lastBytes = now, current
	}
	var eta *int64
	switch {
	case transferred == d.total:
		zero := int64(0)
		eta = &zero // All expected bytes were read; extraction/validation may still be running.
	case idle:
		d.rate = 0
	case d.rate > 0 && now.Sub(d.started) >= time.Second:
		seconds := math.Ceil(float64(d.total-transferred) / d.rate)
		if seconds < float64(math.MaxInt64)/float64(time.Second) {
			estimate := int64(seconds)
			eta = &estimate
		}
	}
	return &downloadEstimate{transferred: transferred, total: d.total, etaSeconds: eta}
}
