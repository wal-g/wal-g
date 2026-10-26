// Package progress displays an indeterminate progress bar for streaming backups.
// Byte counts describe data consumed by readers, not storage acknowledgements.
package progress

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wal-g/wal-g/utility"
	"golang.org/x/term"
)

const (
	refreshInterval = 200 * time.Millisecond
	idleThreshold   = 5 * time.Second
)

type contextKey struct{}

// Tracker counts concurrent streams and serializes progress with log messages.
type Tracker struct {
	bytes         atomic.Int64
	stage         atomic.Pointer[string]
	mu            sync.Mutex
	out           io.Writer
	label         string
	fallbackWidth int
	started       time.Time
	sampled       time.Time
	lastProgress  time.Time
	lastBytes     int64
	frame         int
	visible       bool
	stopped       bool
	stop          chan struct{}
	done          chan struct{}
	once          sync.Once
}

func newTracker(out io.Writer, label string, fallbackWidth int) *Tracker {
	if fallbackWidth <= 0 {
		fallbackWidth = 80
	}
	now := time.Now()
	return &Tracker{
		out: out, label: label, fallbackWidth: fallbackWidth,
		started: now, sampled: now, lastProgress: now,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Start renders progress until Stop or context cancellation. Output errors never
// interrupt a backup. The terminal width is refreshed on every frame; fallbackWidth
// is used when it cannot be determined, with zero selecting an 80-column default.
func Start(ctx context.Context, out io.Writer, label string, fallbackWidth int) *Tracker {
	p := newTracker(out, label, fallbackWidth)
	p.render(p.started)
	go func() {
		defer close(p.done)
		defer p.finish()
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stop:
				return
			case now := <-ticker.C:
				p.render(now)
			}
		}
	}()
	return p
}

// Stop waits for the renderer and removes its last line. It is safe to repeat.
func (p *Tracker) Stop() {
	p.once.Do(func() { close(p.stop) })
	<-p.done
}

func (p *Tracker) finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.clear()
}

func (p *Tracker) clear() {
	if p.visible {
		_, _ = io.WriteString(p.out, "\r\x1b[2K")
		p.visible = false
	}
}

// Write prints a log message on a clean line. The next tick redraws progress.
func (p *Tracker) Write(b []byte) (int, error) {
	return p.writeTo(p.out, b)
}

func (p *Tracker) writeTo(out io.Writer, b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clear()
	return out.Write(b)
}

// Output coordinates command results with progress, including when stdout and
// stderr share a terminal. Terminal control bytes only go to the progress output.
func Output(ctx context.Context, out io.Writer) io.Writer {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		return &outputWriter{tracker: p, out: out}
	}
	return out
}

type outputWriter struct {
	tracker *Tracker
	out     io.Writer
}

func (w *outputWriter) Write(b []byte) (int, error) { return w.tracker.writeTo(w.out, b) }

func (p *Tracker) render(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	// Refresh the width on every frame so resizing does not cause wrapped lines
	// that cannot be removed by clearing the current line.
	width := p.fallbackWidth
	if terminal, ok := p.out.(interface{ Fd() uintptr }); ok {
		if columns, _, err := term.GetSize(int(terminal.Fd())); err == nil && columns > 0 {
			width = columns
		}
	}
	line := fitText(p.formatLine(now, width-1), width-1)
	_, _ = fmt.Fprintf(p.out, "\r\x1b[2K%s", line)
	p.visible = true
}

// formatLine updates the rate sample and animation frame while p.mu is held.
func (p *Tracker) formatLine(now time.Time, width int) string {
	current := p.bytes.Load()
	var rate float64
	if seconds := now.Sub(p.sampled).Seconds(); seconds > 0 {
		rate = float64(current-p.lastBytes) / seconds
	}
	if current != p.lastBytes {
		p.lastProgress = now
	}
	p.sampled, p.lastBytes = now, current
	bar := []byte("            ")
	position := p.frame % (2 * (len(bar) - 3))
	if position > len(bar)-3 {
		position = 2*(len(bar)-3) - position
	}
	copy(bar[position:], "===")
	p.frame++
	line := fmt.Sprintf("%s [%s] %s | %s/s | %s", p.label, bar,
		formatBytes(float64(current)), formatBytes(rate), now.Sub(p.started).Truncate(time.Second))
	var status, idleSuffix string
	if stage := p.stage.Load(); stage != nil {
		status = *stage
	}
	if idle := now.Sub(p.lastProgress).Truncate(time.Second); idle >= idleThreshold {
		idleSuffix = fmt.Sprintf(" (idle %s)", idle)
	}
	// Keep the stage and its idle suffix visible before transfer statistics.
	// Long backup names may be shortened, but must not push the idle time off screen.
	status = fitText(status, width-len(idleSuffix)) + idleSuffix
	if status != "" {
		line = status + " | " + line
	}
	return line
}

func fitText(text string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(strings.Map(func(r rune) rune {
		if r < ' ' || r == 127 {
			return -1
		}
		return r
	}, text))
	if len(runes) <= width {
		return string(runes)
	}
	return string(runes[:width])
}

func formatBytes(n float64) string {
	units := [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f B", n)
	}
	return fmt.Sprintf("%.1f %s", n, units[i])
}

// WithTracker enables tracking for this command and its concurrent workers.
func WithTracker(ctx context.Context, p *Tracker) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

// SetStage changes the displayed stage; it is a no-op without a tracker.
func SetStage(ctx context.Context, stage string) {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		p.stage.Store(&stage)
	}
}

// TrackReader counts each byte consumed, including bytes returned with an error.
// With no tracker it returns the original reader and preserves its interfaces.
func TrackReader(ctx context.Context, r io.Reader) io.Reader {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		return utility.NewWithSizeReader(r, &p.bytes)
	}
	return r
}

// TrackReadCloser preserves ownership and close errors of a downloaded stream.
func TrackReadCloser(ctx context.Context, r io.ReadCloser) io.ReadCloser {
	if p, _ := ctx.Value(contextKey{}).(*Tracker); p != nil {
		return utility.NewWithSizeReadCloser(r, &p.bytes)
	}
	return r
}
