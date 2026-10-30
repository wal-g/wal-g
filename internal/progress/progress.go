// Package progress displays live transfer progress for streaming backups.
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
	failed        atomic.Bool
	stage         atomic.Pointer[string]
	download      *download
	mu            sync.Mutex
	out           io.Writer
	label         string
	fallbackWidth int
	started       time.Time
	stageStarted  time.Time
	file          *fileReporter
	fileLast      time.Time
	fileStage     string
	wake          chan struct{}
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
		started: now, stageStarted: now, sampled: now, lastProgress: now,
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Start renders progress until Stop or context cancellation. Output errors never
// interrupt a backup. The terminal width is refreshed on every frame; fallbackWidth
// is used when it cannot be determined, with zero selecting an 80-column default.
func Start(ctx context.Context, out io.Writer, label string, fallbackWidth int) *Tracker {
	return StartWithFile(ctx, out, label, fallbackWidth, FileOptions{})
}

// StartWithFile optionally publishes JSON snapshots independently of terminal
// rendering. A nil out disables terminal output; file errors never fail a backup.
func StartWithFile(ctx context.Context, out io.Writer, label string, fallbackWidth int, options FileOptions) *Tracker {
	p := newTracker(out, label, fallbackWidth)
	if options.Directory != "" {
		f := newStateFile(options)
		p.file = startFileReporter(f.write, options.OnError)
	}
	p.render(p.started)
	go func() {
		defer close(p.done)
		status := "completed"
		defer func() { p.finish(status) }()
		interval := refreshInterval
		if out == nil {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				status = "canceled"
				return
			case <-p.stop:
				if ctx.Err() != nil {
					status = "canceled"
				}
				return
			case <-p.wake:
				p.render(time.Now())
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
	if p.file != nil {
		p.file.wait()
	}
}

// StopOnFailure preserves a failed outcome when unwinding a command panic.
func (p *Tracker) StopOnFailure() {
	p.failed.Store(true)
	p.Stop()
}

func (p *Tracker) finish(status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.clear()
	if p.file != nil {
		s := p.snapshot(time.Now())
		if p.failed.Load() {
			status = "failed"
		}
		s.Status = status
		s.FinishedAt = &s.UpdatedAt
		p.file.submit(s)
	}
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
	s := p.snapshot(now)
	if p.out != nil {
		// Refresh terminal width on every frame to avoid wrapped lines after resize.
		width := p.fallbackWidth
		if terminal, ok := p.out.(interface{ Fd() uintptr }); ok {
			if columns, _, err := term.GetSize(int(terminal.Fd())); err == nil && columns > 0 {
				width = columns
			}
		}
		line := fitText(p.formatSnapshot(s, width-1), width-1)
		_, _ = fmt.Fprintf(p.out, "\r\x1b[2K%s", line)
		p.visible = true
	}
	if p.file != nil && (p.out == nil || p.fileLast.IsZero() || s.Stage != p.fileStage || now.Sub(p.fileLast) >= time.Second) {
		p.file.submit(s)
		p.fileLast, p.fileStage = now, s.Stage
	}
}

// formatLine is used by deterministic rendering tests; production samples once
// per tick and shares the same snapshot across both outputs.
func (p *Tracker) formatLine(now time.Time, width int) string {
	return p.formatSnapshot(p.snapshot(now), width)
}

func (p *Tracker) formatSnapshot(s Snapshot, width int) string {
	bar := []byte("            ")
	position := p.frame % (2 * (len(bar) - 3))
	if position > len(bar)-3 {
		position = 2*(len(bar)-3) - position
	}
	copy(bar[position:], "===")
	p.frame++
	elapsed := time.Duration(s.ElapsedSeconds * float64(time.Second)).Truncate(time.Second)
	line := fmt.Sprintf("%s [%s] %s | %s/s | %s", p.label, bar,
		formatBytes(float64(s.TransferredBytes)), formatBytes(s.BytesPerSecond), elapsed)
	status, idleSuffix := s.Stage, ""
	if idle := time.Duration(s.IdleSeconds * float64(time.Second)).Truncate(time.Second); idle >= idleThreshold {
		idleSuffix = fmt.Sprintf(" (idle %s)", idle)
	}
	if s.TotalBytes != nil {
		eta := "--"
		if s.ETASeconds != nil {
			eta = (time.Duration(*s.ETASeconds) * time.Second).String()
			if *s.ETASeconds > 0 {
				eta = "~" + eta
			}
		}
		summary := fmt.Sprintf("%s/%s | ETA %s", formatBytes(float64(*s.DownloadedBytes)), formatBytes(float64(*s.TotalBytes)), eta)
		stageWidth := max(20, width-len(idleSuffix)-len(summary)-3)
		status = fitText(status, stageWidth)
		line = fmt.Sprintf("%s | %s/s | %s", summary, formatBytes(s.BytesPerSecond), elapsed)
	}
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
		p.mu.Lock()
		defer p.mu.Unlock()
		if old := p.stage.Load(); old == nil || *old != stage {
			p.stageStarted = time.Now()
		}
		p.stage.Store(&stage)
		p.download = nil
		if p.file != nil {
			select {
			case p.wake <- struct{}{}:
			default:
			}
		}
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
