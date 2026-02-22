package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
)

var LogFile *os.File
var slogLevel slog.Level

// parseLevel converts a string representation to a slog.Level.
// It maps custom string values to standard levels.
func parseLevel(levelStr string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(levelStr)) {
	case "NORMAL", "INFO":
		return slog.LevelInfo, nil
	case "DEVEL", "DEBUG":
		return slog.LevelDebug, nil
	case "WARNING", "WARN":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("got incorrect log level: '%s', expected one of: [NORMAL DEVEL WARNING ERROR]", levelStr)
	}
}

type legacyHandler struct {
	opts   slog.HandlerOptions
	w      io.Writer
	mu     *sync.Mutex // Must be a pointer so cloned handlers share the lock
	buf    *bytes.Buffer
	attrs  []slog.Attr // Attributes added via WithAttrs
	groups []string    // Namespaces added via WithGroup
}

func newLegacyHandler(w io.Writer, opts *slog.HandlerOptions) *legacyHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &legacyHandler{
		opts: *opts,
		w:    w,
		mu:   &sync.Mutex{}, // Initialize the shared lock
		buf:  &bytes.Buffer{},
	}
}

func (h *legacyHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *legacyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := h.clone()
	h2.attrs = append(h2.attrs, attrs...)
	return h2
}

func (h *legacyHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := h.clone()
	h2.groups = append(h2.groups, name)
	return h2
}

//nolint:gocritic
func (h *legacyHandler) Handle(_ context.Context, r slog.Record) error {
	var sourceStr string
	if h.opts.AddSource && r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		sourceStr = fmt.Sprintf("\tsource=%s:%d", f.File, f.Line)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.buf.Reset()

	// Write the line: Level: Time Message [source=file]
	fmt.Fprintf(h.buf, "%s: %s %s%s",
		r.Level,
		r.Time.Format("2006/01/02 15:04:05.000000"), // preserve legacy time format
		r.Message,
		sourceStr,
	)

	// Build the group prefix (e.g., "db.pool.")
	prefix := ""
	if len(h.groups) > 0 {
		prefix = strings.Join(h.groups, ".") + "."
	}

	// Helper function to print attributes
	writeAttr := func(a slog.Attr) {
		a.Value = a.Value.Resolve()
		if a.Key == "" {
			return
		}
		fmt.Fprintf(h.buf, " %s%s=%v", prefix, a.Key, a.Value.Any())
	}

	// Write pre-stored attributes (from WithAttrs)
	for _, a := range h.attrs {
		writeAttr(a)
	}

	// Write record attributes (passed directly to Info, Error, etc.)
	r.Attrs(func(a slog.Attr) bool {
		writeAttr(a)
		return true
	})

	// Add the final newline
	fmt.Fprintln(h.buf)

	_, err := h.w.Write(h.buf.Bytes())
	return err
}

// clone creates a copy of the handler, sharing the Writer and Mutex
func (h *legacyHandler) clone() *legacyHandler {
	// Copy slices to prevent cloned handlers from overwriting each other's arrays
	newAttrs := make([]slog.Attr, len(h.attrs))
	copy(newAttrs, h.attrs)

	newGroups := make([]string, len(h.groups))
	copy(newGroups, h.groups)

	return &legacyHandler{
		opts:   h.opts,
		w:      h.w,
		mu:     h.mu, // Pointer copy: they share the exact same lock!
		buf:    h.buf,
		attrs:  newAttrs,
		groups: newGroups,
	}
}

func SetupLogger(logFile *os.File, logLevel string, logFormat string) error {
	level, errLevel := parseLevel(logLevel)
	slogLevel = level
	source := false
	// if slogLevel == slog.LevelDebug {
	// 	source = true
	// }

	opts := &slog.HandlerOptions{
		Level:     slogLevel,
		AddSource: source,
		ReplaceAttr: func(groups []string, attrs slog.Attr) slog.Attr {
			// Check if the current attribute is the Time key
			if attrs.Key == slog.TimeKey {
				// Convert to UTC and format as ISO 8601 / RFC3339
				t := attrs.Value.Time().UTC()
				return slog.String(slog.TimeKey, t.Format("2006-01-02T15:04:05.000Z"))
			}
			return attrs
		},
	}

	var handler slog.Handler
	switch logFormat {
	case "JSON":
		handler = slog.NewJSONHandler(logFile, opts)
	case "TEXT":
		handler = slog.NewTextHandler(logFile, opts)
	default:
		// "LEGACY"
		handler = newLegacyHandler(logFile, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Defer parseLevel error check to allow configuration of the logger
	if errLevel != nil {
		return errLevel
	}
	return nil
}

func Fatalf(msg string, args ...any) {
	slog.Error(fmt.Sprintf(msg, args...))
	os.Exit(1)
}

func FatalOnError(err error, args ...any) {
	if err != nil {
		FatalError(err, args...)
	}
}

func FatalError(err error, args ...any) {
	slog.Error(fmt.Sprintf(GetErrorFormatter(), err), args...)
	os.Exit(1)
}

func GetErrorFormatter() string {
	if slogLevel == slog.LevelDebug {
		return "%+v"
	}
	return "%v"
}
