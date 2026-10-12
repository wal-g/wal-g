package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type transferError struct {
	operation string
	folder    storage.Folder
	err       error
}

func (e *transferError) Error() string { return fmt.Sprintf("%s: %v", e.operation, e.err) }
func (e *transferError) Unwrap() error { return e.err }

func retryableTransfer(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if storage.IsTransientIOError(err) {
		return true
	}
	var transfer *transferError
	if errors.As(err, &transfer) {
		if classifier, ok := transfer.folder.(storage.RetryClassifier); ok {
			return classifier.IsRetryableError(transfer.err)
		}
	}
	return false
}

func copyRetryDelay(retry int) time.Duration {
	delay := time.Second
	for i := 0; i < retry && delay < 30*time.Second; i++ {
		delay = min(delay*2, 30*time.Second)
	}
	return min(time.Duration(float64(delay)*(0.5+rand.Float64())), 30*time.Second)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func retryCopy(ctx context.Context, retries int, delay func(int) time.Duration, attempt func(context.Context) error) error {
	for run := 0; ; run++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := attempt(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryableTransfer(err) || run >= retries {
			return err
		}
		pause := delay(run)
		operation := "object transfer"
		var transfer *transferError
		if errors.As(err, &transfer) {
			operation = transfer.operation
		}
		tracelog.WarningLogger.Printf("Retrying copy after transient %s failure; retry %d/%d in %s",
			operation, run+1, retries, pause)
		if err := waitForRetry(ctx, pause); err != nil {
			return err
		}
	}
}

func streamObject(ctx context.Context, from, to storage.Folder, source, target string,
	size int64, transformer SourceTransformerFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, err := from.ReadObject(ctx, source)
	if err != nil {
		return &transferError{"read source object", from, err}
	}
	var transformed io.ReadCloser
	defer func() {
		cancel()
		_ = reader.Close()
		if transformed != nil {
			_ = transformed.Close()
		}
	}()
	var content io.Reader = &sourceReader{reader: reader, folder: from, remaining: size}
	if transformer != nil {
		transformed, err = transformer(ctx, content)
		if err != nil {
			return err
		}
		content = transformed
	}
	if err := to.PutObject(ctx, target, content); err != nil {
		var sourceError *transferError
		if errors.As(err, &sourceError) {
			return err
		}
		return &transferError{"write destination object", to, err}
	}
	return nil
}

type sourceReader struct {
	reader    io.Reader
	folder    storage.Folder
	remaining int64
}

func (r *sourceReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		err = errors.New("source object exceeds its inventoried size")
	} else if err == io.EOF && r.remaining != 0 {
		err = io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF {
		err = &transferError{"read source object", r.folder, err}
	}
	return n, err
}
