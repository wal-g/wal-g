package copy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type retryTestFolder struct {
	storage.Folder
	read     func(context.Context, string) (io.ReadCloser, error)
	put      func(context.Context, string, io.Reader) error
	classify func(error) bool
}

func (f *retryTestFolder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if f.read != nil {
		return f.read(ctx, name)
	}
	return f.Folder.ReadObject(ctx, name)
}
func (f *retryTestFolder) PutObject(ctx context.Context, name string, r io.Reader) error {
	if f.put != nil {
		return f.put(ctx, name, r)
	}
	return f.Folder.PutObject(ctx, name, r)
}
func (f *retryTestFolder) IsRetryableError(err error) bool {
	return f.classify != nil && f.classify(err)
}

type closeReader struct {
	io.Reader
	close func() error
}

func (r *closeReader) Close() error { return r.close() }

type brokenReader struct {
	data []byte
	err  error
}

func (r *brokenReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

func noCopyDelay(int) time.Duration { return 0 }

func TestStreamRetryReopensSource(t *testing.T) {
	for _, failure := range []string{"unexpected EOF", "short EOF", "destination reset", "backend error"} {
		t.Run(failure, func(t *testing.T) {
			const payload = "complete original object"
			reads, closes, writes := 0, 0, 0
			backendError := errors.New("retryable provider error")
			from := &retryTestFolder{read: func(context.Context, string) (io.ReadCloser, error) {
				reads++
				var r io.Reader = strings.NewReader(payload)
				if reads == 1 && failure == "unexpected EOF" {
					r = &brokenReader{[]byte(payload[:7]), io.ErrUnexpectedEOF}
				} else if reads == 1 && failure == "short EOF" {
					r = strings.NewReader(payload[:7])
				}
				return &closeReader{r, func() error { closes++; return nil }}, nil
			}}
			var uploaded []byte
			to := &retryTestFolder{classify: func(err error) bool { return errors.Is(err, backendError) }}
			to.put = func(_ context.Context, _ string, r io.Reader) error {
				writes++
				if writes == 1 && (failure == "destination reset" || failure == "backend error") {
					_, err := io.CopyN(io.Discard, r, 7)
					require.NoError(t, err)
					if failure == "backend error" {
						return backendError
					}
					return syscall.ECONNRESET
				}
				var err error
				uploaded, err = io.ReadAll(r)
				return err
			}
			err := retryCopy(t.Context(), 1, noCopyDelay, func(ctx context.Context) error {
				require.Equal(t, reads, closes, "previous attempt must close before reopening")
				return streamObject(ctx, from, to, "source", "target", int64(len(payload)), nil)
			})
			require.NoError(t, err)
			require.Equal(t, payload, string(uploaded))
			require.Equal(t, 2, reads)
			require.Equal(t, reads, closes)
		})
	}
}

func TestRetryBudgetAndTerminalErrors(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		retries, want int
	}{
		{"zero", io.ErrUnexpectedEOF, 0, 1},
		{"exhausted", io.ErrUnexpectedEOF, 2, 3},
		{"permanent", errors.New("permission denied"), 5, 1},
		{"cancelled", context.Canceled, 5, 1},
		{"deadline", context.DeadlineExceeded, 5, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := retryCopy(t.Context(), tc.retries, noCopyDelay, func(context.Context) error { calls++; return tc.err })
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, calls)
		})
	}
}

func TestCopyCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- retryCopy(ctx, 15, func(int) time.Duration { close(started); return time.Hour },
			func(context.Context) error { return io.ErrUnexpectedEOF })
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("backoff ignored cancellation")
	}
}

func TestBothCopyPathsRetryAndCloseTransformers(t *testing.T) {
	for _, common := range []bool{false, true} {
		t.Run(map[bool]string{false: "database copy", true: "st copy"}[common], func(t *testing.T) {
			const payload = "unchanged payload"
			base := memory.NewFolder("in_memory/", memory.NewKVS())
			require.NoError(t, base.PutObject(t.Context(), "object", strings.NewReader(payload)))
			reads, closes, transforms, transformCloses := 0, 0, 0, 0
			from := &retryTestFolder{Folder: base, read: func(context.Context, string) (io.ReadCloser, error) {
				reads++
				var r io.Reader = strings.NewReader(payload)
				if reads == 1 {
					r = &brokenReader{[]byte(payload[:4]), io.ErrUnexpectedEOF}
				}
				return &closeReader{r, func() error { closes++; return nil }}, nil
			}}
			to := memory.NewFolder("in_memory/", memory.NewKVS())
			retries := 1
			opts := ExecuteOptions{MaxRetries: &retries}
			if common {
				objects, err := storage.ListFolderRecursively(t.Context(), from)
				require.NoError(t, err)
				transform := func(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
					transforms++
					return &closeReader{io.MultiReader(strings.NewReader("header:"), r), func() error {
						require.ErrorIs(t, ctx.Err(), context.Canceled)
						transformCloses++
						return nil
					}}, nil
				}
				infos := BuildCopyingInfos(from, to, objects, func(storage.Object) bool { return true }, NoopRenameFunc, transform)
				require.NoError(t, InfosWithOptions(t.Context(), infos, opts))
				require.Equal(t, 2, transforms)
				require.Equal(t, transforms, transformCloses)
			} else {
				plan, err := NewPlan(t.Context(), from, to)
				require.NoError(t, err)
				require.NoError(t, plan.AddObject("object", "object", PayloadPhase, false))
				require.NoError(t, Execute(t.Context(), plan, opts))
			}
			r, err := to.ReadObject(t.Context(), "object")
			require.NoError(t, err)
			defer r.Close()
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			want := payload
			if common {
				want = "header:" + want
			}
			require.Equal(t, want, string(data))
			require.Equal(t, 2, reads)
			require.Equal(t, reads, closes)
		})
	}
}

func TestFailedPayloadDoesNotPublishSentinel(t *testing.T) {
	base := memory.NewFolder("in_memory/", memory.NewKVS())
	require.NoError(t, base.PutObject(t.Context(), "payload", strings.NewReader("payload")))
	reads := 0
	from := &retryTestFolder{Folder: base, read: func(context.Context, string) (io.ReadCloser, error) {
		reads++
		return io.NopCloser(&brokenReader{[]byte("pay"), io.ErrUnexpectedEOF}), nil
	}}
	to := memory.NewFolder("in_memory/", memory.NewKVS())
	plan, err := NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("payload", "payload", PayloadPhase, false))
	plan.AddInline("sentinel", []byte("{}"), BackupCommitPhase, false)
	retries := 1
	require.ErrorIs(t, Execute(t.Context(), plan, ExecuteOptions{MaxRetries: &retries}), io.ErrUnexpectedEOF)
	require.Equal(t, 2, reads)
	exists, err := to.Exists(t.Context(), "sentinel")
	require.NoError(t, err)
	require.False(t, exists)
	reads = 0
	require.NoError(t, Execute(t.Context(), plan, ExecuteOptions{MaxRetries: &retries, SkipErrorsFor: regexp.MustCompile("^payload$")}))
	require.Equal(t, 2, reads)
}

func TestInfosWaitsForCancelledWorkers(t *testing.T) {
	started := make(chan struct{})
	var closed atomic.Bool
	from := &retryTestFolder{read: func(ctx context.Context, name string) (io.ReadCloser, error) {
		if name == "blocked" {
			close(started)
			return &closeReader{&contextReader{ctx}, func() error { closed.Store(true); return nil }}, nil
		}
		<-started
		return nil, errors.New("terminal source error")
	}}
	to := memory.NewFolder("in_memory/", memory.NewKVS())
	objects := []storage.Object{
		storage.NewLocalObject("blocked", time.Time{}, 1), storage.NewLocalObject("fail", time.Time{}, 1),
	}
	infos := BuildCopyingInfos(from, to, objects, func(storage.Object) bool { return true }, NoopRenameFunc, nil)
	require.ErrorContains(t, Infos(t.Context(), infos), "terminal source error")
	require.True(t, closed.Load())
}

type contextReader struct{ ctx context.Context }

func (r *contextReader) Read([]byte) (int, error) { <-r.ctx.Done(); return 0, r.ctx.Err() }

func TestTransformerSetupFailureClosesSource(t *testing.T) {
	closed := false
	from := &retryTestFolder{read: func(context.Context, string) (io.ReadCloser, error) {
		return &closeReader{bytes.NewReader(nil), func() error { closed = true; return nil }}, nil
	}}
	err := streamObject(t.Context(), from, nil, "source", "target", 0,
		func(context.Context, io.Reader) (io.ReadCloser, error) { return nil, errors.New("bad key") })
	require.ErrorContains(t, err, "bad key")
	require.True(t, closed)
}

type retryCrossFolder struct {
	*retryTestFolder
	copies  int
	copyErr error
}

func (*retryCrossFolder) CanCopyFrom(storage.Folder) bool { return true }
func (f *retryCrossFolder) CopyObjectFrom(context.Context, storage.Folder, string, string, int64) error {
	f.copies++
	return f.copyErr
}

func TestRetryStaysWithStreamingFallback(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient stream failure", true: "permanent stream failure"}[permanent], func(t *testing.T) {
			reads := 0
			from := &retryTestFolder{read: func(context.Context, string) (io.ReadCloser, error) {
				reads++
				if permanent {
					return nil, errors.New("permission denied")
				}
				if reads == 1 {
					return nil, io.ErrUnexpectedEOF
				}
				return io.NopCloser(strings.NewReader("payload")), nil
			}}
			to := &retryCrossFolder{retryTestFolder: &retryTestFolder{Folder: memory.NewFolder("target/", memory.NewKVS())},
				copyErr: io.ErrUnexpectedEOF}
			retries := 1
			err := copyObjectWithoutMetrics(t.Context(), from, to, Entry{SourcePath: "object", TargetPath: "object", Size: 7},
				ExecuteOptions{MaxRetries: &retries, UseServerSideCopy: true, ServerSideCopyFallback: true})
			require.Equal(t, 1, to.copies)
			if permanent {
				require.ErrorContains(t, err, "permission denied")
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				require.Equal(t, 1, reads, "earlier server failure must not make a permanent stream error retryable")
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, reads)
			}
		})
	}
}
