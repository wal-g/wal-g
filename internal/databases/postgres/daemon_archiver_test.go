package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/compression"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/daemon"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/internal/statistics"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type archiveTestUploader struct {
	internal.Uploader
	upload   func(context.Context, ioextensions.NamedReader) error
	metadata func(context.Context, string, io.Reader) error
}

func (u archiveTestUploader) Upload(ctx context.Context, name string, body io.Reader) error {
	return u.metadata(ctx, name, body)
}

func (u archiveTestUploader) UploadFile(ctx context.Context, file ioextensions.NamedReader) error {
	return u.upload(ctx, file)
}

func (archiveTestUploader) Folder() storage.Folder { return memory.NewFolder("", memory.NewKVS()) }

func archiveTestConfig(t *testing.T) {
	t.Helper()
	for key, value := range map[string]any{
		conf.UploadWalMetadata:          WalNoMetadataLevel,
		conf.PreventWalOverwriteSetting: false,
		conf.PgReadyRename:              false,
	} {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
}

func archiveTestFiles(t *testing.T, count int) []string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, archiveStatusDir), 0700))
	files := make([]string, count)
	for i := range files {
		files[i] = filepath.Join(dir, fmt.Sprintf("0000000100000000000000%02X", i))
		require.NoError(t, os.WriteFile(files[i], []byte("test WAL"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, archiveStatusDir, filepath.Base(files[i])+readySuffix), nil, 0600))
	}
	return files
}

func archiveTestFactory(upload func(context.Context, ioextensions.NamedReader) error) func(context.Context) (*WalUploader, error) {
	return func(context.Context) (*WalUploader, error) {
		return &WalUploader{Uploader: archiveTestUploader{upload: upload}, PGArchiveStatusManager: asm.NewNopASM()}, nil
	}
}

func archiveTestWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("archive pipeline made no progress")
	}
}

func archiveTestPush(t *testing.T, a *daemonArchiver, filename string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.NoError(t, a.Push(ctx, filename))
}

// A slow speculative upload must neither delay foreground acknowledgement nor
// prevent the other background worker from draining beyond the discovery batch.
func TestDaemonArchiverForegroundAndMarkerRefill(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 7)
	manager := asm.NewFakeASM()
	slowStarted, sixthStarted := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	counts := make(map[string]int)
	a := newDaemonArchiver(t.Context(), 3, 4, time.Minute, manager, archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			mu.Lock()
			counts[file.Name()]++
			mu.Unlock()
			switch file.Name() {
			case files[0]:
				select {
				case <-slowStarted:
				case <-ctx.Done():
					return ctx.Err()
				}
			case files[1]:
				close(slowStarted)
				<-ctx.Done()
				return ctx.Err()
			case files[6]:
				close(sixthStarted)
			}
			_, err := io.Copy(io.Discard, file)
			return err
		}))
	defer a.Close()
	archiveTestPush(t, a, files[0]) // returns with files[1] still blocked
	require.Eventually(t, func() bool { return manager.IsWalAlreadyUploaded(files[4]) }, 3*time.Second, time.Millisecond)
	archiveTestWait(t, sixthStarted)
	archiveTestPush(t, a, files[2]) // hit ASM; no new request was needed to reach files[6]
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, counts[files[2]], "acknowledgement should not re-upload the WAL")
}

func TestDaemonArchiverJoinFailureAndRetry(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	manager := asm.NewFakeASM()
	started, fail := make(chan struct{}), make(chan struct{})
	want := errors.New("injected S3 failure")
	var attempts atomic.Int32
	a := newDaemonArchiver(t.Context(), 2, 1, time.Minute, manager, archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			if file.Name() == files[1] && attempts.Add(1) == 1 {
				close(started)
				select {
				case <-fail:
					return want
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}))
	defer a.Close()
	archiveTestPush(t, a, files[0])
	archiveTestWait(t, started)
	// Keep a reference to the background job; the foreground must join it.
	a.mu.Lock()
	job := a.jobs[files[1]]
	a.mu.Unlock()
	require.NotNil(t, job)
	joined, err := a.startJob(t.Context(), files[1], false)
	require.NoError(t, err)
	require.Same(t, job, joined)
	close(fail)
	archiveTestWait(t, job.done)
	require.ErrorIs(t, job.err, want)
	require.False(t, manager.IsWalAlreadyUploaded(files[1]))
	archiveTestPush(t, a, files[1]) // foreground retry does not wait for BG cooldown
	require.EqualValues(t, 2, attempts.Load())
}

func TestDaemonArchiverCancellationKeepsConcurrencyBound(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 5)
	var active, peak atomic.Int32
	started := make(chan string, 8)
	a := newDaemonArchiver(t.Context(), 3, 4, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			started <- file.Name()
			<-ctx.Done()
			return ctx.Err()
		}))
	defer a.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Push(ctx, files[0]) }()
	for range 3 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("workers not started")
		}
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	// Disconnecting/timing out a caller must not release a still-running job's slot.
	ctx2, cancel2 := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, a.Push(ctx2, files[4]), context.DeadlineExceeded)
	require.EqualValues(t, 3, active.Load())
	require.EqualValues(t, 3, peak.Load())
	a.Close()
	require.Zero(t, active.Load())
	require.Error(t, a.Push(t.Context(), files[0]))
}

func TestDaemonArchiverTimeoutAndRecovery(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 1)
	manager := asm.NewFakeASM()
	var attempts atomic.Int32
	a := newDaemonArchiver(t.Context(), 1, 31, 30*time.Millisecond, manager, archiveTestFactory(
		func(ctx context.Context, _ ioextensions.NamedReader) error {
			if attempts.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}))
	defer a.Close()
	require.ErrorIs(t, a.Push(t.Context(), files[0]), context.DeadlineExceeded)
	require.False(t, manager.IsWalAlreadyUploaded(files[0]))
	archiveTestPush(t, a, files[0])
	require.EqualValues(t, 2, attempts.Load())
}

func TestDaemonArchiverGapRefill(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 7)
	// Force the sorted-list fallback, with more work than background slots.
	require.NoError(t, os.Remove(filepath.Join(filepath.Dir(files[1]), archiveStatusDir, filepath.Base(files[1])+readySuffix)))
	manager := asm.NewFakeASM()
	a := newDaemonArchiver(t.Context(), 2, 5, time.Minute, manager,
		archiveTestFactory(func(context.Context, ioextensions.NamedReader) error { return nil }))
	defer a.Close()
	archiveTestPush(t, a, files[0])
	require.Eventually(t, func() bool { return manager.IsWalAlreadyUploaded(files[6]) }, 3*time.Second, time.Millisecond,
		"cached listing must refill without waiting for the next five-second scan")
}

func TestDaemonArchiverCandidateRolloverAndCache(t *testing.T) {
	archiveTestConfig(t)
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, archiveStatusDir), 0700))
	anchor := filepath.Join(dir, "0000000100000000000000FF")
	next := filepath.Join(dir, "000000010000000100000000")
	require.NoError(t, os.WriteFile(filepath.Join(dir, archiveStatusDir, filepath.Base(next)+readySuffix), nil, 0600))
	manager := asm.NewFakeASM()
	a := &daemonArchiver{window: 2, manager: manager}
	var scan daemonArchiveScan
	require.Equal(t, []string{next}, a.candidates(anchor, &scan))
	require.True(t, scan.lastFullScan.IsZero(), "contiguous WALs must not need ReadDir")
	// READY_RENAME removes .ready, but ASM still provides the lookahead bridge.
	require.NoError(t, manager.MarkWalUploaded(next))
	require.NoError(t, os.Remove(filepath.Join(dir, archiveStatusDir, filepath.Base(next)+readySuffix)))
	require.Equal(t, []string{next}, a.candidates(anchor, &scan))
	require.True(t, scan.lastFullScan.IsZero())
}

func TestDaemonArchiverProtocolAcknowledgesWithoutDraining(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	pgdata := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Dir(files[0]), filepath.Join(pgdata, "pg_wal")))
	for key, value := range map[string]any{conf.PgDataSetting: pgdata, conf.PgDaemonWALUploadTimeout: "3s"} {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
	started := make(chan struct{})
	a := newDaemonArchiver(t.Context(), 2, 1, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			if filepath.Base(file.Name()) == filepath.Base(files[1]) {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}
			select {
			case <-started:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
	defer a.Close()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); processConnection(t.Context(), server, nil, a) }()
	defer func() { _ = client.Close(); archiveTestWait(t, done) }()
	require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
	name := filepath.Base(files[0])
	msg := append(binary.BigEndian.AppendUint16(daemon.WalPushType.ToBytes(), uint16(len(name)+3)), []byte(name)...)
	_, err := client.Write(msg)
	require.NoError(t, err)
	reply := make([]byte, 1)
	_, err = io.ReadFull(client, reply)
	require.NoError(t, err)
	require.Equal(t, daemon.OkType.ToBytes(), reply)
}

func TestDaemonArchiverPersistentMarkersAndPartial(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 1)
	folder, err := fsutil.NewDiskDataFolder(t.TempDir())
	require.NoError(t, err)
	manager := asm.NewDataFolderASM(folder)
	var calls atomic.Int32
	factory := archiveTestFactory(func(context.Context, ioextensions.NamedReader) error { calls.Add(1); return nil })
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, factory)
	job, err := a.startJob(t.Context(), files[0], false)
	require.NoError(t, err)
	archiveTestWait(t, job.done)
	require.NoError(t, job.err)
	a.Close() // simulate daemon restart after upload, before acknowledgement
	a = newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, factory)
	defer a.Close()
	archiveTestPush(t, a, files[0])
	require.EqualValues(t, 1, calls.Load())
	require.False(t, manager.IsWalAlreadyUploaded(files[0]))
	partial := files[0] + ".partial"
	require.NoError(t, os.WriteFile(partial, []byte("partial WAL"), 0600))
	job, err = a.startJob(t.Context(), partial, false)
	require.NoError(t, err)
	archiveTestWait(t, job.done)
	require.NoError(t, job.err)
	require.False(t, manager.IsWalAlreadyUploaded(files[0]), "partial upload cannot confirm the complete segment")
	// Conversely, a complete segment's marker cannot confirm a .partial object
	// under a different storage key or be consumed by that request.
	require.NoError(t, manager.MarkWalUploaded(files[0]))
	before := calls.Load()
	archiveTestPush(t, a, partial)
	require.Equal(t, before+1, calls.Load())
	require.True(t, manager.IsWalAlreadyUploaded(files[0]))
}

type archiveTestStorageSelector struct {
	stats.Collector
	choice atomic.Int32
}

func (s *archiveTestStorageSelector) FirstAliveStorage(context.Context) (*string, error) {
	name := "primary"
	if s.choice.Load() == 1 {
		name = "secondary"
	}
	if s.choice.Load() == 2 {
		return nil, nil
	}
	return &name, nil
}

func (*archiveTestStorageSelector) ReportOperationResult(string, stats.OperationWeight, bool) {}

func TestDaemonArchiverReselectsStorageAndReportsNoAlive(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 3)
	for key, value := range map[string]any{
		conf.PgDataSetting: t.TempDir(), conf.CompressionMethodSetting: "none", conf.UseWalDeltaSetting: false,
	} {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
	primary, secondary := memory.NewFolder("", memory.NewKVS()), memory.NewFolder("", memory.NewKVS())
	selector := &archiveTestStorageSelector{}
	root := multistorage.NewFolder(map[string]storage.Folder{"primary": primary, "secondary": secondary}, selector)
	manager := asm.NewFakeASM()
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, func(ctx context.Context) (*WalUploader, error) {
		return PrepareMultiStorageWalUploader(ctx, root, "")
	})
	defer a.Close()
	archiveTestPush(t, a, files[0])
	selector.choice.Store(1)
	archiveTestPush(t, a, files[1])
	for i, folder := range []storage.Folder{primary, secondary} {
		r, err := folder.GetSubFolder(utility.WalPath).ReadObject(t.Context(), filepath.Base(files[i]))
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, r.Close())
		require.NoError(t, err)
		require.Equal(t, "test WAL", string(data))
	}
	selector.choice.Store(2)
	require.ErrorIs(t, a.Push(t.Context(), files[2]), multistorage.ErrNoAliveStorages)
	require.False(t, manager.IsWalAlreadyUploaded(files[2]))
	selector.choice.Store(0)
	archiveTestPush(t, a, files[2])
}

func TestDaemonArchiverMetadataFailureDoesNotAcknowledge(t *testing.T) {
	archiveTestConfig(t)
	viper.Set(conf.UploadWalMetadata, WalIndividualMetadataLevel)
	files := archiveTestFiles(t, 1)
	manager := asm.NewFakeASM()
	var uploads atomic.Int32
	var metadataAttempts atomic.Int32
	want := errors.New("metadata upload failed")
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, func(context.Context) (*WalUploader, error) {
		return &WalUploader{Uploader: archiveTestUploader{
			upload: func(context.Context, ioextensions.NamedReader) error { uploads.Add(1); return nil },
			metadata: func(context.Context, string, io.Reader) error {
				if metadataAttempts.Add(1) == 1 {
					return want
				}
				return nil
			},
		}}, nil
	})
	defer a.Close()
	require.ErrorIs(t, a.Push(t.Context(), files[0]), want)
	require.True(t, manager.IsWalAlreadyUploaded(files[0]), "keep successful WAL marker for metadata retry")
	archiveTestPush(t, a, files[0])
	require.EqualValues(t, 1, uploads.Load())
	require.EqualValues(t, 2, metadataAttempts.Load())
	require.False(t, manager.IsWalAlreadyUploaded(files[0]))
}

func TestDaemonArchiverWalDeltaKeepsBatchPath(t *testing.T) {
	old := viper.Get(conf.UseWalDeltaSetting)
	viper.Set(conf.UseWalDeltaSetting, true)
	defer viper.Set(conf.UseWalDeltaSetting, old)
	a, err := configureDaemonArchiver(t.Context(), nil)
	require.NoError(t, err)
	require.Nil(t, a)
}

type archiveTestBrokenASM struct{ asm.ArchiveStatusManager }

func (archiveTestBrokenASM) MarkWalUploaded(string) error { return errors.New("ASM disk full") }

func TestDaemonArchiverMarkerFailureBacksOff(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 1)
	a := newDaemonArchiver(t.Context(), 2, 0, time.Minute,
		archiveTestBrokenASM{asm.NewFakeASM()},
		archiveTestFactory(func(context.Context, ioextensions.NamedReader) error { return nil }))
	defer a.Close()
	job, err := a.startJob(t.Context(), files[0], true)
	require.NoError(t, err)
	require.NotNil(t, job)
	archiveTestWait(t, job.done)
	require.NoError(t, job.err, "successful storage upload remains successful")
	a.mu.Lock()
	retryAt := a.retryAfter[files[0]]
	a.mu.Unlock()
	require.True(t, retryAt.After(time.Now()), "missing ASM marker must not cause a tight re-upload loop")
}

// archiveTestListener tells when the daemon is done with a connection: it reads settings
// after replying, which must not overlap with a test restoring them.
type archiveTestListener struct {
	net.Listener
	closed chan struct{}
}

func (l archiveTestListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return archiveTestConn{Conn: c, closed: l.closed}, nil
}

type archiveTestConn struct {
	net.Conn
	closed chan struct{}
}

func (c archiveTestConn) Close() error {
	defer func() { c.closed <- struct{}{} }()
	return c.Conn.Close()
}

func TestDaemonArchiverServeAndClient(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	pgdata := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Dir(files[0]), filepath.Join(pgdata, "pg_wal")))
	for key, value := range map[string]any{
		conf.PgDataSetting: pgdata, conf.PgDaemonWALUploadTimeout: "3s",
		conf.CompressionMethodSetting: "lz4", conf.UseWalDeltaSetting: false,
		conf.UploadConcurrencySetting: 3, conf.TotalBgUploadedLimit: 32,
	} {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
	st := memory.NewStorage("", memory.NewKVS())
	multi, err := multistorage.NewStorage(&multistorage.Config{}, st, nil)
	require.NoError(t, err)
	defer multi.Close()
	// Keep the socket path short enough for sockaddr_un on macOS.
	socketRoot := ""
	if runtime.GOOS == "darwin" {
		socketRoot = "/tmp"
	}
	socketDir, err := os.MkdirTemp(socketRoot, "walg-socket-")
	require.NoError(t, err)
	defer os.Remove(socketDir)
	socket := filepath.Join(socketDir, "s")
	l, err := net.Listen("unix", socket)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	closed := make(chan struct{}, len(files))
	go func() { done <- serve(ctx, archiveTestListener{Listener: l, closed: closed}, multi) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("daemon failed to stop")
		}
	}()
	defer func() {
		for range files {
			archiveTestWait(t, closed)
		}
	}()
	for _, file := range files {
		reply, err := daemon.SendCommand(&daemon.RunOptions{
			MessageType: daemon.WalPushType, SocketName: socket, MessageArgs: []string{filepath.Base(file)},
			DaemonOperationTimeout: 3 * time.Second, DaemonSocketConnectionTimeout: time.Second,
		})
		require.NoError(t, err)
		require.Equal(t, daemon.OkType, reply)
		r, err := st.RootFolder().GetSubFolder(utility.WalPath).ReadObject(t.Context(), filepath.Base(file)+".lz4")
		require.NoError(t, err)
		decoded, err := compression.FindDecompressor("lz4").Decompress(r)
		require.NoError(t, err)
		body, err := io.ReadAll(decoded)
		require.NoError(t, err)
		require.NoError(t, decoded.Close())
		require.NoError(t, r.Close())
		require.Equal(t, "test WAL", string(body))
	}
}

func archiveTestSettings(t *testing.T, settings map[string]any) {
	t.Helper()
	for key, value := range settings {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
}

// archiveTestSkipList configures a skip list last modified age ago.
func archiveTestSkipList(t *testing.T, age time.Duration, names ...string) {
	t.Helper()
	listPath := filepath.Join(t.TempDir(), "skip_list")
	require.NoError(t, os.WriteFile(listPath, []byte(strings.Join(names, "\n")+"\n"), 0640))
	modified := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(listPath, modified, modified))
	archiveTestSettings(t, map[string]any{conf.PgArchiveSkipList: listPath})
}

func archiveTestSkippedCount(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, statistics.WalgMetrics.WalPushSkippedTotal.Write(&metric))
	return metric.GetCounter().GetValue()
}

func TestDaemonArchiverSkipList(t *testing.T) {
	const (
		segment = "000000010000000000000000" // the file archiveTestFiles creates first
		partial = segment + ".partial"
		history = "00000002.history"
	)
	listed := func(age time.Duration, names ...string) func(*testing.T) {
		return func(t *testing.T) { archiveTestSkipList(t, age, names...) }
	}
	for _, test := range []struct {
		name    string
		pushed  string // segment if empty
		prepare func(*testing.T)
		skipped bool
	}{
		{name: "fresh list", prepare: listed(0, segment), skipped: true},
		{name: "among other names", prepare: listed(0, "000000010000000000000009", segment, history), skipped: true},
		{name: "partial file", pushed: partial, prepare: listed(0, partial), skipped: true},
		{name: "history file", pushed: history, prepare: listed(0, history), skipped: true},
		{name: "history file not listed", pushed: history, prepare: listed(0, segment)},
		{name: "within default ttl", prepare: listed(30*time.Second, segment), skipped: true},
		{name: "beyond default ttl", prepare: listed(2*time.Minute, segment)},
		{name: "within configured ttl", skipped: true, prepare: func(t *testing.T) {
			archiveTestSkipList(t, 2*time.Minute, segment)
			archiveTestSettings(t, map[string]any{conf.PgArchiveSkipListTTL: "5m"})
		}},
		{name: "other name", prepare: listed(0, "000000010000000000000009")},
		{name: "name extending the pushed one", prepare: listed(0, partial)},
		{name: "name extended by the pushed one", pushed: partial, prepare: listed(0, segment)},
		{name: "no list configured", prepare: func(*testing.T) {}},
		{name: "missing list", prepare: func(t *testing.T) {
			archiveTestSettings(t, map[string]any{conf.PgArchiveSkipList: filepath.Join(t.TempDir(), "absent")})
		}},
		{name: "unreadable list", prepare: func(t *testing.T) {
			archiveTestSettings(t, map[string]any{conf.PgArchiveSkipList: t.TempDir()})
		}},
		{name: "invalid ttl", prepare: func(t *testing.T) {
			archiveTestSkipList(t, 0, segment)
			archiveTestSettings(t, map[string]any{conf.PgArchiveSkipListTTL: "soon"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			archiveTestConfig(t)
			test.prepare(t)
			files := archiveTestFiles(t, 1)
			pushed := files[0]
			if test.pushed != "" {
				pushed = filepath.Join(filepath.Dir(files[0]), test.pushed)
				require.NoError(t, os.WriteFile(pushed, []byte("test WAL"), 0600))
			}
			var uploads atomic.Int32
			manager := asm.NewFakeASM()
			a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, archiveTestFactory(
				func(context.Context, ioextensions.NamedReader) error { uploads.Add(1); return nil }))
			defer a.Close()
			skippedBefore := archiveTestSkippedCount(t)

			archiveTestPush(t, a, pushed)

			if test.skipped {
				require.Zero(t, uploads.Load())
				require.Equal(t, skippedBefore+1, archiveTestSkippedCount(t))
				require.False(t, manager.IsWalAlreadyUploaded(pushed), "a skipped file is not uploaded")
			} else {
				require.EqualValues(t, 1, uploads.Load())
				require.Equal(t, skippedBefore, archiveTestSkippedCount(t))
			}
		})
	}
}

// A hung upload must neither outlive the acknowledgement nor leave a marker behind, even when
// it completes while being cancelled.
func TestDaemonArchiverSkipListStopsRunningUpload(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	manager := asm.NewFakeASM()
	started := make(chan struct{})
	var finished atomic.Bool
	a := newDaemonArchiver(t.Context(), 2, 1, time.Minute, manager, archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			if file.Name() == files[1] {
				close(started)
				<-ctx.Done()
				finished.Store(true)
			}
			return nil
		}))
	defer a.Close()
	archiveTestPush(t, a, files[0]) // the scan picks up files[1] in the background
	archiveTestWait(t, started)
	archiveTestSkipList(t, 0, filepath.Base(files[1]))

	archiveTestPush(t, a, files[1])

	require.True(t, finished.Load(), "the upload must be over before PostgreSQL gets the file back")
	require.False(t, manager.IsWalAlreadyUploaded(files[1]), "the marker of the cancelled upload must go")
}

func TestDaemonArchiverSkipListDropsMarkerOfUploadedFile(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 1)
	archiveTestSkipList(t, 0, filepath.Base(files[0]))
	manager := asm.NewFakeASM()
	require.NoError(t, manager.MarkWalUploaded(files[0]))
	var uploads atomic.Int32
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, manager, archiveTestFactory(
		func(context.Context, ioextensions.NamedReader) error { uploads.Add(1); return nil }))
	defer a.Close()

	archiveTestPush(t, a, files[0])

	require.Zero(t, uploads.Load())
	require.False(t, manager.IsWalAlreadyUploaded(files[0]))
}

// A request abandoned by its client holds the foreground slot until the upload times out.
func TestDaemonArchiverSkipListNeedsNoForegroundSlot(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	archiveTestSkipList(t, 0, filepath.Base(files[0]))
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(context.Context, ioextensions.NamedReader) error { return nil }))
	defer a.Close()
	a.foreground <- struct{}{}
	defer func() { <-a.foreground }()

	archiveTestPush(t, a, files[0])

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, a.Push(ctx, files[1]), context.DeadlineExceeded, "an unlisted file does wait for the slot")
}

// The retry behind an abandoned request must not upload a file acknowledged in the meantime.
func TestDaemonArchiverSkipListAppearingWhileWaitingForSlot(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 1)
	listPath := filepath.Join(t.TempDir(), "skip_list")
	archiveTestSettings(t, map[string]any{conf.PgArchiveSkipList: listPath})
	var uploads atomic.Int32
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(context.Context, ioextensions.NamedReader) error { uploads.Add(1); return nil }))
	defer a.Close()
	a.foreground <- struct{}{}
	pushed := make(chan error, 1)
	go func() { pushed <- a.Push(t.Context(), files[0]) }()
	time.Sleep(50 * time.Millisecond) // let the request pass the first check and park on the slot

	require.NoError(t, os.WriteFile(listPath, []byte(filepath.Base(files[0])+"\n"), 0640))
	<-a.foreground

	require.NoError(t, <-pushed)
	require.Zero(t, uploads.Load())
}

// A scan holds a batch of candidates while PostgreSQL moves on: it must not start uploading
// a file that may be acknowledged already.
func TestDaemonArchiverBackgroundJobsStayAheadOfAnchor(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 3)
	a := newDaemonArchiver(t.Context(), 2, 0, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(context.Context, ioextensions.NamedReader) error { return nil }))
	defer a.Close()
	a.mu.Lock()
	a.anchor = files[1]
	a.mu.Unlock()
	for _, behind := range files[:2] {
		job, err := a.startJob(t.Context(), behind, true)
		require.NoError(t, err)
		require.Nil(t, job, behind)
	}
	// The only background slot must have survived the refusals.
	job, err := a.startJob(t.Context(), files[2], true)
	require.NoError(t, err)
	require.NotNil(t, job)
	archiveTestWait(t, job.done)
}

func TestDaemonArchiverSkipListWithoutAliveStorage(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	archiveTestSkipList(t, 0, filepath.Base(files[0]))
	selector := &archiveTestStorageSelector{}
	selector.choice.Store(2)
	root := multistorage.NewFolder(map[string]storage.Folder{"primary": memory.NewFolder("", memory.NewKVS())}, selector)
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, asm.NewFakeASM(), func(ctx context.Context) (*WalUploader, error) {
		return PrepareMultiStorageWalUploader(ctx, root, "")
	})
	defer a.Close()
	archiveTestPush(t, a, files[0])
	require.ErrorIs(t, a.Push(t.Context(), files[1]), multistorage.ErrNoAliveStorages)
}

type archiveTestStorage struct {
	storage.Storage
	root storage.Folder
}

func (s archiveTestStorage) RootFolder() storage.Folder { return s.root }

// WAL delta mode has no archiver: the handler must not need a live storage for a listed file.
func TestArchiveMessageHandlerSkipList(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	listed, unlisted := filepath.Base(files[0]), filepath.Base(files[1])
	pgdata := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Dir(files[0]), filepath.Join(pgdata, "pg_wal")))
	archiveTestSettings(t, map[string]any{
		conf.PgDataSetting: pgdata, conf.PgDaemonWALUploadTimeout: "3s",
		conf.CompressionMethodSetting: "none", conf.UseWalDeltaSetting: false,
		conf.TotalBgUploadedLimit: 1, // the scan goroutine would outlive HandleWALPush and read these settings
	})
	archiveTestSkipList(t, 0, listed)
	primary := memory.NewFolder("", memory.NewKVS())
	selector := &archiveTestStorageSelector{}
	st := archiveTestStorage{root: multistorage.NewFolder(map[string]storage.Folder{"primary": primary}, selector)}
	handle := func(name string) error {
		t.Helper()
		server, client := net.Pipe()
		defer client.Close()
		defer server.Close()
		h, err := NewMessageHandler(t.Context(), daemon.WalPushType, server, st)
		require.NoError(t, err)
		go func() { _, _ = io.Copy(io.Discard, client) }()
		return h.Handle(t.Context(), []byte(name))
	}
	archived := func(name string) bool {
		t.Helper()
		exists, err := primary.GetSubFolder(utility.WalPath).Exists(t.Context(), name)
		require.NoError(t, err)
		return exists
	}

	selector.choice.Store(2)
	require.NoError(t, handle(listed))
	require.ErrorIs(t, handle(unlisted), multistorage.ErrNoAliveStorages)

	selector.choice.Store(0)
	require.NoError(t, handle(listed))
	require.False(t, archived(listed), "a listed file is skipped even when storage is alive")
	require.NoError(t, handle(unlisted))
	require.True(t, archived(unlisted))
}

func TestDaemonArchiverPublishesMetricsAfterEachRequest(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 2)
	listed, failing := filepath.Base(files[0]), filepath.Base(files[1])
	pgdata := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Dir(files[0]), filepath.Join(pgdata, "pg_wal")))
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer conn.Close()
	archiveTestSettings(t, map[string]any{
		conf.PgDataSetting: pgdata, conf.PgDaemonWALUploadTimeout: "3s",
		conf.StatsdAddressSetting: conn.LocalAddr().String(),
	})
	archiveTestSkipList(t, 0, listed)
	a := newDaemonArchiver(t.Context(), 1, 0, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(context.Context, ioextensions.NamedReader) error { return errors.New("injected S3 failure") }))
	defer a.Close()
	receive := func(want string) {
		t.Helper()
		buf := make([]byte, 65536)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
		for {
			n, _, err := conn.ReadFrom(buf)
			require.NoError(t, err, "no statsd datagram with %s", want)
			if strings.Contains(string(buf[:n]), want) {
				return
			}
		}
	}
	request := func(name string) []byte {
		t.Helper()
		server, client := net.Pipe()
		defer client.Close()
		done := make(chan struct{})
		go func() { defer close(done); processConnection(t.Context(), server, nil, a) }()
		defer archiveTestWait(t, done)
		require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
		_, err := client.Write(append(binary.BigEndian.AppendUint16(daemon.WalPushType.ToBytes(), uint16(len(name)+3)), []byte(name)...))
		require.NoError(t, err)
		reply := make([]byte, 1)
		_, err = io.ReadFull(client, reply)
		require.NoError(t, err)
		return reply
	}
	// Earlier tests in this process have moved the counter already.
	statistics.PushMetrics()
	receive("walg_wal_push_skipped_total:")

	require.Equal(t, daemon.OkType.ToBytes(), request(listed))
	receive("walg_wal_push_skipped_total:1|c")

	require.Equal(t, daemon.ErrorType.ToBytes(), request(failing))
	receive("walg_wal_push_skipped_total:0|c")
}
