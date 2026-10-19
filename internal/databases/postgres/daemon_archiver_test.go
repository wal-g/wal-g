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
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
// prevent the other background worker from refilling. ASM hits move the window.
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
	mu.Lock()
	require.Equal(t, 0, counts[files[5]], "must respect the lookahead window")
	mu.Unlock()
	archiveTestPush(t, a, files[2]) // hit ASM; opens more of the window
	archiveTestWait(t, sixthStarted)
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
	go func() { done <- serve(ctx, l, multi) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("daemon failed to stop")
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
