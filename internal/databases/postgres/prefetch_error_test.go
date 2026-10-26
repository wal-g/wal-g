package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type prefetchErrorReader struct{ *bytes.Reader }

func (r prefetchErrorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.Len() == 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

type prefetchTestFolder struct {
	storage.Folder
	data      []byte
	ext       string
	readError bool
	calls     atomic.Int32
}

func (f *prefetchTestFolder) GetSubFolder(string) storage.Folder { return f }
func (f *prefetchTestFolder) ReadObject(_ context.Context, name string) (io.ReadCloser, error) {
	f.calls.Add(1)
	if filepath.Ext(name) != f.ext {
		return nil, storage.NewObjectNotFoundError(name)
	}
	if f.readError {
		return io.NopCloser(prefetchErrorReader{bytes.NewReader(f.data)}), nil
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func runTestPrefetch(t *testing.T, dir string, f *prefetchTestFolder, name string) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	prefetchFile(t.Context(), dir, internal.NewFolderReader(f), name, &wg)
	wg.Wait()
}

func TestPrefetchDoesNotPublishFailedDownload(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size        int
		badChecksum bool
	}{
		{"short_read", 128, false},
		{"full_size_terminal_error", int(WalSegmentSize), false},
		{"full_size_bad_lz4_checksum", int(WalSegmentSize), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			old := viper.Get(conf.PrefetchDir)
			viper.Set(conf.PrefetchDir, dir)
			defer viper.Set(conf.PrefetchDir, old)
			data := make([]byte, tc.size)
			binary.LittleEndian.PutUint32(data, 0xD119)
			f := &prefetchTestFolder{data: data, readError: !tc.badChecksum}
			if tc.badChecksum {
				var buf bytes.Buffer
				w := lz4.NewWriter(&buf)
				_, err := w.Write(data)
				require.NoError(t, err)
				require.NoError(t, w.Close())
				f.data, f.ext = buf.Bytes(), ".lz4"
				f.data[len(f.data)-1] ^= 1
				decoded, err := io.ReadAll(lz4.NewReader(bytes.NewReader(f.data)))
				require.Error(t, err)
				require.Len(t, decoded, len(data), "error must follow a complete WAL-sized output")
			}
			name := "000000010000000000000001"
			runTestPrefetch(t, dir, f, name)
			_, _, running, ready := getPrefetchLocations(dir, name)
			for _, filename := range []string{running, ready} {
				_, err := os.Stat(filename)
				require.True(t, os.IsNotExist(err), fmt.Sprintf("failed prefetch left %s: %v", filename, err))
			}
			// A foreground fetch must try storage again and propagate the error,
			// not accept full-size output as a successful cached download.
			before := f.calls.Load()
			err := HandleWALFetch(t.Context(), internal.NewFolderReader(f), name, filepath.Join(dir, "fetched"), NopPrefetcher{})
			require.Error(t, err)
			require.Greater(t, f.calls.Load(), before)
			// Failed prefetch must not leave a running marker that prevents retry.
			f.data, f.ext, f.readError = data, "", false
			runTestPrefetch(t, dir, f, name)
			body, err := os.ReadFile(ready)
			require.NoError(t, err)
			require.Equal(t, data, body)
		})
	}
}

func TestPrefetchLeavesExistingRunningFileAlone(t *testing.T) {
	dir := t.TempDir()
	old := viper.Get(conf.PrefetchDir)
	viper.Set(conf.PrefetchDir, dir)
	defer viper.Set(conf.PrefetchDir, old)
	name := "000000010000000000000001"
	_, runningDir, running, ready := getPrefetchLocations(dir, name)
	require.NoError(t, os.MkdirAll(runningDir, 0700))
	require.NoError(t, os.WriteFile(running, []byte("other prefetcher's data"), 0600))
	f := &prefetchTestFolder{readError: true}
	runTestPrefetch(t, dir, f, name)
	body, err := os.ReadFile(running)
	require.NoError(t, err)
	require.Equal(t, "other prefetcher's data", string(body))
	require.Zero(t, f.calls.Load())
	_, err = os.Stat(ready)
	require.True(t, os.IsNotExist(err))
}
