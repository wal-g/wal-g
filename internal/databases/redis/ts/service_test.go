package ts

import (
	"archive/tar"
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/testtools"
)

func init() {
	internal.ConfigureSettings("")
	config.InitConfig()
	config.Configure()
}

func TestFetchHonorsSkipClean(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	dataPrefix := "ts_20260721T120000Z"
	tarData := makeTar(t, "restored.data", []byte("restored"))
	require.NoError(t, folder.PutObject(
		t.Context(), dataPrefix+internal.TarPartitionFolderName+"part.tar", bytes.NewReader(tarData),
	))

	t.Run("default clean removes stale target contents", func(t *testing.T) {
		target := t.TempDir()
		stalePath := target + "/stale.data"
		require.NoError(t, os.WriteFile(stalePath, []byte("stale"), 0o600))

		require.NoError(t, Fetch(t.Context(), FetchArgs{
			Folder:     folder,
			DataPrefix: dataPrefix,
			TargetDir:  target,
		}))

		assert.NoFileExists(t, stalePath)
		assertFileContents(t, target+"/restored.data", []byte("restored"))
	})

	t.Run("skip clean preserves target contents and extracts ts payload", func(t *testing.T) {
		target := t.TempDir()
		stalePath := target + "/stale.data"
		require.NoError(t, os.WriteFile(stalePath, []byte("stale"), 0o600))

		require.NoError(t, Fetch(t.Context(), FetchArgs{
			Folder:     folder,
			DataPrefix: dataPrefix,
			TargetDir:  target,
			SkipClean:  true,
		}))

		assertFileContents(t, stalePath, []byte("stale"))
		assertFileContents(t, target+"/restored.data", []byte("restored"))
	})
}

func assertFileContents(t *testing.T, path string, expected []byte) {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, expected, contents)
}

func makeTar(t *testing.T, name string, contents []byte) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(contents))}))
	_, err := writer.Write(contents)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
