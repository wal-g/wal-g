package internal_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression/none"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/test/mocks"
	"go.uber.org/mock/gomock"
)

func TestConcurrentDownloaderFinalizesCustomTarInterpreter(t *testing.T) {
	const backupName = "backup"
	folder := fs.NewFolder(t.TempDir(), "")
	archiveFolder := folder.GetSubFolder(backupName + internal.TarPartitionFolderName)
	require.NoError(t, archiveFolder.PutObject(t.Context(), "part.tar", bytes.NewReader(testTar(t))))
	uploader := internal.NewRegularUploader(none.Compressor{}, folder)
	downloader := internal.CreateConcurrentDownloader(uploader, nil)
	interpreter := &finalizingTestTarInterpreter{}

	require.NoError(t, downloader.DownloadWithTarInterpreter(
		t.Context(), backupName, t.TempDir(), nil, interpreter,
	))
	require.Equal(t, "payload", interpreter.data)
	require.True(t, interpreter.finalized)
}

func TestConcurrentDownloaderReturnsFinalizerError(t *testing.T) {
	const backupName = "backup"
	folder := fs.NewFolder(t.TempDir(), "")
	archiveFolder := folder.GetSubFolder(backupName + internal.TarPartitionFolderName)
	require.NoError(t, archiveFolder.PutObject(t.Context(), "part.tar", bytes.NewReader(testTar(t))))
	uploader := internal.NewRegularUploader(none.Compressor{}, folder)
	downloader := internal.CreateConcurrentDownloader(uploader, nil)
	interpreter := &finalizingTestTarInterpreter{finalizeErr: errors.New("incomplete files")}

	err := downloader.DownloadWithTarInterpreter(t.Context(), backupName, t.TempDir(), nil, interpreter)
	require.ErrorContains(t, err, "incomplete files")
}

func TestConcurrentDownloaderRejectsArchiveSubfolders(t *testing.T) {
	const backupName = "backup"
	folder := fs.NewFolder(t.TempDir(), "")
	archiveFolder := folder.GetSubFolder(backupName + internal.TarPartitionFolderName)
	archiveFolder.GetSubFolder("unexpected")
	uploader := internal.NewRegularUploader(none.Compressor{}, folder)
	downloader := internal.CreateConcurrentDownloader(uploader, nil)

	err := downloader.Download(t.Context(), backupName, t.TempDir(), nil)
	require.ErrorContains(t, err, "unknown subfolders")
}

func TestConcurrentDownloaderUsesFilterWithoutListing(t *testing.T) {
	const backupName = "backup"
	ctrl := gomock.NewController(t)
	rootFolder := mocks.NewMockFolder(ctrl)
	tarsFolder := mocks.NewMockFolder(ctrl)
	rootFolder.EXPECT().GetSubFolder("backup/tar_partitions").Return(tarsFolder)
	tarsFolder.EXPECT().ReadObject(gomock.Any(), "selected.tar").Return(
		io.NopCloser(bytes.NewReader(testTar(t))), nil,
	)
	uploader := internal.NewRegularUploader(none.Compressor{}, rootFolder)
	downloader := internal.CreateConcurrentDownloader(uploader, nil)
	interpreter := &finalizingTestTarInterpreter{}

	require.NoError(t, downloader.DownloadWithTarInterpreter(
		t.Context(), backupName, t.TempDir(), map[string]struct{}{"selected.tar": {}}, interpreter,
	))
	require.Equal(t, "payload", interpreter.data)
	require.True(t, interpreter.finalized)
}

func TestConcurrentDownloaderRejectsNonLocalFilteredArchiveName(t *testing.T) {
	const backupName = "backup"
	ctrl := gomock.NewController(t)
	rootFolder := mocks.NewMockFolder(ctrl)
	tarsFolder := mocks.NewMockFolder(ctrl)
	rootFolder.EXPECT().GetSubFolder("backup/tar_partitions").Return(tarsFolder)
	uploader := internal.NewRegularUploader(none.Compressor{}, rootFolder)
	downloader := internal.CreateConcurrentDownloader(uploader, nil)

	err := downloader.Download(t.Context(), backupName, t.TempDir(), map[string]struct{}{"../part.tar": {}})
	require.ErrorContains(t, err, `invalid tar object name "../part.tar" in filter`)
}

func testTar(t *testing.T) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	header := &tar.Header{Name: "file.data", Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len("payload"))}
	require.NoError(t, writer.WriteHeader(header))
	_, err := writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return archive.Bytes()
}

type finalizingTestTarInterpreter struct {
	data        string
	finalized   bool
	finalizeErr error
}

func (interpreter *finalizingTestTarInterpreter) Interpret(reader io.Reader, _ *tar.Header) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	interpreter.data = string(data)
	return nil
}

func (interpreter *finalizingTestTarInterpreter) Finalize() error {
	interpreter.finalized = true
	return interpreter.finalizeErr
}
