package archive

import (
	"context"
	"io"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/test/mocks"
)

// TestStorageUploader_UploadOplogArchive_ProperInterfaces ensures storage layer receives proper stream
// s3 library enables caches when stream content can be cast to io.ReaderAt and io.ReadSeeker interfaces
func TestStorageUploader_UploadOplogArchive_ProperInterfaces(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	storageProv := mocks.NewMockFolder(mockCtl)
	storageProv.EXPECT().PutObjectWithContext(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(_ context.Context, _ string, content io.Reader) error {
		if _, ok := content.(io.ReaderAt); !ok {
			t.Errorf("can not cast PutObject content to io.ReaderAt")
		}
		if _, ok := content.(io.ReadSeeker); !ok {
			t.Errorf("can not cast PutObject content to io.ReadSeeker")
		}
		return nil
	})

	uploaderProv := internal.NewRegularUploader(compression.Compressors[lz4.AlgorithmName], storageProv)
	su := NewStorageUploader(uploaderProv)
	r, w := io.Pipe()
	go func() {
		n, err := w.Write([]byte("test_data_stream"))
		assert.Equal(t, 16, n)
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
	}()

	firstTS := models.Timestamp{TS: 100, Inc: 1}
	lastTS := models.Timestamp{TS: 120, Inc: 1}
	if err := su.UploadOplogArchive(context.Background(), r, firstTS, lastTS); err != nil {
		t.Errorf("UploadOplogArchive() error = %v", err)
	}
}

func TestStorageDownloaderListOplogArchivesSegmentFallsBackToListFolder(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	arch := mustArchive(t, models.Timestamp{TS: 100, Inc: 1}, models.Timestamp{TS: 130, Inc: 1})
	folder := mocks.NewMockFolder(mockCtl)
	folder.EXPECT().GetPath().Return(models.OplogArchBasePath).AnyTimes()
	folder.EXPECT().ListFolder().Return(
		[]storage.Object{storage.NewLocalObject(arch.Filename(), time.Time{}, 0)},
		nil,
		nil,
	)

	downloader := &StorageDownloader{oplogsFolder: folder}

	var (
		got []models.Archive
		err error
	)
	assert.NotPanics(t, func() {
		got, err = downloader.ListOplogArchivesSegment(nil, nil)
	})
	require.NoError(t, err)
	assert.Equal(t, []models.Archive{arch}, got)
}

func TestStorageDownloaderLastKnownArchiveTSUsesSegmentResults(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	arch := mustArchive(t, models.Timestamp{TS: 100, Inc: 1}, models.Timestamp{TS: 130, Inc: 1})
	folder := mocks.NewMockFolderExt(mockCtl)
	var segmentCalls int
	folder.EXPECT().ListFolderSegment(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(_, _ *string) ([]storage.Object, []storage.Folder, error) {
			segmentCalls++
			return []storage.Object{storage.NewLocalObject(arch.Filename(), time.Time{}, 0)}, nil, nil
		},
	)
	folder.EXPECT().GetPath().AnyTimes()
	folder.EXPECT().ListFolder().Times(0)
	downloader := &StorageDownloader{oplogsFolder: folder}

	got, err := downloader.LastKnownArchiveTS()
	require.NoError(t, err)
	assert.Equal(t, arch.End, got)
	assert.Equal(t, 1, segmentCalls)
}

func mustArchive(t *testing.T, start, end models.Timestamp) models.Archive {
	t.Helper()

	arch, err := models.NewArchive(start, end, "lz4", models.ArchiveTypeOplog)
	require.NoError(t, err)
	return arch
}
