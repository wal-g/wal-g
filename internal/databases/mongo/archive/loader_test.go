package archive

import (
	"context"
	"io"
	"testing"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
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

	if err := su.UploadOplogArchive(r, models.Timestamp{TS: 100, Inc: 1}, models.Timestamp{TS: 120, Inc: 1}); err != nil {
		t.Errorf("UploadOplogArchive() error = %v", err)
	}
}
