package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

type fileLifetimeUploader struct {
	internal.Uploader
	file *os.File
	err  error
}

func (u *fileLifetimeUploader) UploadFile(_ context.Context, r ioextensions.NamedReader) error {
	u.file = r.(*ioextensions.NamedReaderImpl).Reader.(*os.File)
	return u.err
}

func TestUploadWALFileClosesSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil}, {"failure", errors.New("storage upload failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "000000010000000000000001")
			require.NoError(t, os.WriteFile(name, []byte("WAL"), 0600))
			u := &fileLifetimeUploader{err: tc.err}
			err := uploadWALFile(t.Context(), NewWalUploader(u, nil), name, false)
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.err)
			}
			require.NotNil(t, u.file)
			// Stat may return a native invalid-handle error on Windows. Read
			// consistently reports os.ErrClosed for a closed file on all platforms.
			_, err = u.file.Read(make([]byte, 1))
			require.ErrorIs(t, err, os.ErrClosed)
		})
	}
}
