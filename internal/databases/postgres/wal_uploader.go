package postgres

import (
	"context"
	"io"
	"path"

	"github.com/wal-g/wal-g/internal/asm"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

// WalUploader extends uploader with wal specific functionality.
type WalUploader struct {
	internal.Uploader
	ArchiveStatusManager   asm.ArchiveStatusManager
	PGArchiveStatusManager asm.ArchiveStatusManager
	*DeltaFileManager
}

func (walUploader *WalUploader) getUseWalDelta() (useWalDelta bool) {
	return walUploader.DeltaFileManager != nil
}

func NewWalUploader(
	baseUploader internal.Uploader,
	deltaFileManager *DeltaFileManager,
) *WalUploader {
	return &WalUploader{
		Uploader:         baseUploader,
		DeltaFileManager: deltaFileManager,
	}
}

// Clone creates similar WalUploader with new WaitGroup
func (walUploader *WalUploader) clone() *WalUploader {
	return &WalUploader{
		Uploader:               walUploader.Uploader.Clone(),
		ArchiveStatusManager:   walUploader.ArchiveStatusManager,
		PGArchiveStatusManager: walUploader.PGArchiveStatusManager,
		DeltaFileManager:       walUploader.DeltaFileManager,
	}
}

// TODO : unit tests
func (walUploader *WalUploader) UploadWalFile(ctx context.Context, file ioextensions.NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if walUploader.getUseWalDelta() && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, walUploader.DeltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer utility.LoggedClose(recordingReader, "")
		}
	} else {
		walFileReader = file
	}

	return walUploader.UploadFile(ctx, ioextensions.NewNamedReaderImpl(walFileReader, file.Name()))
}

func (walUploader *WalUploader) FlushFiles(ctx context.Context) {
	walUploader.DeltaFileManager.FlushFiles(ctx, walUploader)
}
