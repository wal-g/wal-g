package postgres

import (
	"io"
	"path"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// WalUploader extends uploader with wal specific functionality.
type WalUploader struct {
	*internal.Uploader
	*DeltaFileManager
}

func (walUploader *WalUploader) getUseWalDelta() (useWalDelta bool) {
	return walUploader.DeltaFileManager != nil
}

func NewWalUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
	deltaFileManager *DeltaFileManager,
) *WalUploader {
	uploader := internal.NewUploader(compressor, uploadingLocation)

	return &WalUploader{
		uploader,
		deltaFileManager,
	}
}

// Clone creates similar WalUploader with new WaitGroup
func (walUploader *WalUploader) clone() *WalUploader {
	return &WalUploader{
		walUploader.Uploader.Clone(),
		walUploader.DeltaFileManager,
	}
}

// TODO : unit tests
func (walUploader *WalUploader) UploadWalFile(file ioextensions.NamedReader) error {
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

	return walUploader.UploadFile(ioextensions.NewNamedReaderImpl(walFileReader, file.Name()))
}

func (walUploader *WalUploader) FlushFiles() {
	walUploader.DeltaFileManager.FlushFiles(walUploader.Uploader)
}
