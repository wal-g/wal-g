package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
	"io"
	"path"
)

// WalUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type WalUploader struct {
	*Uploader
	*DeltaFileManager
}

func (uploader *WalUploader) getUseWalDelta() (useWalDelta bool) {
	return uploader.DeltaFileManager != nil
}

func NewWalUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
	deltaFileManager *DeltaFileManager,
) *WalUploader {
	uploader := NewUploader(compressor, uploadingLocation)

	return &WalUploader{
		uploader,
		deltaFileManager,
	}
}

// Clone creates similar WalUploader with new WaitGroup
func (uploader *WalUploader) clone() *WalUploader {
	return &WalUploader{
		uploader.Uploader.clone(),
		uploader.DeltaFileManager,
	}
}

// TODO : unit tests
func (uploader *WalUploader) UploadWalFile(file NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if uploader.getUseWalDelta() && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, uploader.DeltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer utility.LoggedClose(recordingReader, "")
		}
	} else {
		walFileReader = file
	}

	return uploader.UploadFile(newNamedReaderImpl(walFileReader, file.Name()))
}
