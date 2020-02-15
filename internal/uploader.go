package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
	"io"
	"path"
)

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type Uploader struct {
	*TrueUploader
	*DeltaFileManager
}

func (uploader *Uploader) getUseWalDelta() (useWalDelta bool) {
	return uploader.DeltaFileManager != nil
}

func NewUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
	deltaFileManager *DeltaFileManager,
) *Uploader {
	trueUploader := NewTrueUploader(compressor, uploadingLocation)

	return &Uploader{
		trueUploader,
		deltaFileManager,
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) clone() *Uploader {
	return &Uploader{
		uploader.TrueUploader.clone(),
		uploader.DeltaFileManager,
	}
}

// TODO : unit tests
func (uploader *Uploader) UploadWalFile(file NamedReader) error {
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
