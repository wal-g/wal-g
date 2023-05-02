package internal

import (
	"path/filepath"
	"sync/atomic"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

type DirectoryUploader interface {
	Upload(path string) TarFileSets
}

type CommonDirectoryUploader struct {
	crypter crypto.Crypter

	tarBallFilePacker    TarBallFilePacker
	tarBallComposerMaker TarBallComposerMaker
	tarSizeThreshold     int64

	excludedFiles map[string]utility.Empty
	backupName    string
	uploader      Uploader
}

func NewCommonDirectoryUploader(
	crypter crypto.Crypter, packer TarBallFilePacker,
	tarBallComposerMaker TarBallComposerMaker, tarSizeThreshold int64,
	excludedFiles map[string]utility.Empty, backupName string,
	uploader Uploader) *CommonDirectoryUploader {
	return &CommonDirectoryUploader{
		crypter:              crypter,
		tarBallFilePacker:    packer,
		tarBallComposerMaker: tarBallComposerMaker,
		tarSizeThreshold:     tarSizeThreshold,
		excludedFiles:        excludedFiles,
		backupName:           backupName,
		uploader:             uploader,
	}
}

func (u *CommonDirectoryUploader) Upload(path string) TarFileSets {
	bundle := NewBundle(path, u.crypter, u.tarSizeThreshold, u.excludedFiles)

	// Start a new tar bundle, walk the pgDataDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	err := bundle.StartQueue(NewStorageTarBallMaker(u.backupName, u.uploader))
	tracelog.ErrorLogger.FatalOnError(err)

	err = bundle.SetupComposer(u.tarBallComposerMaker)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(path, bundle.AddToBundle)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := bundle.FinishComposing()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	uncompressedSize := atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	compressedSize, err := u.uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("Uncompressed size: %d", uncompressedSize)
	tracelog.DebugLogger.Printf("Compressed size: %d", compressedSize)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	u.uploader.Finish()
	if u.uploader.Failed() {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", path)
	}
	return tarFileSets
}
