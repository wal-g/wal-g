package postgres

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"

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

func PrepareMultiStorageWalUploader(folder storage.Folder, targetStorage string) (*WalUploader, error) {
	folder = multistorage.SetPolicies(folder, policies.TakeFirstStorage)
	var err error
	if targetStorage == "" {
		folder, err = multistorage.UseFirstAliveStorage(folder)
	} else {
		folder, err = multistorage.UseSpecificStorage(targetStorage, folder)
	}
	if err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Printf("Files will be uploaded to storage: %v", multistorage.UsedStorages(folder)[0])

	baseUploader, err := internal.ConfigureUploaderToFolder(folder)
	if err != nil {
		return nil, fmt.Errorf("configure base uploader: %w", err)
	}

	walUploader, err := ConfigureWalUploader(baseUploader)
	if err != nil {
		return nil, fmt.Errorf("configure wal uploader: %w", err)
	}

	archiveStatusManager, err := internal.ConfigureArchiveStatusManager()
	if err == nil {
		walUploader.ArchiveStatusManager = asm.NewDataFolderASM(archiveStatusManager)
	} else {
		tracelog.ErrorLogger.PrintError(err)
		walUploader.ArchiveStatusManager = asm.NewNopASM()
	}

	PGArchiveStatusManager, err := internal.ConfigurePGArchiveStatusManager()
	if err == nil {
		walUploader.PGArchiveStatusManager = asm.NewDataFolderASM(PGArchiveStatusManager)
	} else {
		tracelog.ErrorLogger.PrintError(err)
		walUploader.PGArchiveStatusManager = asm.NewNopASM()
	}

	walUploader.ChangeDirectory(utility.WalPath)
	return walUploader, nil
}
