package aof

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/diskwatcher"

	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/utility"
)

type BackupService struct {
	Context                 context.Context
	DiskWatcher             *diskwatcher.DiskWatcher
	concurrentUploader      *internal.ConcurrentUploader
	metaConstructor         internal.MetaConstructor
	backupFilesListProvider *BackupFilesListProvider
	filesPinner             *FilesPinner
}

func GenerateNewBackupName() string {
	return "aof_" + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

type Fobj struct {
	fobj     *os.File
	isClosed bool
}

func (f *Fobj) Close() error {
	if f.isClosed {
		return nil
	}
	f.isClosed = true
	return f.fobj.Close()
}

func (f *Fobj) Name() string {
	return f.fobj.Name()
}

type FilesPinner struct {
	fobjs []Fobj
}

func NewFilesPinner() *FilesPinner {
	return &FilesPinner{}
}

func (p *FilesPinner) Pin(paths []string) error {
	for _, path := range paths {
		fobj, err := os.Open(path)
		if err != nil {
			return err
		}
		p.fobjs = append(p.fobjs, Fobj{fobj, false})
	}
	return nil
}

func (p *FilesPinner) Unpin() {
	for _, fobj := range p.fobjs {
		if fobj.isClosed {
			continue
		}
		err := fobj.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to close file %s: %v", fobj.Name(), err)
		}
	}
	p.fobjs = nil
}

func CreateBackupService(ctx context.Context, diskWatcher *diskwatcher.DiskWatcher, uploader *internal.ConcurrentUploader,
	metaConstructor internal.MetaConstructor, backupFilesListProvider *BackupFilesListProvider, filesPinner *FilesPinner,
) (*BackupService, error) {
	return &BackupService{
		Context:                 ctx,
		DiskWatcher:             diskWatcher,
		concurrentUploader:      uploader,
		backupFilesListProvider: backupFilesListProvider,
		filesPinner:             filesPinner,
		metaConstructor:         metaConstructor,
	}, nil
}

func (bs *BackupService) DoBackup(backupName string, permanent bool) error {
	err := bs.metaConstructor.Init()
	if err != nil {
		return errors.Wrapf(err, "can not init meta provider")
	}

	backupFiles := bs.backupFilesListProvider.Get()

	err = bs.filesPinner.Pin(backupFiles)
	defer bs.filesPinner.Unpin()
	if err != nil {
		return errors.Wrapf(err, "unable to prevent files list from removal")
	}

	backupMetas, err := internal.GetBackupFileMetas(backupFiles)
	if err != nil {
		return err
	}

	uploadErrChan := make(chan error)
	go func() {
		defer close(uploadErrChan)
		err := bs.concurrentUploader.UploadBackupFiles(backupMetas)
		if err != nil {
			uploadErrChan <- errors.Wrapf(err, "unable to upload backup files")
			return
		}
		uploadErrChan <- nil
	}()

	bs.DiskWatcher.Start()
	defer bs.DiskWatcher.Stop()

	select {
	case err := <-uploadErrChan:
		if err != nil {
			return err
		}
	case <-bs.DiskWatcher.Signaling:
		return fmt.Errorf("disk is filled above limit, exiting")
	}

	err = bs.concurrentUploader.Finalize()
	if err != nil {
		return err
	}

	return bs.Finalize(backupName)
}

func (bs *BackupService) Finalize(backupName string) error {
	if err := bs.metaConstructor.Finalize(backupName); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	backupSentinelInfo := bs.metaConstructor.MetaInfo()
	backup := backupSentinelInfo.(*archive.Backup)
	backup.BackupName = backupName
	backup.BackupSize = bs.concurrentUploader.CompressedSize
	backup.DataSize = bs.concurrentUploader.UncompressedSize
	if err := bs.concurrentUploader.UploadSentinel(backupSentinelInfo, backupName); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}
