package mysql

import (
	"encoding/json"
	"github.com/wal-g/storages/storage"
	tracelog "github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/xerrors"
	"io"
)

type BackupMetadataReader struct {
	Dto internal.ExtendedMetadataDto
}

func (b BackupMetadataReader) Read(p []byte) (n int, err error) {

	msg, err := json.Marshal(b.Dto)
	if err != nil {
		return 0, err
	}

	cnt := copy(p, msg)
	return cnt, nil
}

func NewBackupMetadataReader(dto internal.ExtendedMetadataDto) BackupMetadataReader {
	return BackupMetadataReader{
		Dto: dto,
	}
}

func BackupMetadataToUpload(
	backup *internal.Backup,
	flagsAdd []string,
	flagsErase []string) (io.Reader, error) {

	meta, err := backup.FetchMeta()
	if err != nil {
		return nil, err
	}
	flags, ok := meta.UserData.([]string)
	if !ok {
		return nil, xerrors.Errorf("faailed to convert to string slcie %s", meta.UserData)
	}

	filter := make(map[string]struct{})
	for _, e := range flagsErase {
		filter[e] = struct{}{}
	}

	result := make([]string, 0, 0)
	for _, e := range flags {
		if _, ok := filter[e]; ok {
			// erase this flag
			continue
		}
		result = append(result, e)
	}

	meta.UserData = append(result, flagsAdd...)

	return NewBackupMetadataReader(meta), nil
}

func ChattrBackup(uploader *internal.Uploader, folder storage.Folder, backupName string, flagsAdd []string, flagsErase []string) {

	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backup := internal.NewBackup(baseBackupFolder, backupName)

	metadataToUpload, err := BackupMetadataToUpload(backup, flagsAdd, flagsErase)

	tracelog.ErrorLogger.FatalOnError(err)

	err = uploader.Upload(backup.MetadataPath(), metadataToUpload)

	tracelog.ErrorLogger.FatalOnError(err)
}
