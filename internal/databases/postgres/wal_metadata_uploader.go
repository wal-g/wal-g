package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/fs"
)

const (
	WalBulkMetadataLevel       = "BULK"
	WalIndividualMetadataLevel = "INDIVIDUAL"
	WalNoMetadataLevel         = "NOMETADATA"
)

var WalMetadataLevels = []string{WalBulkMetadataLevel, WalIndividualMetadataLevel, WalNoMetadataLevel}

type WalMetadataDescription struct {
	CreatedTime    time.Time `json:"created_time"`
	DatetimeFormat string    `json:"date_fmt"`
}

type WalMetadataUploader struct {
	useBulkMetadataUpload bool
	walMetadataFolder     *fs.Folder
}

func NewWalMetadataUploader(walMetadataSetting string) (*WalMetadataUploader, error) {
	if err := checkWalMetadataLevel(walMetadataSetting); err != nil {
		return nil, err
	}

	walMetadataUploader := &WalMetadataUploader{}

	if walMetadataSetting == WalBulkMetadataLevel {
		walMetadataUploader.useBulkMetadataUpload = true
		walMetadataUploader.walMetadataFolder = fs.NewFolder(internal.GetRelativeArchiveDataFolderPath(), "")
	}

	return walMetadataUploader, nil
}

func (u *WalMetadataUploader) UploadWalMetadata(
	ctx context.Context,
	walFileName string,
	createdTime time.Time,
	uploader internal.Uploader,
) error {
	var walMetadata WalMetadataDescription
	walMetadataMap := make(map[string]WalMetadataDescription)
	walMetadataName := walFileName + ".json"
	walMetadata.DatetimeFormat = MetadataDatetimeFormat
	walMetadata.CreatedTime = createdTime
	walMetadataMap[walFileName] = walMetadata

	dtoBody, err := json.Marshal(walMetadataMap)
	if err != nil {
		return errors.Wrapf(err, "Unable to marshal walmetadata")
	}
	if u.useBulkMetadataUpload {
		err = u.walMetadataFolder.PutObject(walMetadataName, bytes.NewReader(dtoBody))
		if err != nil {
			return errors.Wrapf(err, "upload: could not Upload metadata'%s'\n", walFileName)
		}
		err = u.uploadBulkMetadataFile(ctx, walFileName, uploader)
	} else {
		err = uploader.Upload(ctx, walMetadataName, bytes.NewReader(dtoBody))
	}
	return errors.Wrapf(err, "upload: could not Upload metadata'%s'\n", walFileName)
}

func (u *WalMetadataUploader) uploadBulkMetadataFile(ctx context.Context, walFileName string, uploader internal.Uploader) error {
	// Creating consolidated wal metadata only for bulk option
	// Checking if the walfile name ends with "F" (last file in the series) and consolidating all
	// the metadata together.
	// For example, All the metadata for the files in the series 000000030000000800000010,
	//  000000030000000800000011 to 00000003000000080000001F
	// will be consolidated together and single  file 00000003000000080000001.json will be created.
	// Parameter isSourceWalPush will identify if the source of the file is from wal-push or from wal-receive.
	if walFileName[len(walFileName)-1:] != "F" {
		return nil
	}

	walSearchString := walFileName[0 : len(walFileName)-1]
	walMetadataFiles, err := filepath.Glob(u.walMetadataFolder.GetFilePath("") + "/" + walSearchString + "*.json")
	if err != nil {
		return err
	}

	walMetadata := make(map[string]WalMetadataDescription)
	walMetadataArray := make(map[string]WalMetadataDescription)

	for _, walMetadataFile := range walMetadataFiles {
		file, err := os.ReadFile(walMetadataFile)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(file, &walMetadata); err != nil {
			return err
		}

		for k := range walMetadata {
			walMetadataArray[k] = walMetadata[k]
		}
	}
	dtoBody, err := json.Marshal(walMetadataArray)
	if err != nil {
		return err
	}
	if err = uploader.Upload(ctx, walSearchString+".json", bytes.NewReader(dtoBody)); err != nil {
		return err
	}
	//Deleting the temporary metadata files created
	for _, walMetadataFile := range walMetadataFiles {
		if err = os.Remove(walMetadataFile); err != nil {
			tracelog.InfoLogger.Printf("Unable to remove walmetadata file %s", walMetadataFile)
		}
	}
	return errors.Wrapf(err, "Unable to upload bulk wal metadata %s", walFileName)
}

func checkWalMetadataLevel(walMetadataLevel string) error {
	isCorrect := false
	for _, level := range WalMetadataLevels {
		if walMetadataLevel == level {
			isCorrect = true
		}
	}
	if !isCorrect {
		return errors.Errorf("got incorrect Wal metadata  level: '%s', expected one of: '%v'",
			walMetadataLevel, WalMetadataLevels)
	}
	return nil
}

func uploadLocalWalMetadata(ctx context.Context, walFilePath string, uploader internal.Uploader) error {
	walMetadataSetting := viper.GetString(internal.UploadWalMetadata)
	if walMetadataSetting == WalNoMetadataLevel {
		return nil
	}

	walMetadataUploader, err := NewWalMetadataUploader(walMetadataSetting)
	if err != nil {
		return err
	}

	fileStat, err := os.Stat(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not stat wal file'%s'\n", walFilePath)
	}
	createdTime := fileStat.ModTime().UTC()
	walFileName := path.Base(walFilePath)

	return walMetadataUploader.UploadWalMetadata(ctx, walFileName, createdTime, uploader)
}

func uploadRemoteWalMetadata(ctx context.Context, walFileName string, uploader internal.Uploader) error {
	walMetadataSetting := viper.GetString(internal.UploadWalMetadata)
	if walMetadataSetting == WalNoMetadataLevel {
		return nil
	}

	walMetadataUploader, err := NewWalMetadataUploader(walMetadataSetting)
	if err != nil {
		return err
	}

	//Identifying timestamp of the WAL file generated will be bit different as wal-receive can run from any remote
	//machine and may not have access to the pg_wal/pg_xlog folder on the postgres cluster machine.
	createdTime := time.Now().UTC()

	return walMetadataUploader.UploadWalMetadata(ctx, walFileName, createdTime, uploader)
}
