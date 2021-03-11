package internal

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"

	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type CantOverwriteWalFileError struct {
	error
}

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

func checkWalMetadataLevel(walMetadataLevel string) error {
	isCorrect := false
	for _, level := range WalMetadataLevels {
		if walMetadataLevel == level {
			isCorrect = true
		}
	}
	if !isCorrect {
		return errors.Errorf("got incorrect Wal metadata  level: '%s', expected one of: '%v'", walMetadataLevel, WalMetadataLevels)
	}
	return nil
}

func newCantOverwriteWalFileError(walFilePath string) CantOverwriteWalFileError {
	return CantOverwriteWalFileError{errors.Errorf("WAL file '%s' already archived, contents differ, unable to overwrite", walFilePath)}
}

func (err CantOverwriteWalFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *WalUploader, walFilePath string) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)
	if uploader.ArchiveStatusManager.IsWalAlreadyUploaded(walFilePath) {
		err := uploader.ArchiveStatusManager.UnmarkWalFile(walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		if viper.GetString(UploadWalMetadata) == WalBulkMetadataLevel && walFilePath[len(walFilePath)-1:] == "F" {
			uploadBulkMetadata(uploader, walFilePath)
		}
		return
	}

	concurrency, err := GetMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	totalBgUploadedLimit := viper.GetInt32(TotalBgUploadedLimit)
	preventWalOverwrite := viper.GetBool(PreventWalOverwriteSetting)

	bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), totalBgUploadedLimit-1, uploader, preventWalOverwrite)
	// Look for new WALs while doing main upload
	bgUploader.Start()
	err = uploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite)
	tracelog.ErrorLogger.FatalOnError(err)
	if err == nil && viper.GetString(UploadWalMetadata) == WalBulkMetadataLevel && walFilePath[len(walFilePath)-1:] == "F" {
		// Creating consolidated wal metadata only for bulk option
		// Checking if the walfile name ends with "F" (last file in the series) and consolidating all the metadata together.
		// For example, All the metadata for the files in the series 000000030000000800000010, 000000030000000800000011 to 00000003000000080000001F
		// will be consolidated together and single  file 00000003000000080000001.json will be created.
		uploadBulkMetadata(uploader, walFilePath)
	}

	err = bgUploader.Stop()
	tracelog.ErrorLogger.FatalOnError(err)

	if uploader.getUseWalDelta() {
		uploader.FlushFiles()
	}
} //

func uploadBulkMetadata(uploader *WalUploader, walFilePath string) {

	walMetadataFolder := fs.NewFolder(getArchiveDataFolderPath(), "")
	walFileName := filepath.Base(walFilePath)
	walSearchString := walFileName[0 : len(walFileName)-1]
	walMetadataFiles, _ := filepath.Glob(walMetadataFolder.GetFilePath("") + "/" + walSearchString + "*.json")

	walMetadata := make(map[string]WalMetadataDescription)
	walMetadataArray := make(map[string]WalMetadataDescription)

	for _, walMetadataFile := range walMetadataFiles {
		file, _ := ioutil.ReadFile(walMetadataFile)
		err := json.Unmarshal(file, &walMetadata)
		if err == nil {
			for k := range walMetadata {
				walMetadataArray[k] = walMetadata[k]
			}
		}
	}
	dtoBody, _ := json.Marshal(walMetadataArray)
	_ = uploader.Upload(walSearchString+".json", bytes.NewReader(dtoBody))
	//Deleting the temporary metadata files created
	for _, walMetadataFile := range walMetadataFiles {
		err := os.Remove(walMetadataFile)
		if err != nil {
			tracelog.InfoLogger.Printf("Unable to remove walmetadata file %s", walMetadataFile)
		}
	}
}

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(uploader *WalUploader, walFilePath string, preventWalOverwrite bool) error {
	if preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
		if overwriteAttempt {
			return err
		} else if err != nil {
			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
		}
	}
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", walFilePath)
	}
	err = uploader.UploadWalFile(walFile)
	if err == nil && viper.GetString(UploadWalMetadata) != WalNoMetadataLevel {
		err = uploadWALMetadataFile(uploader, walFilePath)
		if err != nil {
			return errors.Wrapf(err, "Failed to upload metadata file")
		}
	}
	return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
}

// Function to upload WAL Metadata file based on the parameter passed
func uploadWALMetadataFile(uploader *WalUploader, walFilePath string) error {
	err := checkWalMetadataLevel(viper.GetString(UploadWalMetadata))
	if err != nil {
		return errors.Wrapf(err, "Incorrect wal metadata level")
	}
	fileStat, err := os.Stat(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not stat wal file'%s'\n", walFilePath)
	}
	var walMetadata WalMetadataDescription
	walMetadataMap := make(map[string]WalMetadataDescription)
	walName := fileStat.Name()
	walMetadataName := walName + ".json"
	walMetadata.CreatedTime = fileStat.ModTime().UTC()
	walMetadata.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	walMetadataMap[walName] = walMetadata

	dtoBody, err := json.Marshal(walMetadataMap)
	if err != nil {
		return errors.Wrapf(err, "Unable to marshal walmetadata")
	}
	if viper.GetString(UploadWalMetadata) == WalBulkMetadataLevel {
		walMetadataFolder := fs.NewFolder(getArchiveDataFolderPath(), "")
		err = walMetadataFolder.PutObject(walMetadataName, bytes.NewReader(dtoBody))
	} else {
		err = uploader.Upload(walMetadataName, bytes.NewReader(dtoBody))
	}
	return errors.Wrapf(err, "upload: could not Upload metadata'%s'\n", walFilePath)
}

// TODO : unit tests
func checkWALOverwrite(uploader *WalUploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := DownloadAndDecompressStorageFile(uploader.UploadingFolder, filepath.Base(walFilePath))
	if err != nil {
		if _, ok := err.(ArchiveNonExistenceError); ok {
			err = nil
		}
		return false, err
	}

	archived, err := ioutil.ReadAll(walFileReader)
	if err != nil {
		return false, err
	}

	localBytes, err := ioutil.ReadFile(walFilePath)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(archived, localBytes) {
		return true, newCantOverwriteWalFileError(walFilePath)
	} else {
		tracelog.InfoLogger.Printf("WAL file '%s' already archived with equal content, skipping", walFilePath)
		return true, nil
	}
}
