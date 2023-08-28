package mongo

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupDetail struct {
	models.Backup
	ModifyTime time.Time `json:"modify_time"`
}

func (bd *BackupDetail) PrintableFields() []printlist.TableField {
	prettyModifyTime := internal.PrettyFormatTime(bd.ModifyTime)
	return append(bd.Backup.PrintableFields(),
		printlist.TableField{
			Name:        "modify_time",
			PrettyName:  "Modify time",
			Value:       internal.FormatTime(bd.ModifyTime),
			PrettyValue: &prettyModifyTime,
		},
	)
}

func NewBackupDetail(backupTime internal.BackupTime, sentinel *models.Backup) *BackupDetail {
	return &BackupDetail{
		Backup:     *sentinel,
		ModifyTime: backupTime.Time,
	}
}

// TODO: unit tests
func HandleDetailedBackupList(folder storage.Folder, output io.Writer, pretty, json bool) error {
	backupTimes, err := internal.GetBackups(folder)
	if err != nil {
		return err
	}

	backupDetails := make([]*BackupDetail, 0, len(backupTimes))
	for _, backupTime := range backupTimes {
		sentinel, err := common.DownloadSentinel(folder, backupTime.BackupName)
		if err != nil {
			return errors.Wrapf(err, "Unable to load sentinel of backup %v", backupTime.BackupName)
		}
		backupDetail := NewBackupDetail(backupTime, sentinel)
		backupDetails = append(backupDetails, backupDetail)
	}

	sort.Slice(backupDetails, func(i, j int) bool {
		return backupDetails[i].FinishLocalTime.Before(backupDetails[j].FinishLocalTime)
	})

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = backupDetails[i]
	}
	err = printlist.List(printableEntities, output, pretty, json)
	if err != nil {
		return fmt.Errorf("print backups: %w", err)
	}
	return nil
}
