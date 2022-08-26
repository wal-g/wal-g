package mongo

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupDetail struct {
	models.Backup
	ModifyTime time.Time `json:"modify_time"`
}

func NewBackupDetail(backupTime internal.BackupTime, sentinel *models.Backup) *BackupDetail {
	return &BackupDetail{
		Backup:     *sentinel,
		ModifyTime: backupTime.Time,
	}
}

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
		return backupDetails[i].FinishLocalTime.After(backupDetails[j].FinishLocalTime)
	})

	switch {
	case json:
		err = internal.WriteAsJSON(backupDetails, output, pretty)
	case pretty:
		printBackupDetailsPretty(backupDetails, output)
	default:
		err = printBackupDetailsWithTabFormatter(backupDetails, output)
	}
	return err
}

func printBackupDetailsPretty(backupDetails []*BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()

	writer.AppendHeader(table.Row{
		"#", "Name", "Type", "Version", "Last modified", "Start time", "Finish time", "Hostname", "Start Ts", "End Ts",
		"Uncompressed size", "Compressed size", "Permanent", "User data"})
	for i, backupDetail := range backupDetails {
		writer.AppendRow(table.Row{
			i,
			backupDetail.BackupName,
			backupDetail.BackupType,
			backupDetail.MongoMeta.Version,
			backupDetail.ModifyTime.Format(time.RFC3339),
			backupDetail.StartLocalTime.Format(time.RFC3339),
			backupDetail.FinishLocalTime.Format(time.RFC3339),
			backupDetail.Hostname,
			backupDetail.MongoMeta.Before.LastMajTS.ToBsonTS(),
			backupDetail.MongoMeta.After.LastMajTS.ToBsonTS(),
			backupDetail.UncompressedSize,
			backupDetail.CompressedSize,
			backupDetail.Permanent,
			marshalUserData(backupDetail.UserData),
		})
	}
}

func printBackupDetailsWithTabFormatter(backupDetails []*BackupDetail, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() { _ = writer.Flush() }()
	_, err := fmt.Fprintln(writer,
		"name\ttype\tversion\tlast_modified\tstart_time\tfinish_time\thostname\tstart_ts\tend_ts\tuncompressed_size"+
			"\tcompressed_size\tpermanent\tuser_data")
	if err != nil {
		return err
	}
	for _, backupDetail := range backupDetails {
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			backupDetail.BackupName,
			backupDetail.BackupType,
			backupDetail.MongoMeta.Version,
			backupDetail.ModifyTime.Format(time.RFC3339),
			backupDetail.StartLocalTime.Format(time.RFC3339),
			backupDetail.FinishLocalTime.Format(time.RFC3339),
			backupDetail.Hostname,
			backupDetail.MongoMeta.Before.LastMajTS.ToBsonTS(),
			backupDetail.MongoMeta.After.LastMajTS.ToBsonTS(),
			backupDetail.UncompressedSize,
			backupDetail.CompressedSize,
			backupDetail.Permanent,
			marshalUserData(backupDetail.UserData),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func marshalUserData(userData interface{}) string {
	rawUserData, err := json.Marshal(userData)
	if err != nil {
		rawUserData = []byte(fmt.Sprintf("{\"error\": \"unable to marshal %+v\"}", userData))
	}
	return string(rawUserData)
}
