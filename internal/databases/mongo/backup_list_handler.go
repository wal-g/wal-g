package mongo

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BackupDetail struct {
	BackupName string    `json:"backup_name"`
	ModifyTime time.Time `json:"modify_time"`

	StartTS primitive.Timestamp `json:"ts_start"`
	EndTS   primitive.Timestamp `json:"ts_end"`

	StartLocalTime  time.Time `json:"start_local_time"`
	FinishLocalTime time.Time `json:"stop_local_time"`

	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	CompressedSize   int64  `json:"compressed_size,omitempty"`
	Hostname         string `json:"hostname,omitempty"`

	IsPermanent bool        `json:"is_permanent"`
	UserData    interface{} `json:"user_data,omitempty"`
}

func NewBinaryBackupDetail(backupTime internal.BackupTime, sentinel *binary.MongodBackupSentinel) *BackupDetail {
	return &BackupDetail{
		BackupName:       backupTime.BackupName,
		ModifyTime:       backupTime.Time,
		StartTS:          sentinel.MongodMeta.StartTS,
		EndTS:            sentinel.MongodMeta.EndTS,
		StartLocalTime:   sentinel.StartLocalTime,
		FinishLocalTime:  sentinel.FinishLocalTime,
		UncompressedSize: sentinel.UncompressedDataSize,
		CompressedSize:   sentinel.CompressedDataSize,
		Hostname:         sentinel.Hostname,
		IsPermanent:      sentinel.Permanent,
		UserData:         sentinel.UserData,
	}
}

func NewLogicalBackupDetail(backupTime internal.BackupTime, sentinel *models.Backup) *BackupDetail {
	return &BackupDetail{
		BackupName:      backupTime.BackupName,
		ModifyTime:      backupTime.Time,
		StartTS:         sentinel.MongoMeta.Before.LastMajTS.ToBsonTS(),
		EndTS:           sentinel.MongoMeta.After.LastMajTS.ToBsonTS(),
		StartLocalTime:  sentinel.StartLocalTime,
		FinishLocalTime: sentinel.FinishLocalTime,
		//UncompressedSize: sentinel.UncompressedDataSize,
		CompressedSize: sentinel.DataSize,
		//Hostname:         sentinel.Hostname,
		IsPermanent: sentinel.Permanent,
		UserData:    sentinel.UserData,
	}
}

func HandleDetailedBackupList(folder storage.Folder, output io.Writer, pretty, json bool) error {
	backupTimes, err := internal.GetBackups(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch list of backups in storage: %s", err)

	if err != nil {
		return err
	}

	backupDetails := make([]*BackupDetail, 0, len(backupTimes))
	for _, backupTime := range backupTimes {
		backup := internal.NewBackup(folder, backupTime.BackupName)

		if strings.HasPrefix(backup.Name, "binary") {
			var sentinel binary.MongodBackupSentinel
			err = backup.FetchSentinel(&sentinel)
			tracelog.ErrorLogger.FatalfOnError("Failed to load sentinel for backup %s", err)

			backupDetails = append(backupDetails, NewBinaryBackupDetail(backupTime, &sentinel))
		} else {
			var sentinel models.Backup
			err = backup.FetchSentinel(&sentinel)
			tracelog.ErrorLogger.FatalfOnError("Failed to load sentinel for backup %s", err)
			if sentinel.BackupName == "" {
				sentinel.BackupName = backup.Name
			}

			backupDetails = append(backupDetails, NewLogicalBackupDetail(backupTime, &sentinel))
		}
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
		"#", "Name", "Last modified", "Start time", "Finish time", "Hostname", "Start Ts", "End Ts",
		"Uncompressed size", "Compressed size", "Permanent", "User data"})
	for i, backupDetail := range backupDetails {
		writer.AppendRow(table.Row{
			i,
			backupDetail.BackupName,
			backupDetail.ModifyTime.Format(time.RFC3339),
			backupDetail.StartLocalTime.Format(time.RFC3339),
			backupDetail.FinishLocalTime.Format(time.RFC3339),
			backupDetail.Hostname,
			backupDetail.StartTS,
			backupDetail.EndTS,
			backupDetail.UncompressedSize,
			backupDetail.CompressedSize,
			backupDetail.IsPermanent,
			marshalUserData(backupDetail.UserData),
		})
	}
}

func printBackupDetailsWithTabFormatter(backupDetails []*BackupDetail, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() { _ = writer.Flush() }()
	_, err := fmt.Fprintln(writer,
		"name\tlast_modified\tstart_time\tfinish_time\thostname\tstart_ts\tend_ts\tuncompressed_size"+
			"\tcompressed_size\tpermanent\tuser_data")
	if err != nil {
		return err
	}
	for _, backupDetail := range backupDetails {
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%s\n",
			backupDetail.BackupName,
			backupDetail.ModifyTime.Format(time.RFC3339),
			backupDetail.StartLocalTime.Format(time.RFC3339),
			backupDetail.FinishLocalTime.Format(time.RFC3339),
			backupDetail.Hostname,
			backupDetail.StartTS,
			backupDetail.EndTS,
			backupDetail.UncompressedSize,
			backupDetail.CompressedSize,
			backupDetail.IsPermanent,
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
		rawUserData = []byte("<marshall_error>")
	}
	return string(rawUserData)
}
