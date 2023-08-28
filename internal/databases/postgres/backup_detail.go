package postgres

import (
	"fmt"
	"strconv"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
)

// BackupDetail is used to append ExtendedMetadataDto details to BackupTime struct
type BackupDetail struct {
	internal.BackupTime
	ExtendedMetadataDto
}

func (bd *BackupDetail) PrintableFields() []printlist.TableField {
	prettyStartTime := internal.PrettyFormatTime(bd.StartTime)
	prettyFinishTime := internal.PrettyFormatTime(bd.FinishTime)
	return append(bd.BackupTime.PrintableFields(),
		printlist.TableField{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       internal.FormatTime(bd.StartTime),
			PrettyValue: &prettyStartTime,
		},
		printlist.TableField{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       internal.FormatTime(bd.FinishTime),
			PrettyValue: &prettyFinishTime,
		},
		printlist.TableField{
			Name:       "hostname",
			PrettyName: "Hostname",
			Value:      bd.Hostname,
		},
		printlist.TableField{
			Name:       "data_dir",
			PrettyName: "Datadir",
			Value:      bd.DataDir,
		},
		printlist.TableField{
			Name:       "pg_version",
			PrettyName: "PG version",
			Value:      strconv.Itoa(bd.PgVersion),
		},
		printlist.TableField{
			Name:       "start_lsn",
			PrettyName: "Start LSN",
			Value:      bd.StartLsn.String(),
		},
		printlist.TableField{
			Name:       "finish_lsn",
			PrettyName: "Finish LSN",
			Value:      bd.FinishLsn.String(),
		},
		printlist.TableField{
			Name:       "is_permanent",
			PrettyName: "Permanent",
			Value:      fmt.Sprintf("%v", bd.IsPermanent),
		},
	)
}
