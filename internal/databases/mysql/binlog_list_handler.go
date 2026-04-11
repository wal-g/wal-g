package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type BinlogInfo struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size,omitempty"`
	LastModified time.Time `json:"last_modified,omitempty"`
}

func (b BinlogInfo) PrintableFields() []printlist.TableField {
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      b.Name,
		},
		{
			Name:       "size",
			PrettyName: "Size",
			Value:      fmt.Sprintf("%d", b.Size),
		},
		{
			Name:       "modified",
			PrettyName: "Modified",
			Value:      b.LastModified.Format(time.RFC3339),
		},
	}
}

func HandleBinlogList(folder storage.Folder, since, until string, pretty, json bool) {
	binlogFolder := folder.GetSubFolder(BinlogPath)

	startTime, endTime, err := parseTimeRange(folder, since, until)
	logging.FatalOnError(err)

	logFiles, err := getLogsCoveringInterval(binlogFolder, startTime, true, endTime)
	if err != nil {
		logging.FatalOnError(fmt.Errorf("failed to list binlog files: %w", err))
	}

	if len(logFiles) == 0 {
		if since != "" || until != "" {
			tracelog.InfoLogger.Println("No binlogs found matching the time filter criteria")
		} else {
			tracelog.InfoLogger.Println("No binlogs found in storage")
		}
		return
	}

	var binlogs []printlist.Entity
	for _, file := range logFiles {
		binlogName := strings.TrimSuffix(file.GetName(), filepath.Ext(file.GetName()))
		binlogInfo := BinlogInfo{
			Name:         binlogName,
			Size:         file.GetSize(),
			LastModified: file.GetLastModified(),
		}
		binlogs = append(binlogs, binlogInfo)
	}

	err = printlist.List(binlogs, os.Stdout, pretty, json)
	logging.FatalOnError(err)
}

func parseTimeRange(folder storage.Folder, since, until string) (time.Time, time.Time, error) {
	var startTime, endTime time.Time
	var err error

	if strings.TrimSpace(since) != "" {
		startTime, err = parseTimeFilter(folder, since)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse --since time '%s': %w", since, err)
		}
	}

	if strings.TrimSpace(until) != "" {
		endTime, err = parseTimeFilter(folder, until)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse --until time '%s': %w", until, err)
		}
	} else {
		endTime = time.Now().UTC()
	}

	if !startTime.IsZero() && !endTime.IsZero() && startTime.After(endTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid time range: 'since' (%v) must be before 'until' (%v)",
			startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	}

	return startTime, endTime, nil
}

func parseTimeFilter(folder storage.Folder, timeStr string) (time.Time, error) {
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return time.Time{}, nil
	}

	if strings.ToUpper(timeStr) == internal.LatestString {
		startTS, _, _, err := getTimestamps(folder, internal.LatestString, "", "")
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to get latest backup timestamp: %w", err)
		}
		return startTS, nil
	}

	if duration, err := time.ParseDuration(timeStr); err == nil {
		return time.Now().UTC().Add(-duration), nil
	}

	return utility.ParseUntilTS(timeStr)
}
