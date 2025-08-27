package mysql

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type BinlogInfo struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size,omitempty"`
	LastModified time.Time `json:"last_modified,omitempty"`
}

type TimeFilter struct {
	Since time.Time
	Until time.Time
}

func (tf *TimeFilter) IsValid() bool {
	if tf.Since.IsZero() || tf.Until.IsZero() {
		return true
	}
	return tf.Since.Before(tf.Until) || tf.Since.Equal(tf.Until)
}

func (tf *TimeFilter) Matches(t time.Time) bool {
	if !tf.Since.IsZero() && t.Before(tf.Since) {
		return false
	}
	if !tf.Until.IsZero() && t.After(tf.Until) {
		return false
	}
	return true
}

func HandleBinlogList(folder storage.Folder, since, until string) {
	binlogFolder := folder.GetSubFolder(BinlogPath)

	binlogFiles, _, err := binlogFolder.ListFolder()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to list binlog folder: %v", err)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	if len(binlogFiles) == 0 {
		tracelog.InfoLogger.Println("No binlogs found in storage")
		return
	}

	timeFilter, err := parseTimeFilters(since, until)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to parse time filters: %v", err)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	if !timeFilter.IsValid() {
		tracelog.ErrorLogger.Printf("Invalid time range: 'since' (%v) must be before 'until' (%v)",
			timeFilter.Since.Format(time.RFC3339),
			timeFilter.Until.Format(time.RFC3339))
		tracelog.ErrorLogger.FatalOnError(fmt.Errorf("invalid time range"))
	}

	var binlogs []BinlogInfo
	for _, file := range binlogFiles {
		lastModified := file.GetLastModified()

		if !timeFilter.Matches(lastModified) {
			continue
		}

		binlogName := strings.TrimSuffix(file.GetName(), filepath.Ext(file.GetName()))
		binlogInfo := BinlogInfo{
			Name:         binlogName,
			Size:         file.GetSize(),
			LastModified: lastModified,
		}

		binlogs = append(binlogs, binlogInfo)
	}

	if len(binlogs) == 0 {
		if since != "" || until != "" {
			tracelog.InfoLogger.Println("No binlogs found matching the time filter criteria")
		} else {
			tracelog.InfoLogger.Println("No binlogs found in storage")
		}
		return
	}

	sort.Slice(binlogs, func(i, j int) bool {
		return binlogs[i].LastModified.Before(binlogs[j].LastModified)
	})

	printBinlogsPlain(binlogs)
}

func parseTimeFilters(since, until string) (*TimeFilter, error) {
	filter := &TimeFilter{}
	var err error

	if strings.TrimSpace(since) != "" {
		filter.Since, err = parseTimeFilter(since)
		if err != nil {
			return nil, fmt.Errorf("failed to parse --since time '%s': %w", since, err)
		}
	}

	if strings.TrimSpace(until) != "" {
		filter.Until, err = parseTimeFilter(until)
		if err != nil {
			return nil, fmt.Errorf("failed to parse --until time '%s': %w", until, err)
		}
	}

	return filter, nil
}

func parseTimeFilter(timeStr string) (time.Time, error) {
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return time.Time{}, nil
	}

	if duration, err := time.ParseDuration(timeStr); err == nil {
		return time.Now().UTC().Add(-duration), nil
	}

	return utility.ParseUntilTS(timeStr)
}

func printBinlogsPlain(binlogs []BinlogInfo) {
	fmt.Printf("%-30s %10s %s\n", "NAME", "SIZE", "MODIFIED")
	for _, binlog := range binlogs {
		fmt.Printf("%-30s %10d %s\n",
			binlog.Name,
			binlog.Size,
			binlog.LastModified.Format(time.RFC3339))
	}
}
