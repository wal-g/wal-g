package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var walSummaryName = regexp.MustCompile(`^([0-9A-F]{8})([0-9A-F]{16})([0-9A-F]{16})\.summary$`)

type walSummaryFile struct {
	name       string
	timeline   uint32
	start, end LSN
}

func parseWalSummaryName(name string) (walSummaryFile, error) {
	match := walSummaryName.FindStringSubmatch(name)
	if match == nil {
		return walSummaryFile{}, fmt.Errorf("invalid WAL summary name %q", name)
	}
	timeline, _ := strconv.ParseUint(match[1], 16, 32)
	start, _ := strconv.ParseUint(match[2], 16, 64)
	end, _ := strconv.ParseUint(match[3], 16, 64)
	if timeline == 0 || end <= start {
		return walSummaryFile{}, fmt.Errorf("invalid WAL summary range %q", name)
	}
	return walSummaryFile{name, uint32(timeline), LSN(start), LSN(end)}, nil
}

// HandleWalSummaryPush archives one immutable, published .summary file. It does
// no directory discovery and can be called independently of WAL archival.
func HandleWalSummaryPush(ctx context.Context, uploader internal.Uploader, filename string) error {
	if _, err := parseWalSummaryName(filepath.Base(filename)); err != nil {
		return err
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "close WAL summary")
	uploader = uploader.Clone()
	uploader.ChangeDirectory(utility.WalSummaryPath)
	return uploader.UploadFile(ctx, file)
}

func selectWalSummaries(files []walSummaryFile, from, to LSN) ([]walSummaryFile, error) {
	sort.Slice(files, func(i, j int) bool { return files[i].start < files[j].start })
	var selected []walSummaryFile
	covered := from
	for _, file := range files {
		if covered >= to {
			break
		}
		if file.end <= covered {
			continue
		}
		if file.start > covered {
			break
		}
		selected = append(selected, file)
		covered = file.end
	}
	if covered < to {
		return nil, fmt.Errorf("WAL summary gap at %s (required %s through %s)", covered, from, to)
	}
	return selected, nil
}

func (bh *BackupHandler) loadWalSummaryDeltaMap(ctx context.Context, root storage.Folder) error {
	if bh.PgInfo.PgVersion < 180000 || bh.Workers.Bundle.Replica {
		return fmt.Errorf("WAL summary PoC requires a PostgreSQL 18+ primary")
	}
	if viper.GetBool(conf.UseWalDeltaSetting) {
		return fmt.Errorf("WALG_USE_WAL_SUMMARY and WALG_USE_WAL_DELTA are mutually exclusive")
	}
	previousTimeline, err := ParseTimelineFromBackupName(bh.prevBackupInfo.name)
	if err != nil {
		return err
	}
	if previousTimeline != bh.Workers.Bundle.Timeline {
		return fmt.Errorf("WAL summary PoC requires backups on the same timeline")
	}
	if err := bh.waitForWalSummary(ctx); err != nil {
		return err
	}
	files, archived, err := bh.listWalSummaries(ctx, root)
	if err != nil {
		return err
	}
	files, err = selectWalSummaries(files, *bh.Workers.Bundle.IncrementFromLsn, bh.CurBackupInfo.startLSN)
	if err != nil {
		return err
	}
	uploader, err := internal.ConfigureUploaderToFolder(root)
	if err != nil {
		return err
	}
	delta := NewPagedFileDeltaMap()
	reader := internal.NewFolderReader(root.GetSubFolder(utility.WalSummaryPath))
	for _, file := range files {
		if !archived[file.name] {
			filename := filepath.Join(bh.PgInfo.PgDataDirectory, "pg_wal", "summaries", file.name)
			if err := HandleWalSummaryPush(ctx, uploader, filename); err != nil {
				return err
			}
		}
		in, err := internal.DownloadAndDecompressStorageFile(ctx, reader, file.name)
		if err != nil {
			return err
		}
		err = readWalSummary(in, delta)
		err = errors.Join(err, in.Close())
		if err != nil {
			return fmt.Errorf("read WAL summary %s: %w", file.name, err)
		}
	}
	// Publish only a fully covered, checksum-verified map. No page-scan fallback.
	bh.Workers.Bundle.DeltaMap = delta
	bh.Workers.Bundle.UseWalSummary = true
	tracelog.InfoLogger.Printf("Using WAL summaries: %d files, %d relations", len(files), len(delta))
	return nil
}

func (bh *BackupHandler) waitForWalSummary(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	runner := bh.Workers.QueryRunner
	enabled, err := runner.GetParameter(ctx, "summarize_wal")
	if err != nil {
		return err
	}
	if enabled != "on" {
		return fmt.Errorf("WAL summary PoC requires summarize_wal=on")
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		runner.Mu.Lock()
		err := runner.Connection.QueryRow(ctx, `SELECT summarized_tli = $1 AND summarized_lsn >= $2::pg_lsn
			FROM pg_catalog.pg_get_wal_summarizer_state()`, int64(bh.Workers.Bundle.Timeline),
			bh.CurBackupInfo.startLSN.String()).Scan(&ready)
		runner.Mu.Unlock()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for WAL summaries: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (bh *BackupHandler) listWalSummaries(ctx context.Context, root storage.Folder) ([]walSummaryFile, map[string]bool, error) {
	// One flat LIST of wal_sum_005/ and one server-side summary directory scan
	// per incremental backup (typically hundreds/thousands of retained summaries).
	// Polling uses summarizer shared state, never directory or archive listings.
	objects, _, err := root.GetSubFolder(utility.WalSummaryPath).ListFolder(ctx)
	if err != nil {
		return nil, nil, err
	}
	archived := make(map[string]bool)
	files := make(map[string]walSummaryFile)
	add := func(name string, remote bool) {
		file, err := parseWalSummaryName(name)
		if err == nil && file.timeline == bh.Workers.Bundle.Timeline &&
			file.end > *bh.Workers.Bundle.IncrementFromLsn && file.start < bh.CurBackupInfo.startLSN {
			files[name] = file
			if remote {
				archived[name] = true
			}
		}
	}
	for _, object := range objects {
		stem, _, ok := strings.Cut(object.GetName(), ".summary")
		if ok {
			add(stem+".summary", true)
		}
	}
	runner := bh.Workers.QueryRunner
	runner.Mu.Lock()
	defer runner.Mu.Unlock()
	rows, err := runner.Connection.Query(ctx, `SELECT tli, start_lsn::text, end_lsn::text
		FROM pg_catalog.pg_available_wal_summaries() WHERE tli = $1
		AND end_lsn > $2::pg_lsn AND start_lsn < $3::pg_lsn`, int64(bh.Workers.Bundle.Timeline),
		bh.Workers.Bundle.IncrementFromLsn.String(), bh.CurBackupInfo.startLSN.String())
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var timeline uint32
		var start, end string
		if err := rows.Scan(&timeline, &start, &end); err != nil {
			return nil, nil, err
		}
		first, err := ParseLSN(start)
		if err != nil {
			return nil, nil, err
		}
		last, err := ParseLSN(end)
		if err != nil {
			return nil, nil, err
		}
		add(fmt.Sprintf("%08X%016X%016X.summary", timeline, uint64(first), uint64(last)), false)
	}
	result := make([]walSummaryFile, 0, len(files))
	for _, file := range files {
		result = append(result, file)
	}
	return result, archived, rows.Err()
}
