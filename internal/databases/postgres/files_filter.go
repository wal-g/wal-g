package postgres

import (
	"errors"
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

var filesToExclude = []string{
	"log", "pg_log", "pg_xlog", "pg_wal", // Directories
	"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf", // Files
	"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	"standby.signal", // Signal files
}

type PgFilesFilterType int

const (
	RegularPgFileFilter PgFilesFilterType = iota + 1
	CatchupPgFilesFilter
)

func SelectPgFilesFilter(ffType PgFilesFilterType, pgVersion int) (internal.FilesFilter, error) {
	switch ffType {
	case RegularPgFileFilter:
		return NewRegularPgFilesFilter(pgVersion), nil
	case CatchupPgFilesFilter:
		return NewCatchupPgFilesFilter(), nil
	default:
		return nil, errors.New(fmt.Sprintf("PgFilesFilterFactory: Unknown PgFilesFilterType: %v", ffType))
	}
}

type RegularPgFilesFilter struct {
	excludedFilenames map[string]utility.Empty
}

var _ internal.FilesFilter = &RegularPgFilesFilter{}

func NewRegularPgFilesFilter(version int) internal.FilesFilter {
	var excludedFilenames = make(map[string]utility.Empty)
	for _, filename := range filesToExclude {
		excludedFilenames[filename] = utility.Empty{}
	}
	// extend common file filters:
	if version > 90600 {
		// for Pg 9.6+ we don't rely on `backup_label` and `tablespace_map` from datadir because we create
		// fictional files from `pg_stop_backup()`. Don't archive them - so we won't have data race during recovery.
		excludedFilenames[BackupLabelFilename] = utility.Empty{}
		excludedFilenames[TablespaceMapFilename] = utility.Empty{}
	}

	return &RegularPgFilesFilter{
		excludedFilenames: excludedFilenames,
	}
}

func NewCatchupPgFilesFilter() internal.FilesFilter {
	var excludedFilenames = make(map[string]utility.Empty)

	for _, filename := range filesToExclude {
		excludedFilenames[filename] = utility.Empty{}
	}
	// extend common file filters:
	for _, fname := range []string{"pg_hba.conf", "postgresql.conf", "postgresql.auto.conf"} {
		excludedFilenames[fname] = utility.Empty{}
	}

	return &RegularPgFilesFilter{
		excludedFilenames: excludedFilenames,
	}
}

func (ff *RegularPgFilesFilter) ShouldUploadFile(path string) bool {
	_, ok := ff.excludedFilenames[path]
	return !ok
}
