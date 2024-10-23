package postgres

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

var filesToExclude = []string{
	"log", "pg_log", "pg_xlog", "pg_wal", // Directories
	"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf", // Files
	"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	"standby.signal", // Signal files
}

type PgFilesFilter struct {
	excludedFilenames map[string]utility.Empty
}

var _ internal.FilesFilter = &PgFilesFilter{}

func NewPgFilesFilter(version int) internal.FilesFilter {
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

	return &PgFilesFilter{
		excludedFilenames: excludedFilenames,
	}
}

func NewPgCatchupFilesFilter() internal.FilesFilter {
	var excludedFilenames = make(map[string]utility.Empty)

	for _, filename := range filesToExclude {
		excludedFilenames[filename] = utility.Empty{}
	}
	// extend common file filters:
	for _, fname := range []string{"pg_hba.conf", "postgresql.conf", "postgresql.auto.conf"} {
		excludedFilenames[fname] = utility.Empty{}
	}

	return &PgFilesFilter{
		excludedFilenames: excludedFilenames,
	}
}

func (ff *PgFilesFilter) ShouldUploadFile(path string) bool {
	_, ok := ff.excludedFilenames[path]
	return !ok
}
