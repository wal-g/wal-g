package walg_test

import (
	"testing"

	"github.com/wal-g/wal-g"
)

// Tests building start backup query
func TestBuildStartBackup(t *testing.T) {
	queryBuilder := &walg.PgQueryRunner{Version: 0}
	_, err := queryBuilder.BuildStartBackup()
	if err == nil {
		t.Error("BuildStartBackup did not error on version 0")
	}

	queryBuilder.Version = 81000
	_, err = queryBuilder.BuildStartBackup()
	if err.Error() != "Could not determine start backup query for version 81000" {
		t.Errorf("Incorrect error for BuildStartBackup with version 81000, got error %s", err)
	}

	queryBuilder.Version = 90321
	queryString, err := queryBuilder.BuildStartBackup()
	if queryString != "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true) lsn" {
		t.Errorf("Got wrong query string for BuildStartBackup with version 90321, got %s", queryString)
	}

	queryBuilder.Version = 90600
	queryString, err = queryBuilder.BuildStartBackup()
	if queryString != "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn" {
		t.Errorf("Got wrong query string for BuildStartBackup with version 90600, got %s", queryString)
	}

	queryBuilder.Version = 100000
	queryString, err = queryBuilder.BuildStartBackup()
	if queryString != "SELECT case when pg_is_in_recovery() then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn" {
		t.Errorf("Got wrong query string for BuildStartBackup with version 100000, got %s", queryString)
	}
}

// Tests building stop backup query
func TestBuildStopBackup(t *testing.T) {
	queryBuilder := &walg.PgQueryRunner{Version: 0}
	_, err := queryBuilder.BuildStopBackup()
	if err == nil {
		t.Error("BuildStopBackup did not error on version 0")
	}

	queryBuilder.Version = 81000
	_, err = queryBuilder.BuildStopBackup()
	if err.Error() != "Could not determine stop backup query for version 81000" {
		t.Errorf("Incorrect error for BuildStopBackup with version 81000, got error %s", err)
	}

	queryBuilder.Version = 90321
	queryString, err := queryBuilder.BuildStopBackup()
	if queryString != "SELECT (pg_xlogfile_name_offset(lsn)).file_name, lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text FROM pg_stop_backup() lsn" {
		t.Errorf("Got wrong query string for BuildStopBackup with version 90321, got %s", queryString)
	}

	queryBuilder.Version = 90600
	queryString, err = queryBuilder.BuildStopBackup()
	if queryString != "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)" {
		t.Errorf("Got wrong query string for BuildStopBackup with version 90600, got %s", queryString)
	}

	queryBuilder.Version = 100000
	queryString, err = queryBuilder.BuildStopBackup()
	if queryString != "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)" {
		t.Errorf("Got wrong query string for BuildStopBackup with version 100000, got %s", queryString)
	}
}
