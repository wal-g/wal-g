package postgres_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	postgres.HandleDetailedBackupList(folder, true, false)
}

func TestBackupListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name   modified             wal_segment_backup_start\n" +
		"base_1 2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_0 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_2 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	buf := new(bytes.Buffer)
	internal.SortBackupTimeSlices(backups)
	internal.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "+---+--------+-----------------------------------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| # | NAME   | MODIFIED                          | WAL SEGMENT BACKUP START | START TIME | FINISH TIME | HOSTNAME | DATADIR | PG VERSION | START LSN | FINISH LSN | PERMANENT |\n" +
		"+---+--------+-----------------------------------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| 0 | base_1 | Sunday, 01-Jan-17 01:01:01 UTC    | ZZZZZZZZZZZZZZZZZZZZZZZZ | -          | -           |          |         |          0 |       0/0 |        0/0 | false     |\n" +
		"| 1 | base_0 | Monday, 01-Jan-18 01:01:01 UTC    | ZZZZZZZZZZZZZZZZZZZZZZZZ | -          | -           |          |         |          0 |       0/0 |        0/0 | false     |\n" +
		"| 2 | base_2 | Wednesday, 01-Jan-20 01:01:01 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ | -          | -           |          |         |          0 |       0/0 |        0/0 | false     |\n" +
		"+---+--------+-----------------------------------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)
	buf := new(bytes.Buffer)
	postgres.WritePrettyBackupList(details, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingCreationTimeGaps(t *testing.T) {
	const expected = "name   modified             wal_segment_backup_start start_time           finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"base_1 2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ -                    -                             0          0/0       0/0        false\n" +
		"base_0 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ -                    -                             0          0/0       0/0        false\n" +
		"base_2 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1998-01-01T01:01:01Z -                             0          0/0       0/0        false\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.CreationTimeGaps)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(details, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingModificationTimeGaps(t *testing.T) {
	const expected = "name   modified             wal_segment_backup_start start_time           finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"base_0 -                    ZZZZZZZZZZZZZZZZZZZZZZZZ 1997-01-01T01:01:01Z -                             0          0/0       0/0        false\n" +
		"base_2 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1998-01-01T01:01:01Z -                             0          0/0       0/0        false\n" +
		"base_1 -                    ZZZZZZZZZZZZZZZZZZZZZZZZ 1999-01-01T01:01:01Z -                             0          0/0       0/0        false\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.ModificationTimeGaps)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(details, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingNoTimeGaps(t *testing.T) {
	const expected = "name   modified             wal_segment_backup_start start_time           finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"base_0 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1997-01-01T01:01:01Z -                             0          0/0       0/0        false\n" +
		"base_2 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1998-01-01T01:01:01Z -                             0          0/0       0/0        false\n" +
		"base_1 2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1999-01-01T01:01:01Z -                             0          0/0       0/0        false\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoTimeGaps)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(details, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingTimeGaps(t *testing.T) {
	const expected = "name   modified             wal_segment_backup_start start_time           finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"base_2 -                    ZZZZZZZZZZZZZZZZZZZZZZZZ 1998-01-01T01:01:01Z -                             0          0/0       0/0        false\n" +
		"base_1 2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ -                    -                             0          0/0       0/0        false\n" +
		"base_0 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ 1997-01-01T01:01:01Z -                             0          0/0       0/0        false\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.CreationAndModificationTimeGaps)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(details, os.Stdout)
	postgres.WriteBackupList(details, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectJsonOutput(t *testing.T) {
	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)

	var actual []postgres.BackupDetail
	buf := new(bytes.Buffer)

	err = internal.WriteAsJSON(details, buf, false)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, details)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"base_1\",\n" +
		"        \"time\": \"2017-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\",\n" +
		"        \"storage_name\": \"default\",\n" +
		"        \"start_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"finish_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"date_fmt\": \"\",\n" +
		"        \"hostname\": \"\",\n" +
		"        \"data_dir\": \"\",\n" +
		"        \"pg_version\": 0,\n" +
		"        \"start_lsn\": 0,\n" +
		"        \"finish_lsn\": 0,\n" +
		"        \"is_permanent\": false,\n" +
		"        \"system_identifier\": null,\n" +
		"        \"uncompressed_size\": 0,\n" +
		"        \"compressed_size\": 0\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_0\",\n" +
		"        \"time\": \"2018-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\",\n" +
		"        \"storage_name\": \"default\",\n" +
		"        \"start_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"finish_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"date_fmt\": \"\",\n" +
		"        \"hostname\": \"\",\n" +
		"        \"data_dir\": \"\",\n" +
		"        \"pg_version\": 0,\n" +
		"        \"start_lsn\": 0,\n" +
		"        \"finish_lsn\": 0,\n" +
		"        \"is_permanent\": false,\n" +
		"        \"system_identifier\": null,\n" +
		"        \"uncompressed_size\": 0,\n" +
		"        \"compressed_size\": 0\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_2\",\n" +
		"        \"time\": \"2020-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\",\n" +
		"        \"storage_name\": \"default\",\n" +
		"        \"start_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"finish_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"date_fmt\": \"\",\n" +
		"        \"hostname\": \"\",\n" +
		"        \"data_dir\": \"\",\n" +
		"        \"pg_version\": 0,\n" +
		"        \"start_lsn\": 0,\n" +
		"        \"finish_lsn\": 0,\n" +
		"        \"is_permanent\": false,\n" +
		"        \"system_identifier\": null,\n" +
		"        \"uncompressed_size\": 0,\n" +
		"        \"compressed_size\": 0\n" +
		"    }\n" +
		"]"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	var unmarshaledDetails []postgres.BackupDetail
	backups, err := internal.GetBackups(folder)
	assert.NoError(t, err)
	details, err := postgres.GetBackupsDetails(folder, backups)
	assert.NoError(t, err)
	postgres.SortBackupDetails(details)
	buf := new(bytes.Buffer)

	err = internal.WriteAsJSON(details, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshaledDetails)

	assert.NoError(t, err)
	assert.Equal(t, unmarshaledDetails, details)
	assert.Equal(t, buf.String(), expectedString)
}
