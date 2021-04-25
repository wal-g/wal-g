package postgres_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	postgres.HandleBackupListWithFlags(folder, true, false, false)
}

func TestBackupListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name   created modified             wal_segment_backup_start\n" +
		"base_2 -       2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_0 -       2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_1 -       2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)
	buf := new(bytes.Buffer)
	postgres.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+--------+---------+-----------------------------------+--------------------------+\n" +
		"| # | NAME   | CREATED | MODIFIED                          | WAL SEGMENT BACKUP START |\n" +
		"+---+--------+---------+-----------------------------------+--------------------------+\n" +
		"| 0 | base_2 | -       | Wednesday, 01-Jan-20 01:01:01 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_0 | -       | Monday, 01-Jan-18 01:01:01 UTC    | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 2 | base_1 | -       | Sunday, 01-Jan-17 01:01:01 UTC    | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+--------+---------+-----------------------------------+--------------------------+\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)
	buf := new(bytes.Buffer)
	postgres.WritePrettyBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingCreationTimeGaps(t *testing.T) {
	const expected = "" +
		"name   created              modified             wal_segment_backup_start\n" +
		"base_2 1998-01-01T01:01:01Z 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_0 -                    2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_1 -                    2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.CreationTimeGaps)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingModificationTimeGaps(t *testing.T) {
	const expected = "" +
		"name   created              modified             wal_segment_backup_start\n" +
		"base_1 1999-01-01T01:01:01Z -                    ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_2 1998-01-01T01:01:01Z 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_0 1997-01-01T01:01:01Z -                    ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.ModificationTimeGaps)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingNoTimeGaps(t *testing.T) {
	const expected = "" +
		"name   created              modified             wal_segment_backup_start\n" +
		"base_1 1999-01-01T01:01:01Z 2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_2 1998-01-01T01:01:01Z 2020-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_0 1997-01-01T01:01:01Z 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoTimeGaps)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectOrderingTimeGaps(t *testing.T) {
	const expected = "" +
		"name   created              modified             wal_segment_backup_start\n" +
		"base_0 1997-01-01T01:01:01Z 2018-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_1 -                    2017-01-01T01:01:01Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_2 1998-01-01T01:01:01Z -                    ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.CreationAndModificationTimeGaps)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	postgres.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}


func TestBackupListCorrectJsonOutput(t *testing.T) {
	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)

	var actual []postgres.BackupTime
	buf := new(bytes.Buffer)

	err = postgres.WriteAsJSON(backups, buf, false)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, backups)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"base_2\",\n" +
		"        \"creation_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"modification_time\": \"2020-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_0\",\n" +
		"        \"creation_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"modification_time\": \"2018-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_1\",\n" +
		"        \"creation_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"modification_time\": \"2017-01-01T01:01:01.000000001Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    }\n" +
		"]"

	folder := testtools.CreatePostgresMockStorageFolderWithTimeMetadata(t, testtools.NoCreationTime)
	var unmarshaledBackups []postgres.BackupTime
	backups, err := postgres.GetBackups(folder)
	assert.NoError(t, err)
	buf := new(bytes.Buffer)

	err = postgres.WriteAsJSON(backups, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshaledBackups)

	assert.NoError(t, err)
	assert.Equal(t, unmarshaledBackups, backups)
	assert.Equal(t, buf.String(), expectedString)
}
