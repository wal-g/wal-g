package greenplum_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := CreateMockStorageFolder(t)
	backups, err := greenplum.ListStorageBackups(folder)

	assert.NoError(t, err)
	assert.True(t, len(backups) > 0)
}

func TestBackupListCorrectDetailedJsonOutput(t *testing.T) {
	folder := CreateMockStorageFolder(t)

	backups, err := greenplum.ListStorageBackups(folder)
	assert.NoError(t, err)

	details := greenplum.MakeBackupDetails(backups)

	var actual []greenplum.BackupDetail
	buf := new(bytes.Buffer)
	err = internal.WriteAsJSON(details, buf, false)
	assert.NoError(t, err)

	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, details, actual)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = `[
    {
        "Name": "backup_20221212T151258Z",
        "restore_point": "backup_20221212T151258Z",
        "user_data": {
            "backup_id": "some_id1"
        },
        "start_time": "2022-12-12T12:12:58.287495Z",
        "finish_time": "2022-12-12T12:18:58.826198Z",
        "date_fmt": "%Y-%m-%dT%H:%M:%S.%fZ",
        "hostname": "some.host.name",
        "gp_version": "6.19.3",
        "is_permanent": false,
        "uncompressed_size": 2139586909,
        "compressed_size": 91217782,
        "data_catalog_size": 20161814071
    },
    {
        "Name": "backup_20221213T011727Z_D_20221212T151258Z",
        "restore_point": "backup_20221213T011727Z_D_20221212T151258Z",
        "user_data": {
            "backup_id": "some_id2"
        },
        "start_time": "2022-12-12T22:17:27.196163Z",
        "finish_time": "2022-12-12T22:18:27.803675Z",
        "date_fmt": "%Y-%m-%dT%H:%M:%S.%fZ",
        "hostname": "some.host.name",
        "gp_version": "6.19.3",
        "is_permanent": false,
        "uncompressed_size": 36283663,
        "compressed_size": 2532570,
        "data_catalog_size": 20161790703,
        "increment_from": "backup_20221212T151258Z",
        "increment_full_name": "backup_20221212T151258Z",
        "increment_count": 1
    }
]`

	folder := CreateMockStorageFolder(t)

	backups, err := greenplum.ListStorageBackups(folder)
	assert.NoError(t, err)

	details := greenplum.MakeBackupDetails(backups)

	buf := new(bytes.Buffer)
	err = internal.WriteAsJSON(details, buf, true)
	assert.NoError(t, err)
	assert.Equal(t, expectedString, buf.String())

	var unmarshaledDetails []greenplum.BackupDetail
	err = json.Unmarshal(buf.Bytes(), &unmarshaledDetails)
	assert.NoError(t, err)
	assert.Equal(t, details, unmarshaledDetails)
}

func CreateMockStorageFolder(t *testing.T) storage.Folder {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backupNames := map[string]interface{}{
		"backup_20221212T151258Z": map[string]interface{}{
			"restore_point": "backup_20221212T151258Z",
			"user_data": map[string]interface{}{
				"backup_id": "some_id1",
			},
			"segments": []map[string]interface{}{
				{"db_id": 1, "content_id": -1, "role": "p", "port": 5432, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/master/gpseg-1", "backup_id": "53230431-14e0-4d51-9d66-245a48acad7d", "backup_name": "base_000000020000000600000011", "restore_point_lsn": "6/48024E00"},
				{"db_id": 4, "content_id": 0, "role": "p", "port": 6000, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/primary/gpseg0", "backup_id": "baca3358-09c2-4d4b-8ed2-2286c8dbbfbc", "backup_name": "base_00000002000000080000002D", "restore_point_lsn": "8/B816EBA0"},
				{"db_id": 5, "content_id": 1, "role": "p", "port": 7000, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/mirror/gpseg1", "backup_id": "40dddd45-63f3-426d-a40f-6096a7519c0b", "backup_name": "base_00000004000000080000002E", "restore_point_lsn": "8/BC02FEE0"},
			},
			"start_time":        "2022-12-12T12:12:58.287495Z",
			"finish_time":       "2022-12-12T12:18:58.826198Z",
			"date_fmt":          "%Y-%m-%dT%H:%M:%S.%fZ",
			"hostname":          "some.host.name",
			"gp_version":        "6.19.3",
			"is_permanent":      false,
			"uncompressed_size": 2139586909,
			"compressed_size":   91217782,
			"data_catalog_size": 20161814071,
		},
		"backup_20221213T011727Z_D_20221212T151258Z": map[string]interface{}{
			"restore_point": "backup_20221213T011727Z_D_20221212T151258Z",
			"user_data": map[string]interface{}{
				"backup_id": "some_id2",
			},
			"segments": []map[string]interface{}{
				{"db_id": 1, "content_id": -1, "role": "p", "port": 5432, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/master/gpseg-1", "backup_id": "seg_backup_id1", "backup_name": "base_000000020000000600000011", "restore_point_lsn": "6/48024E00"},
				{"db_id": 4, "content_id": 0, "role": "p", "port": 6000, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/primary/gpseg0", "backup_id": "seg_backup_id2", "backup_name": "base_00000002000000080000002D", "restore_point_lsn": "8/B816EBA0"},
				{"db_id": 5, "content_id": 1, "role": "p", "port": 7000, "hostname": "host.name", "data_dir": "/var/lib/greenplum/data1/mirror/gpseg1", "backup_id": "seg_backup_id3", "backup_name": "base_00000004000000080000002E", "restore_point_lsn": "8/BC02FEE0"},
			},
			"start_time":          "2022-12-12T22:17:27.196163Z",
			"finish_time":         "2022-12-12T22:18:27.803675Z",
			"date_fmt":            "%Y-%m-%dT%H:%M:%S.%fZ",
			"hostname":            "some.host.name",
			"gp_version":          "6.19.3",
			"is_permanent":        false,
			"uncompressed_size":   36283663,
			"compressed_size":     2532570,
			"data_catalog_size":   20161790703,
			"increment_from":      "backup_20221212T151258Z",
			"increment_full_name": "backup_20221212T151258Z",
			"increment_count":     1,
		},
	}

	restorePoints := map[string]interface{}{
		"point1_restore_point.json": map[string]interface{}{
			"name":        "point1",
			"start_time":  "2022-12-13T09:00:01.596568Z",
			"finish_time": "2022-12-13T09:00:01.710603Z",
			"hostname":    "some.host.name",
			"gp_version":  "6.19.3",
			"lsn_by_segment": map[string]interface{}{
				"0":  "A/B00C8318",
				"1":  "A/B00C3300",
				"-1": "8/4002D548",
			},
		},
	}

	for backupName := range backupNames {
		bytesMetadata, err := json.Marshal(backupNames[backupName])
		assert.NoError(t, err)
		metadataString := string(bytesMetadata)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(metadataString))
		assert.NoError(t, err)
	}

	for pointName := range restorePoints {
		bytesMetadata, err := json.Marshal(restorePoints[pointName])
		assert.NoError(t, err)
		metadataString := string(bytesMetadata)
		err = baseBackupFolder.PutObject(pointName+greenplum.RestorePointSuffix, strings.NewReader(metadataString))
		assert.NoError(t, err)
	}
	return folder
}
