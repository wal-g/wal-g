package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestApplyBinlogMetadata(t *testing.T) {
	var anotherServerUuid = "00000000-0000-0000-0000-0000000000bb"
	cases := []struct {
		name                 string
		gtidArchivedInitial  string
		uploadedBinlogFile   string
		successorFile        string
		expectedGtidArchived string
	}{
		{
			name:                 "initialize checkpoint",
			gtidArchivedInitial:  "",
			uploadedBinlogFile:   binlog3,
			successorFile:        binlog4,
			expectedGtidArchived: testDataBinlogServerUuid + ":3",
		},
		{
			name:                 "advance checkpoint after upload",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFile:   binlog2,
			successorFile:        binlog3,
			expectedGtidArchived: testDataBinlogServerUuid + ":1-2",
		},
		{
			name:                 "preserve gaps",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFile:   binlog4,
			successorFile:        binlog5,
			expectedGtidArchived: testDataBinlogServerUuid + ":1:4",
		},
		{
			name:                 "preserve transactions from another UUID",
			gtidArchivedInitial:  anotherServerUuid + ":1-5",
			uploadedBinlogFile:   binlog1,
			successorFile:        binlog2,
			expectedGtidArchived: anotherServerUuid + ":1-5," + testDataBinlogServerUuid + ":1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := openPushTestFile(t, filepath.Join("testdata/binlogs", tc.uploadedBinlogFile))
			successor := openPushTestFile(t, filepath.Join("testdata/binlogs", tc.successorFile))
			currentGTID, err := readBinlogFilePreviousGTIDs(file, mysql.MySQLFlavor)
			require.NoError(t, err)
			nextGTID, err := readBinlogFilePreviousGTIDs(successor, mysql.MySQLFlavor)
			require.NoError(t, err)
			metadata := &BinlogMetadata{
				BinlogName:        tc.uploadedBinlogFile,
				PreviousGTIDs:     currentGTID,
				NextPreviousGTIDs: nextGTID,
			}
			sentinel := BinlogSentinelDto{GTIDArchived: tc.gtidArchivedInitial}

			err = updateSentinelMetadata(&sentinel, metadata, mysql.MySQLFlavor)

			require.NoError(t, err)
			require.Equal(t, tc.expectedGtidArchived, sentinel.GTIDArchived)
			require.Equal(t, tc.uploadedBinlogFile, sentinel.LastUploadedBinlog)
		})
	}
}

func openPushTestFile(t *testing.T, filename string) *os.File {
	t.Helper()
	file, err := os.Open(filename)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	return file
}
