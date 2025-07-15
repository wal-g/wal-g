package greenplum

import (
	"os"
	"os/exec"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
)

func TestPrepareContentIDsToFetch(t *testing.T) {
	testcases := []struct {
		fetchContentId    []int
		segmentConfig     []cluster.SegConfig
		contentIDsToFetch map[int]bool
	}{
		{
			fetchContentId:    []int{},
			segmentConfig:     []cluster.SegConfig{},
			contentIDsToFetch: map[int]bool{},
		},
		{
			fetchContentId:    []int{},
			segmentConfig:     []cluster.SegConfig{{ContentID: 21}, {ContentID: 42}},
			contentIDsToFetch: map[int]bool{21: true, 42: true},
		},
		{
			fetchContentId:    []int{1},
			segmentConfig:     []cluster.SegConfig{{ContentID: 1231}, {ContentID: 6743}, {ContentID: 7643}},
			contentIDsToFetch: map[int]bool{1: true},
		},
		{
			fetchContentId:    []int{65, 42, 12, 76, 22},
			segmentConfig:     []cluster.SegConfig{},
			contentIDsToFetch: map[int]bool{65: true, 42: true, 12: true, 76: true, 22: true},
		},
		{
			fetchContentId:    []int{5, 4, 3, 2, 1},
			segmentConfig:     []cluster.SegConfig{{ContentID: 4}, {ContentID: 5}, {ContentID: 6}},
			contentIDsToFetch: map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true},
		},
		{
			fetchContentId:    []int{6, 7, 8, 9, 10},
			segmentConfig:     []cluster.SegConfig{{ContentID: 1}, {ContentID: 5}, {ContentID: 7}},
			contentIDsToFetch: map[int]bool{6: true, 7: true, 8: true, 9: true, 10: true},
		},
	}

	for _, tc := range testcases {
		contentIDsToFetch := prepareContentIDsToFetch(tc.fetchContentId, tc.segmentConfig)
		assert.Equal(t, tc.contentIDsToFetch, contentIDsToFetch)
	}
}

func TestBuildFetchCommand(t *testing.T) {
	beforeValue := config.CfgFile
	defer func() {
		config.CfgFile = beforeValue
	}()
	config.CfgFile = "testConfig"

	testcases := []struct {
		handler   *FetchHandler
		contentID int
		cmdLine   string
	}{
		{
			&FetchHandler{
				cluster:             nil,
				backupIDByContentID: nil,
				backup:              internal.Backup{},
				contentIDsToFetch:   map[int]bool{},
				fetchMode:           "",
				restorePoint:        "",
				partialRestoreArgs:  nil,
			},
			1,
			newSkippedSegmentMsg(1),
		},
		{
			&FetchHandler{
				cluster:             nil,
				backupIDByContentID: nil,
				backup:              internal.Backup{},
				contentIDsToFetch:   map[int]bool{1: false},
				fetchMode:           "",
				restorePoint:        "",
				partialRestoreArgs:  nil,
			},
			1,
			newSkippedSegmentMsg(1),
		},
		{
			&FetchHandler{
				cluster: &cluster.Cluster{
					ContentIDs: nil,
					Hostnames:  nil,
					Segments:   nil,
					ByContent: map[int][]*cluster.SegConfig{
						1: {
							{
								DbID:      1,
								ContentID: 2,
								Role:      "controlled",
								Port:      1234,
								Hostname:  "test.com",
								DataDir:   "/etc/test/",
							},
						},
					},
					ByHost:   nil,
					Executor: nil,
				},
				backupIDByContentID: map[int]string{
					1: "testing",
				},
				backup:             internal.Backup{},
				contentIDsToFetch:  map[int]bool{1: true},
				fetchMode:          "",
				restorePoint:       "",
				partialRestoreArgs: nil,
			},
			1,
			"PGPORT=1234 " +
				"wal-g " +
				"seg-backup-fetch " +
				"/etc/test/ " +
				"--content-id=2 " +
				"--target-user-data=\"{\\\"id\\\":\\\"testing\\\"}\" " +
				"--config=testConfig >> " + formatSegmentLogPath(1) + " 2>&1",
		},
		{
			&FetchHandler{
				cluster: &cluster.Cluster{
					ContentIDs: nil,
					Hostnames:  nil,
					Segments:   nil,
					ByContent: map[int][]*cluster.SegConfig{
						1: {
							{
								DbID:      1,
								ContentID: 2,
								Role:      "controlled",
								Port:      1234,
								Hostname:  "test.com",
								DataDir:   "/etc/test/",
							},
						},
					},
					ByHost:   nil,
					Executor: nil,
				},
				backupIDByContentID: map[int]string{
					1: "other-value-from-testing",
				},
				backup:            internal.Backup{},
				contentIDsToFetch: map[int]bool{1: true},
				fetchMode:         "",
				restorePoint:      "",
				partialRestoreArgs: []string{
					"test1", "test2",
				},
			},
			1,
			"PGPORT=1234 " +
				"wal-g " +
				"seg-backup-fetch " +
				"/etc/test/ " +
				"--content-id=2 " +
				"--target-user-data=\"{\\\"id\\\":\\\"other-value-from-testing\\\"}\" " +
				"--config=testConfig " +
				"--restore-only=test1,test2 " +
				">> " + formatSegmentLogPath(1) + " 2>&1",
		},
	}

	for _, tc := range testcases {
		cmdLine := tc.handler.buildFetchCommand(tc.contentID)
		assert.Equal(t, tc.cmdLine, cmdLine)
	}
}

func TestBuildFetchCommandCrushes(t *testing.T) {
	handler := &FetchHandler{
		cluster: &cluster.Cluster{
			ByContent: map[int][]*cluster.SegConfig{
				1: {
					{
						DbID:      1,
						ContentID: 2,
						Role:      "controlled",
						Port:      1234,
						Hostname:  "test.com",
						DataDir:   "/etc/test/",
					},
				},
			},
		},
		backupIDByContentID: map[int]string{},
		backup:              internal.Backup{},
		contentIDsToFetch:   map[int]bool{1: true},
	}

	if os.Getenv("FROM_TEST_BUILD_FETCH_COMMAND") == "1" {
		handler.buildFetchCommand(1)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestBuildFetchCommandCrushes")
	cmd.Env = append(os.Environ(), "FROM_TEST_BUILD_FETCH_COMMAND=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}
