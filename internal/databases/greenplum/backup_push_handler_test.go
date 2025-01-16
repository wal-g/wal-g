package greenplum

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/wal-g/internal/config"
)

func TestBuildBackupPushCommand(t *testing.T) {
	beforeValue := config.CfgFile
	defer func() {
		config.CfgFile = beforeValue
	}()
	config.CfgFile = "testConfig"

	currentTime := time.Unix(1_690_000_000, 0)

	testcases := []struct {
		handler          *BackupHandler
		contentID        int
		cmdLineBeginning string
		cmdLineEnd       string
	}{
		{
			&BackupHandler{
				arguments: BackupArguments{
					Uploader: nil,
					segmentFwdArgs: []SegmentFwdArg{
						{
							Name:  "param1",
							Value: "argument1",
						},
						{
							Name:  "param2",
							Value: "argument2",
						},
					},
					logsDir: "test_no_use",
				},
				workers: BackupWorkers{},
				globalCluster: &cluster.Cluster{
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
				currBackupInfo: CurrBackupInfo{
					backupName:           "test_curr",
					segmentBackups:       make(map[string]*cluster.SegConfig),
					startTime:            currentTime.UTC(),
					finishTime:           currentTime.Add(10 * time.Second).UTC(),
					systemIdentifier:     nil,
					gpVersion:            Version{},
					segmentsMetadata:     nil,
					backupPidByContentID: nil,
					incrementCount:       0,
				},
				prevBackupInfo: PrevBackupInfo{
					name:        "test_prev",
					sentinelDto: BackupSentinelDto{},
					deltaBaseBackupIDs: map[int]string{
						1: "00000000-0000-0000-0000-000000000000",
					},
				},
			},
			1,
			"nohup " + "wal-g seg-cmd-run " +
				"seg-backup-push " +
				"--content-id=2 " +
				"'/etc/test/ --add-user-data=",
			" --pgport=1234 " +
				"--delta-from-user-data=" + NewSegmentUserDataFromID("00000000-0000-0000-0000-000000000000").String() +
				" --param1=argument1 --param2=argument2' " +
				"--config=testConfig " +
				"&>> " + formatSegmentLogPath(1) + " " +
				"& echo $!",
		},
		{
			&BackupHandler{
				arguments: BackupArguments{
					Uploader:       nil,
					segmentFwdArgs: make([]SegmentFwdArg, 0),
					logsDir:        "test_no_use",
				},
				workers: BackupWorkers{},
				globalCluster: &cluster.Cluster{
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
				currBackupInfo: CurrBackupInfo{
					backupName:           "test_curr",
					segmentBackups:       make(map[string]*cluster.SegConfig),
					startTime:            currentTime.UTC(),
					finishTime:           currentTime.Add(10 * time.Second).UTC(),
					systemIdentifier:     nil,
					gpVersion:            Version{},
					segmentsMetadata:     nil,
					backupPidByContentID: nil,
					incrementCount:       0,
				},
				prevBackupInfo: PrevBackupInfo{
					name:        "test_prev",
					sentinelDto: BackupSentinelDto{},
					deltaBaseBackupIDs: map[int]string{
						1: "00000000-0000-0000-0000-000000000000",
					},
				},
			},
			1,
			"nohup " + "wal-g seg-cmd-run " +
				"seg-backup-push " +
				"--content-id=2 " +
				"'/etc/test/ --add-user-data=",
			" --pgport=1234 " +
				"--delta-from-user-data=" + NewSegmentUserDataFromID("00000000-0000-0000-0000-000000000000").String() +
				"' " +
				"--config=testConfig " +
				"&>> " + formatSegmentLogPath(1) + " " +
				"& echo $!",
		},
		{
			&BackupHandler{
				arguments: BackupArguments{
					Uploader:       nil,
					segmentFwdArgs: make([]SegmentFwdArg, 0),
					logsDir:        "test_no_use",
				},
				workers: BackupWorkers{},
				globalCluster: &cluster.Cluster{
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
				currBackupInfo: CurrBackupInfo{
					backupName:           "test_curr",
					segmentBackups:       make(map[string]*cluster.SegConfig),
					startTime:            currentTime.UTC(),
					finishTime:           currentTime.Add(10 * time.Second).UTC(),
					systemIdentifier:     nil,
					gpVersion:            Version{},
					segmentsMetadata:     nil,
					backupPidByContentID: nil,
					incrementCount:       0,
				},
				prevBackupInfo: PrevBackupInfo{},
			},
			1,
			"nohup " + "wal-g seg-cmd-run " +
				"seg-backup-push " +
				"--content-id=2 " +
				"'/etc/test/ --add-user-data=",
			" --pgport=1234" +
				"' " +
				"--config=testConfig " +
				"&>> " + formatSegmentLogPath(1) + " " +
				"& echo $!",
		},
	}

	for _, tc := range testcases {
		cmdLine := tc.handler.buildBackupPushCommand(tc.contentID)

		if !strings.HasPrefix(cmdLine, tc.cmdLineBeginning) ||
			!strings.HasSuffix(cmdLine, tc.cmdLineEnd) {

			t.Fatalf("wrong cmdLine")

		}
		var segData SegmentUserData
		marshalled := strings.TrimPrefix(cmdLine, tc.cmdLineBeginning)

		marshalled = strings.TrimSuffix(marshalled, tc.cmdLineEnd)
		err := json.Unmarshal([]byte(marshalled), &segData)
		if err != nil {
			t.Fatalf("cant unmarshal UserData")

		}

		_, err = uuid.Parse(segData.ID)
		if err != nil {
			t.Fatalf("wrong uuid")
		}
		_, ok := tc.handler.currBackupInfo.segmentBackups[segData.ID]
		if !ok {
			t.Fatalf("UserData is not saved")
		}
	}
}

func TestBuildBackupPushCommandCrushes(t *testing.T) {

	handler := &BackupHandler{
		arguments: BackupArguments{
			Uploader:       nil,
			segmentFwdArgs: make([]SegmentFwdArg, 0),
			logsDir:        "test_no_use",
		},
		workers: BackupWorkers{},
		globalCluster: &cluster.Cluster{
			ContentIDs: nil,
			Hostnames:  nil,
			Segments:   nil,
			ByContent:  map[int][]*cluster.SegConfig{},
			ByHost:     nil,
			Executor:   nil,
		},
		currBackupInfo: CurrBackupInfo{},
		prevBackupInfo: PrevBackupInfo{},
	}

	if os.Getenv("FROM_TEST_BUILD_BACKUP_PUSH_COMMAND") == "1" {
		handler.buildBackupPushCommand(1)

		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestBuildBackupPushCommandCrushes")
	cmd.Env = append(os.Environ(), "FROM_TEST_BUILD_BACKUP_PUSH_COMMAND=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}
