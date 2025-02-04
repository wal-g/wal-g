package greenplum

import (
	"context"
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type AOLengthCheckHandler struct {
	checkBackup bool
	backupName  string
	rootFolder  storage.Folder
}

func NewAOLengthCheckHandler(
	logsDir string,
	checkBackup bool,
	backupName string,
	rootFolder storage.Folder,
) (*AOLengthCheckHandler, error) {
	initGpLog(logsDir)
	return &AOLengthCheckHandler{
		checkBackup: checkBackup,
		backupName:  backupName,
		rootFolder:  rootFolder,
	}, nil
}

func (checker *AOLengthCheckHandler) CheckAOTableLength() {
	conn, err := postgres.Connect()
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
	}
	defer func() {
		err := conn.Close(context.TODO())
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to close connection %v", err)
		}
	}()

	globalCluster, _, _, err := getGpClusterInfo(conn)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("could not get cluster info %v", err)
	}

	segmentsBackups := make(map[int]string)
	if checker.checkBackup {
		segmentsBackups, err = getSegmentBackupNames(checker.backupName, checker.rootFolder)
		if err != nil {
			tracelog.ErrorLogger.FatalfOnError("could not get segment`s backups %v", err)
		}
	}

	remoteOutput := globalCluster.GenerateAndExecuteCommand("Run ao/aocs length check",
		cluster.ON_SEGMENTS,
		func(contentID int) string {
			return checker.buildCheckAOLengthCmd(contentID, segmentsBackups, globalCluster)
		})

	for _, command := range remoteOutput.Commands {
		if command.Error != nil {
			tracelog.ErrorLogger.Printf("error (segment %d):\n%v\n%s\n", command.Content, command.Error, command.Stderr)
		}
	}

	if remoteOutput.NumErrors > 0 {
		tracelog.ErrorLogger.Fatalln("check failed, for more information check log on segments")
	} else {
		tracelog.InfoLogger.Println("check passed")
	}
}

func (checker *AOLengthCheckHandler) buildCheckAOLengthCmd(contentID int, backupNames map[int]string,
	globalCluster *cluster.Cluster) string {
	segment := globalCluster.ByContent[contentID][0]
	runCheckArgs := []string{
		fmt.Sprintf("--port=%d", segment.Port),
		fmt.Sprintf("--segnum=%d", segment.ContentID),
	}

	if checker.checkBackup {
		runCheckArgs = append(runCheckArgs, "--check-backup", fmt.Sprintf("--backup-name=%s", backupNames[contentID]))
	}

	runCheckArgsLine := strings.Join(runCheckArgs, " ")

	cmd := []string{
		// nohup to avoid the SIGHUP on SSH session disconnect
		"nohup", "wal-g",
		// config for wal-g
		fmt.Sprintf("--config=%s", conf.CfgFile),
		// method
		"check-ao-aocs-length-segment",
		// actual arguments to be passed to the check-ao command
		runCheckArgsLine,
		// forward stdout and stderr to the log file
		"&>>", formatSegmentLogPath(contentID),
	}
	cmdLine := strings.Join(cmd, " ")
	tracelog.InfoLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

func getSegmentBackupNames(name string, rootFolder storage.Folder) (map[int]string, error) {
	backup, err := internal.GetBackupByName(name, utility.BaseBackupPath, rootFolder)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to get latest backup")
		return nil, err
	}
	var sentinel BackupSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to get latest backup")
		return nil, err
	}
	segmentsBackupNames := map[int]string{}
	for _, meta := range sentinel.Segments {
		segmentsBackupNames[meta.ContentID] = meta.BackupName
	}
	return segmentsBackupNames, nil
}
