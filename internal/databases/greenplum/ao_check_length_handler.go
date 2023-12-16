package greenplum

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type AOLengthCheckHandler struct {
	logsDir     string
	checkBackup bool
}

func NewAOLengthCheckHandler(logsDir string, checkBackup bool) (*AOLengthCheckHandler, error) {
	initGpLog(logsDir)
	return &AOLengthCheckHandler{
		logsDir:     logsDir,
		checkBackup: checkBackup,
	}, nil
}

func (checker *AOLengthCheckHandler) CheckAOTableLength() {
	conn, err := postgres.Connect()
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to get connection %v", err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to close connection %v", err)
		}
	}()

	globalCluster, _, _, err := getGpClusterInfo(conn)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("could not get cluster info %v", err)
	}

	remoteOutput := globalCluster.GenerateAndExecuteCommand("Run ao/aocs length check",
		cluster.ON_SEGMENTS,
		func(contentID int) string {
			return checker.buildCheckAOLengthCmd(contentID, globalCluster)
		})

	for _, command := range remoteOutput.Commands {
		if command.Error != nil {
			tracelog.ErrorLogger.Printf("error (segment %d):\n%v\n%s\n", command.Content, command.Error, command.Stderr)
		}
	}

	if remoteOutput.NumErrors > 0 {
		tracelog.ErrorLogger.Fatalln("failed check")
	} else {
		tracelog.InfoLogger.Println("check passed")
	}
}

func (checker *AOLengthCheckHandler) buildCheckAOLengthCmd(contentID int, globalCluster *cluster.Cluster) string {
	segment := globalCluster.ByContent[contentID][0]

	runCheckArgs := []string{
		fmt.Sprintf("--port=%d", segment.Port),
		fmt.Sprintf("--segnum=%d", segment.ContentID),
	}

	if checker.checkBackup {
		runCheckArgs = append(runCheckArgs, "--check-backup")
	}

	runCheckArgsLine := strings.Join(runCheckArgs, " ")

	cmd := []string{
		// nohup to avoid the SIGHUP on SSH session disconnect
		"nohup", "wal-g",
		// config for wal-g
		fmt.Sprintf("--config=%s", internal.CfgFile),
		// method
		"check-ao-aocs-length-segment",
		// actual arguments to be passed to the backup-push command
		runCheckArgsLine,
		// forward stdout and stderr to the log file
		"&>>", formatSegmentLogPath(contentID),
	}
	cmdLine := strings.Join(cmd, " ")
	tracelog.InfoLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}
