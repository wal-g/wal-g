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
	logsDir string
}

func NewAOLengthCheckHandler(logsDir string) (*AOLengthCheckHandler, error) {
	initGpLog(logsDir)
	return &AOLengthCheckHandler{logsDir: logsDir}, nil
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

	remoteOutput := globalCluster.GenerateAndExecuteCommand("Run ao/aocs table length check",
		cluster.ON_SEGMENTS,
		func(contentID int) string {
			return buildCheckAOLengthCmd(contentID, globalCluster)
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

func buildCheckAOLengthCmd(contentID int, globalCluster *cluster.Cluster) string {
	segment := globalCluster.ByContent[contentID][0]

	backupPushArgs := []string{
		fmt.Sprintf("--port=%d", segment.Port),
		fmt.Sprintf("--segnum=%d", segment.ContentID),
	}

	backupPushArgsLine := strings.Join(backupPushArgs, " ")

	cmd := []string{
		// nohup to avoid the SIGHUP on SSH session disconnect
		"nohup", "wal-g",
		// config for wal-g
		fmt.Sprintf("--config=%s", internal.CfgFile),
		// method
		"c",
		// actual arguments to be passed to the backup-push command
		backupPushArgsLine,
		// forward stdout and stderr to the log file
		"&>>", formatSegmentLogPath(contentID),
	}
	cmdLine := strings.Join(cmd, " ")
	tracelog.InfoLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}
