package greenplum

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/google/uuid"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

// BackupArguments holds all arguments parsed from cmd to this handler class
type BackupArguments struct {
	isPermanent    bool
	userData       interface{}
	segmentFwdArgs []SegmentFwdArg
}

type SegmentUserData struct {
	ID string `json:"id"`
}

func NewSegmentUserData() SegmentUserData {
	return SegmentUserData{ID: uuid.New().String()}
}

func NewSegmentUserDataFromID(backupID string) SegmentUserData {
	return SegmentUserData{ID: backupID}
}

func (d SegmentUserData) String() string {
	b, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// QuotedString will do json.Marshal-ing followed by quoting in order to escape special control characters
// in the resulting JSON so it can be transferred as the cmdline argument to a segment
func (d SegmentUserData) QuotedString() string {
	return strconv.Quote(d.String())
}

// SegmentFwdArg describes the specific WAL-G
// arguments that is going to be forwarded to the segments
type SegmentFwdArg struct {
	Name  string
	Value string
}

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	Uploader *internal.Uploader
	Conn     *pgx.Conn
}

// CurrBackupInfo holds all information that is harvest during the backup process
type CurrBackupInfo struct {
	backupName       string
	segmentBackups   map[string]*cluster.SegConfig
	startTime        time.Time
	systemIdentifier *uint64
	gpVersion        semver.Version
	segmentsMetadata map[string]postgres.ExtendedMetadataDto
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	arguments      BackupArguments
	workers        BackupWorkers
	globalCluster  *cluster.Cluster
	currBackupInfo CurrBackupInfo
}

func (bh *BackupHandler) buildCommand(contentID int) string {
	segment := bh.globalCluster.ByContent[contentID][0]
	segUserData := NewSegmentUserData()
	bh.currBackupInfo.segmentBackups[segUserData.ID] = segment
	cmd := []string{
		"WALG_LOG_LEVEL=DEVEL",
		fmt.Sprintf("PGPORT=%d", segment.Port),
		"wal-g pg",
		fmt.Sprintf("backup-push %s", segment.DataDir),
		fmt.Sprintf("--walg-storage-prefix=%d", segment.ContentID),
		fmt.Sprintf("--add-user-data=%s", segUserData.QuotedString()),
		fmt.Sprintf("--config=%s", internal.CfgFile),
	}

	for _, arg := range bh.arguments.segmentFwdArgs {
		cmd = append(cmd, fmt.Sprintf("--%s=%s", arg.Name, arg.Value))
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

// HandleBackupPush handles the backup being read from filesystem and being pushed to the repository
func (bh *BackupHandler) HandleBackupPush() {
	bh.currBackupInfo.backupName = "backup_" + time.Now().Format(utility.BackupTimeFormat)
	bh.currBackupInfo.startTime = utility.TimeNowCrossPlatformUTC()

	tracelog.InfoLogger.Println("Running wal-g on segments")
	gplog.InitializeLogging("wal-g", "")
	remoteOutput := bh.globalCluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return bh.buildCommand(contentID)
		})
	bh.globalCluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("WAL-G output (segment %d):\n%s\n", command.Content, command.Stderr)
	}

	err := bh.connect()
	tracelog.ErrorLogger.FatalOnError(err)
	restoreLSNs, err := bh.createRestorePoint(bh.currBackupInfo.backupName)
	tracelog.ErrorLogger.FatalOnError(err)

	bh.currBackupInfo.segmentsMetadata, err = bh.fetchSegmentBackupsMetadata()
	tracelog.ErrorLogger.FatalOnError(err)

	sentinelDto := NewBackupSentinelDto(bh.currBackupInfo, restoreLSNs, bh.arguments.userData, bh.arguments.isPermanent)
	err = bh.uploadSentinel(sentinelDto)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", bh.currBackupInfo.backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	tracelog.InfoLogger.Printf("Backup %s successfully created", bh.currBackupInfo.backupName)
}

func (bh *BackupHandler) uploadSentinel(sentinelDto BackupSentinelDto) (err error) {
	tracelog.InfoLogger.Println("Uploading sentinel file")
	tracelog.InfoLogger.Println(sentinelDto.String())

	sentinelUploader := bh.workers.Uploader
	sentinelUploader.UploadingFolder = sentinelUploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	return internal.UploadSentinel(sentinelUploader, sentinelDto, bh.currBackupInfo.backupName)
}

func (bh *BackupHandler) connect() (err error) {
	tracelog.DebugLogger.Println("Connecting to Greenplum master.")
	bh.workers.Conn, err = postgres.Connect()
	if err != nil {
		return
	}
	return
}

func (bh *BackupHandler) createRestorePoint(restorePointName string) (restoreLSNs map[int]string, err error) {
	tracelog.InfoLogger.Printf("Creating restore point with name %s", restorePointName)
	queryRunner, err := NewGpQueryRunner(bh.workers.Conn)
	if err != nil {
		return
	}
	restoreLSNs, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
	if err != nil {
		return nil, err
	}
	return restoreLSNs, nil
}

func getGpClusterInfo() (globalCluster *cluster.Cluster, version semver.Version, systemIdentifier *uint64, err error) {
	tracelog.DebugLogger.Println("Initializing tmp connection to read Greenplum info")
	tmpConn, err := postgres.Connect()
	if err != nil {
		return globalCluster, semver.Version{}, nil, err
	}

	queryRunner, err := NewGpQueryRunner(tmpConn)
	if err != nil {
		return globalCluster, semver.Version{}, nil, err
	}

	versionStr, err := queryRunner.GetGreenplumVersion()
	if err != nil {
		return globalCluster, semver.Version{}, nil, err
	}
	tracelog.DebugLogger.Printf("Greenplum version: %s", versionStr)
	versionStart := strings.Index(versionStr, "(Greenplum Database ") + len("(Greenplum Database ")
	versionEnd := strings.Index(versionStr, ")")
	versionStr = versionStr[versionStart:versionEnd]
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(versionStr)[0]
	semVer, err := semver.Make(threeDigitVersion)
	if err != nil {
		return globalCluster, semver.Version{}, nil, err
	}

	segConfigs, err := queryRunner.GetGreenplumSegmentsInfo(semVer)
	if err != nil {
		return globalCluster, semver.Version{}, nil, err
	}
	globalCluster = cluster.NewCluster(segConfigs)

	return globalCluster, semVer, queryRunner.pgQueryRunner.SystemIdentifier, nil
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return bh, err
	}

	globalCluster, version, systemIdentifier, err := getGpClusterInfo()
	if err != nil {
		return bh, err
	}

	bh = &BackupHandler{
		arguments: arguments,
		workers: BackupWorkers{
			Uploader: uploader,
		},
		globalCluster: globalCluster,
		currBackupInfo: CurrBackupInfo{
			segmentBackups:   make(map[string]*cluster.SegConfig),
			gpVersion:        version,
			systemIdentifier: systemIdentifier,
		},
	}
	return bh, err
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(isPermanent bool, userData interface{}, fwdArgs []SegmentFwdArg) BackupArguments {
	return BackupArguments{
		isPermanent:    isPermanent,
		userData:       userData,
		segmentFwdArgs: fwdArgs,
	}
}

func (bh *BackupHandler) fetchSegmentBackupsMetadata() (map[string]postgres.ExtendedMetadataDto, error) {
	metadata := make(map[string]postgres.ExtendedMetadataDto)
	for backupID, segCfg := range bh.currBackupInfo.segmentBackups {
		selector, err := internal.NewUserDataBackupSelector(NewSegmentUserDataFromID(backupID).String(), postgres.NewGenericMetaFetcher())
		if err != nil {
			return nil, err
		}
		segBackupsFolder := bh.workers.Uploader.UploadingFolder.GetSubFolder(strconv.Itoa(segCfg.ContentID))

		backupName, err := selector.Select(segBackupsFolder)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to select matching backup for id %s from subfolder %s", backupID, segBackupsFolder.GetPath())
		}
		backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, segBackupsFolder)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get %s backup from subfolder %s", backupName, segBackupsFolder.GetPath())
		}

		var meta postgres.ExtendedMetadataDto
		err = backup.FetchMetadata(&meta)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get %s backup metadata from subfolder %s", backupName, segBackupsFolder.GetPath())
		}

		metadata[backupID] = meta
	}

	return metadata, nil
}
