package greenplum

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

const RestorePointSuffix = "_restore_point.json"

type RestorePointMetadata struct {
	Name             string         `json:"name"`
	StartTime        time.Time      `json:"start_time"`
	FinishTime       time.Time      `json:"finish_time"`
	Hostname         string         `json:"hostname"`
	GpVersion        string         `json:"gp_version"`
	SystemIdentifier *uint64        `json:"system_identifier"`
	LsnBySegment     map[int]string `json:"lsn_by_segment"`
}

func (s *RestorePointMetadata) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

func RestorePointMetadataFileName(pointName string) string {
	return pointName + RestorePointSuffix
}

type RestorePointCreator struct {
	pointName        string
	startTime        time.Time
	systemIdentifier *uint64
	gpVersion        semver.Version

	Uploader *internal.Uploader
	Conn     *pgx.Conn

	logsDir string
}

// NewRestorePointCreator returns a restore point creator
func NewRestorePointCreator(pointName string) (rpc *RestorePointCreator, err error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return nil, err
	}

	conn, err := postgres.Connect()
	if err != nil {
		return nil, err
	}

	_, version, systemIdentifier, err := getGpClusterInfo(conn)
	if err != nil {
		return nil, err
	}

	rpc = &RestorePointCreator{
		pointName:        pointName,
		Uploader:         uploader,
		Conn:             conn,
		systemIdentifier: systemIdentifier,
		gpVersion:        version,
		logsDir:          viper.GetString(internal.GPLogsDirectory),
	}
	rpc.Uploader.UploadingFolder = rpc.Uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	return rpc, nil
}

// Create creates cluster-wide consistent restore point
func (rpc *RestorePointCreator) Create() {
	rpc.startTime = utility.TimeNowCrossPlatformUTC()
	gplog.InitializeLogging("wal-g", rpc.logsDir)

	err := rpc.checkExists()
	tracelog.ErrorLogger.FatalOnError(err)

	restoreLSNs, err := createRestorePoint(rpc.Conn, rpc.pointName)
	tracelog.ErrorLogger.FatalOnError(err)

	err = rpc.uploadMetadata(restoreLSNs)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for restore point %s", rpc.pointName)
		tracelog.ErrorLogger.FatalError(err)
	}
	tracelog.InfoLogger.Printf("Restore point %s successfully created", rpc.pointName)
}

func createRestorePoint(conn *pgx.Conn, restorePointName string) (restoreLSNs map[int]string, err error) {
	tracelog.InfoLogger.Printf("Creating restore point with name %s", restorePointName)
	queryRunner, err := NewGpQueryRunner(conn)
	if err != nil {
		return
	}
	restoreLSNs, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
	if err != nil {
		return nil, err
	}
	return restoreLSNs, nil
}

func (rpc *RestorePointCreator) checkExists() error {
	exists, err := rpc.Uploader.UploadingFolder.Exists(RestorePointMetadataFileName(rpc.pointName))
	if err != nil {
		return fmt.Errorf("failed to check restore point existence: %v", err)
	}
	if exists {
		return fmt.Errorf("restore point with name %s already exists", rpc.pointName)
	}
	return nil
}

func (rpc *RestorePointCreator) uploadMetadata(restoreLSNs map[int]string) (err error) {
	hostname, err := os.Hostname()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to fetch the hostname for metadata, leaving empty: %v", err)
	}

	meta := RestorePointMetadata{
		Name:             rpc.pointName,
		StartTime:        rpc.startTime,
		FinishTime:       utility.TimeNowCrossPlatformUTC(),
		Hostname:         hostname,
		GpVersion:        rpc.gpVersion.String(),
		SystemIdentifier: rpc.systemIdentifier,
		LsnBySegment:     restoreLSNs,
	}

	metaFileName := RestorePointMetadataFileName(rpc.pointName)
	tracelog.InfoLogger.Printf("Uploading restore point metadata file %s", metaFileName)
	tracelog.InfoLogger.Println(meta.String())

	return internal.UploadDto(rpc.Uploader.UploadingFolder, meta, metaFileName)
}
