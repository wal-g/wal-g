package greenplum

import (
	"encoding/json"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/spf13/viper"

	"github.com/jackc/pgx/v5"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

const RestorePointSuffix = "_restore_point.json"
const RestorePointCreateRetries = 5

type RestorePointMetadata struct {
	Name             string         `json:"name"`
	StartTime        time.Time      `json:"start_time"`
	FinishTime       time.Time      `json:"finish_time"`
	Hostname         string         `json:"hostname"`
	GpVersion        string         `json:"gp_version"`
	GpFlavor         string         `json:"gp_flavor"`
	SystemIdentifier *uint64        `json:"system_identifier"`
	LsnBySegment     map[int]string `json:"lsn_by_segment"`
	StorageName      string         `json:"storage_name"`
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

func FetchRestorePointMetadata(folder storage.Folder, pointName string) (RestorePointMetadata, error) {
	var restorePoint RestorePointMetadata
	err := internal.FetchDto(folder.GetSubFolder(utility.BaseBackupPath),
		&restorePoint, RestorePointMetadataFileName(pointName))
	if err != nil {
		return RestorePointMetadata{}, fmt.Errorf("failed to fetch metadata for restore point %s: %w", pointName, err)
	}

	return restorePoint, nil
}

// ValidateMatch checks that restore point is reachable from the provided backup
func ValidateMatch(folder storage.Folder, backupName, restorePoint, storage string) error {
	backup, err := NewBackupInStorage(folder, backupName, storage)
	if err != nil {
		return err
	}
	bSentinel, err := backup.GetSentinel()
	if err != nil {
		return fmt.Errorf("failed to fetch %s sentinel: %w", backupName, err)
	}

	rpMeta, err := FetchRestorePointMetadata(folder, restorePoint)
	if err != nil {
		tracelog.WarningLogger.Printf(
			"failed to fetch restore point %s metadata, will skip the validation check: %v", restorePoint, err)
		return nil
	}

	if bSentinel.FinishTime.After(rpMeta.FinishTime) {
		return fmt.Errorf("%s backup finish time (%s) is after the %s provided restore point finish time (%s)",
			backupName, bSentinel.FinishTime, restorePoint, rpMeta.FinishTime)
	}

	return nil
}

type RestorePointCreator struct {
	pointName        string
	startTime        time.Time
	systemIdentifier *uint64
	gpVersion        Version

	Uploader internal.Uploader
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
		logsDir:          viper.GetString(conf.GPLogsDirectory),
	}
	rpc.Uploader.ChangeDirectory(utility.BaseBackupPath)

	return rpc, nil
}

// Create creates cluster-wide consistent restore point
func (rpc *RestorePointCreator) Create() {
	rpc.startTime = utility.TimeNowCrossPlatformUTC()
	initGpLog(rpc.logsDir)

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
		return nil, err
	}

	for retries := 0; retries < RestorePointCreateRetries; retries++ {
		restoreLSNs, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
		if err == nil {
			// After create restore point should archive related WAL log segments.
			// This ensures the new cluster can retrieve complete WAL logs with the restore point for restoration.
			globalCluster, gpVersion, _, err := getGpClusterInfo(conn)
			if err != nil {
				return nil, err
			}
			tracelog.InfoLogger.Println("Switch xlog on cluster")
			remoteOutput := globalCluster.GenerateAndExecuteCommand("Running wal-g", cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
				func(contentID int) string {
					seg, ok := globalCluster.ByContent[contentID]
					if ok {
						var pgOptions, switchFunction string
						if gpVersion.Flavor == Greenplum && gpVersion.Major == 6 {
							pgOptions = "-c gp_session_role=utility"
							switchFunction = "pg_switch_xlog()"
						} else {
							pgOptions = "-c gp_role=utility"
							switchFunction = "pg_switch_wal()"
						}
						return fmt.Sprintf("PGOPTIONS='%s' psql -p %d -d postgres -c 'select %s;'", pgOptions, seg[0].Port, switchFunction)
					}
					return ""
				})
			globalCluster.CheckClusterError(remoteOutput, "Unable to switch xlog on cluster", func(contentID int) string {
				return "Unable to switch xlog on cluster"
			}, true)
			return restoreLSNs, nil
		}
	}
	return nil, err
}

func (rpc *RestorePointCreator) checkExists() error {
	exists, err := rpc.Uploader.Folder().Exists(RestorePointMetadataFileName(rpc.pointName))
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
		GpFlavor:         rpc.gpVersion.Flavor.String(),
		SystemIdentifier: rpc.systemIdentifier,
		LsnBySegment:     restoreLSNs,
	}

	metaFileName := RestorePointMetadataFileName(rpc.pointName)
	tracelog.InfoLogger.Printf("Uploading restore point metadata file %s", metaFileName)
	tracelog.InfoLogger.Println(meta.String())

	return internal.UploadDto(rpc.Uploader.Folder(), meta, metaFileName)
}

type RestorePointTime struct {
	Name        string    `json:"restore_point_name"`
	Time        time.Time `json:"time"`
	StorageName string    `json:"storage_name"`
}

type NoRestorePointsFoundError struct {
	error
}

func NewNoRestorePointsFoundError() NoRestorePointsFoundError {
	return NoRestorePointsFoundError{fmt.Errorf("no restore points found")}
}

func FetchAllRestorePoints(folder storage.Folder) ([]RestorePointMetadata, error) {
	restorePointMetas := make([]RestorePointMetadata, 0)

	restorePointTimes, err := GetRestorePoints(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return restorePointMetas, err
	}

	for _, rp := range restorePointTimes {
		meta, err := FetchRestorePointMetadata(folder, rp.Name)
		if err != nil {
			return restorePointMetas, fmt.Errorf("fetch restore point %s metadata: %v", rp.Name, err)
		}

		restorePointMetas = append(restorePointMetas, meta)
	}

	return restorePointMetas, nil
}

// FindRestorePointBeforeTS finds restore point that was created before the provided timestamp
// and finish time closest to the provided timestamp
func FindRestorePointBeforeTS(timestampStr string, restorePointMetas []RestorePointMetadata) (string, error) {
	ts, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return "", fmt.Errorf("timestamp parse error: %v", err)
	}

	var targetPoint *RestorePointMetadata
	for i := range restorePointMetas {
		meta := restorePointMetas[i]
		// target restore point should be created before or right at the provided ts
		if meta.FinishTime.After(ts) && !meta.FinishTime.Equal(ts) {
			continue
		}

		// we choose the restore point closest to the provided time
		if targetPoint == nil || targetPoint.FinishTime.Before(meta.FinishTime) {
			targetPoint = &meta
		}
	}

	if targetPoint == nil {
		return "", NewNoRestorePointsFoundError()
	}

	tracelog.InfoLogger.Printf("Found restore point %s with finish time %s, closest to the provided time %s",
		targetPoint.Name, targetPoint.FinishTime, ts)
	return targetPoint.Name, nil
}

// Finds restore point that contains timestamp
func FindRestorePointWithTS(timestampStr string, restorePointMetas []RestorePointMetadata) (string, error) {
	ts, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return "", fmt.Errorf("timestamp parse error: %v", err)
	}
	// add second because we round down when formatting
	ts = ts.Add(time.Second)

	var targetPoint *RestorePointMetadata
	for i := range restorePointMetas {
		meta := restorePointMetas[i]
		// target restore point should be finished after or right at the provided ts
		if meta.FinishTime.Before(ts) {
			continue
		}

		// we choose the restore point closest to the provided time
		if targetPoint == nil || meta.FinishTime.Before(targetPoint.FinishTime) {
			targetPoint = &meta
		}
	}

	if targetPoint == nil {
		return "", NewNoRestorePointsFoundError()
	}

	tracelog.InfoLogger.Printf("Found restore point %s with start time %s, closest to the provided time %s",
		targetPoint.Name, targetPoint.StartTime, ts)
	return targetPoint.Name, nil
}

// GetRestorePoints receives restore points descriptions and sorts them by time
func GetRestorePoints(folder storage.Folder) (restorePoints []RestorePointTime, err error) {
	restorePointsObjects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	restorePoints = GetRestorePointsTimeSlices(restorePointsObjects)

	count := len(restorePoints)
	if count == 0 {
		return nil, NewNoRestorePointsFoundError()
	}
	return
}

func GetRestorePointsTimeSlices(restorePoints []storage.Object) []RestorePointTime {
	restorePointsTimes := make([]RestorePointTime, 0)
	for _, object := range restorePoints {
		key := object.GetName()
		if !strings.HasSuffix(key, RestorePointSuffix) {
			continue
		}
		storageName := multistorage.GetStorage(object)
		time := object.GetLastModified()
		restorePointsTimes = append(restorePointsTimes,
			RestorePointTime{Name: StripRightmostRestorePointName(key), Time: time, StorageName: storageName})
	}

	sort.Slice(restorePointsTimes, func(i, j int) bool {
		return restorePointsTimes[i].Time.Before(restorePointsTimes[j].Time)
	})
	return restorePointsTimes
}

func StripRightmostRestorePointName(path string) string {
	path = strings.Trim(path, "/")
	all := strings.SplitAfter(path, "/")
	return stripRestorePointSuffix(all[len(all)-1])
}

func stripRestorePointSuffix(pathValue string) string {
	return strings.Split(pathValue, RestorePointSuffix)[0]
}
