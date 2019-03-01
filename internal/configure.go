package internal

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"golang.org/x/time/rate"
	"os"
	"path/filepath"
)

const (
	DefaultStreamingPartSizeFor10Concurrency = 20 << 20
	DefaultDataBurstRateLimit                = 8 * int64(DatabasePageSize)
	DefaultDataFolderPath                    = "/tmp"
)

// TODO : unit tests
func getPathFromPrefix(prefix string) (bucket, server string, err error) {
	storageUrl, err := url.Parse(prefix)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse url '%s'", prefix)
	}
	if storageUrl.Scheme == "" || storageUrl.Host == "" {
		return "", "", errors.Errorf("missing url scheme=%q and/or host=%q", storageUrl.Scheme, storageUrl.Host)
	}

	bucket = storageUrl.Host
	server = strings.TrimPrefix(storageUrl.Path, "/")

	// Allover the code this parameter is concatenated with '/'.
	// TODO: Get rid of numerous string literals concatenated with this
	server = strings.TrimSuffix(server, "/")
	return bucket, server, nil
}

// TODO : unit tests
func configureLimiters() error {
	if diskLimitStr := getSettingValue("WALG_DISK_RATE_LIMIT"); diskLimitStr != "" {
		diskLimit, err := strconv.ParseInt(diskLimitStr, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse WALG_DISK_RATE_LIMIT")
		}
		DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit), int(diskLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}

	if netLimitStr := getSettingValue("WALG_NETWORK_RATE_LIMIT"); netLimitStr != "" {
		netLimit, err := strconv.ParseInt(netLimitStr, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse WALG_NETWORK_RATE_LIMIT")
		}
		NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit), int(netLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}
	return nil
}

// TODO : unit tests
func configureFolder() (StorageFolder, error) {
	waleS3Prefix := getSettingValue("WALE_S3_PREFIX")
	waleFilePrefix := getSettingValue("WALE_FILE_PREFIX")
	waleGSPrefix := getSettingValue("WALE_GS_PREFIX")
	waleAZPrefix := getSettingValue("WALE_AZ_PREFIX")
	waleSwiftPrefix := getSettingValue("WALE_SWIFT_PREFIX")
	if waleS3Prefix != "" {
		return ConfigureS3Folder(waleS3Prefix)
	} else if waleFilePrefix != "" {
		return ConfigureFSFolder(waleFilePrefix)
	} else if waleGSPrefix != "" {
		return ConfigureGSFolder(waleGSPrefix)
	} else if waleAZPrefix != "" {
		return ConfigureAzureFolder(waleAZPrefix)
	} else if waleSwiftPrefix != "" {
		return ConfigureSwiftFolder(waleSwiftPrefix)
	}
	return nil, NewUnsetEnvVarError([]string{"WALG_S3_PREFIX", "WALG_FILE_PREFIX", "WALG_GS_PREFIX", "WALG_AZ_PREFIX", "WALG_SWIFT_PREFIX"})
}

// TODO : unit tests
func getDataFolderPath() string {
	pgdata, ok := LookupConfigValue("PGDATA")
	var dataFolderPath string
	if !ok {
		dataFolderPath = DefaultDataFolderPath
	} else {
		dataFolderPath = filepath.Join(pgdata, "pg_wal")
		if _, err := os.Stat(dataFolderPath); err != nil {
			dataFolderPath = filepath.Join(pgdata, "pg_xlog")
			if _, err := os.Stat(dataFolderPath); err != nil {
				dataFolderPath = DefaultDataFolderPath
			}
		}
	}
	dataFolderPath = filepath.Join(dataFolderPath, "walg_data")
	return dataFolderPath
}

// TODO : unit tests
func configureWalDeltaUsage() (useWalDelta bool, deltaDataFolder DataFolder, err error) {
	if useWalDeltaStr, ok := LookupConfigValue("WALG_USE_WAL_DELTA"); ok {
		useWalDelta, err = strconv.ParseBool(useWalDeltaStr)
		if err != nil {
			return false, nil, errors.Wrapf(err, "failed to parse WALG_USE_WAL_DELTA")
		}
	}
	if !useWalDelta {
		return
	}
	dataFolderPath := getDataFolderPath()
	deltaDataFolder, err = NewDiskDataFolder(dataFolderPath)
	if err != nil {
		useWalDelta = false
		tracelog.WarningLogger.Printf("can't use wal delta feature because can't open delta data folder '%s'"+
			" due to error: '%v'\n", dataFolderPath, err)
		err = nil
	}
	return
}

// TODO : unit tests
func configureCompressor() (Compressor, error) {
	compressionMethod := getSettingValue("WALG_COMPRESSION_METHOD")
	if compressionMethod == "" {
		compressionMethod = Lz4AlgorithmName
	}
	if _, ok := Compressors[compressionMethod]; !ok {
		return nil, NewUnknownCompressionMethodError()
	}
	return Compressors[compressionMethod], nil
}

// TODO : unit tests
func configureLogging() error {
	logLevel, ok := LookupConfigValue("WALG_LOG_LEVEL")
	if ok {
		return tracelog.UpdateLogLevel(logLevel)
	}
	return nil
}

// Configure connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func Configure() (uploader *Uploader, destinationFolder StorageFolder, err error) {
	err = configureLogging()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure logging")
	}

	err = configureLimiters()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure limiters")
	}

	folder, err := configureFolder()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure folder")
	}

	compressor, err := configureCompressor()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure compression")
	}

	useWalDelta, deltaDataFolder, err := configureWalDeltaUsage()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to configure WAL Delta usage")
	}

	preventWalOverwrite := false
	if preventWalOverwriteStr := getSettingValue("WALG_PREVENT_WAL_OVERWRITE"); preventWalOverwriteStr != "" {
		preventWalOverwrite, err = strconv.ParseBool(preventWalOverwriteStr)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse WALG_PREVENT_WAL_OVERWRITE")
		}
	}

	uploader = NewUploader(compressor, folder, deltaDataFolder, useWalDelta, preventWalOverwrite)

	return uploader, folder, err
}
