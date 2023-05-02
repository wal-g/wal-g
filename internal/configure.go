package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/wal-g/wal-g/internal/crypto/yckms"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/awskms"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

const (
	pgDefaultDatabasePageSize = 8192
	DefaultDataBurstRateLimit = 8 * pgDefaultDatabasePageSize
	DefaultDataFolderPath     = "/tmp"
	WaleFileHost              = "file://localhost"
)

const MinAllowedConcurrency = 1

var DeprecatedExternalGpgMessage = fmt.Sprintf(
	`You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables %s or %s.
`, PgpKeySetting, PgpKeyPathSetting)

type UnconfiguredStorageError struct {
	error
}

func newUnconfiguredStorageError(storagePrefixVariants []string) UnconfiguredStorageError {
	return UnconfiguredStorageError{
		errors.Errorf("No storage is configured now, please set one of following settings: %v",
			storagePrefixVariants)}
}

func (err UnconfiguredStorageError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnknownCompressionMethodError struct {
	error
}

func newUnknownCompressionMethodError(method string) UnknownCompressionMethodError {
	return UnknownCompressionMethodError{
		errors.Errorf("Unknown compression method: '%s', supported methods are: %v",
			method, compression.CompressingAlgorithms)}
}

func (err UnknownCompressionMethodError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsetRequiredSettingError struct {
	error
}

func NewUnsetRequiredSettingError(settingName string) UnsetRequiredSettingError {
	return UnsetRequiredSettingError{errors.Errorf("%v is required to be set, but it isn't", settingName)}
}

func (err UnsetRequiredSettingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InvalidConcurrencyValueError struct {
	error
}

func newInvalidConcurrencyValueError(concurrencyType string, value int) InvalidConcurrencyValueError {
	return InvalidConcurrencyValueError{
		errors.Errorf("%v value is expected to be positive but is: %v",
			concurrencyType, value)}
}

func (err InvalidConcurrencyValueError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnmarshallingError struct {
	error
}

func newUnmarshallingError(subject string, err error) UnmarshallingError {
	return UnmarshallingError{errors.Errorf("Failed to unmarshal %s: %v", subject, err)}
}

func (err UnmarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
func ConfigureLimiters() {
	if Turbo {
		return
	}
	if viper.IsSet(DiskRateLimitSetting) {
		diskLimit := viper.GetInt64(DiskRateLimitSetting)
		limiters.DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit),
			int(diskLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}

	if viper.IsSet(NetworkRateLimitSetting) {
		netLimit := viper.GetInt64(NetworkRateLimitSetting)
		limiters.NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit),
			int(netLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}
}

// TODO : unit tests
func ConfigureFolder() (storage.Folder, error) {
	folder, err := ConfigureFolderForSpecificConfig(viper.GetViper())
	if err != nil {
		return nil, err
	}

	if limiters.NetworkLimiter != nil {
		folder = NewLimitedFolder(folder, limiters.NetworkLimiter)
	}

	return ConfigureStoragePrefix(folder), nil
}

func ConfigureStoragePrefix(folder storage.Folder) storage.Folder {
	prefix := viper.GetString(StoragePrefixSetting)
	if prefix != "" {
		folder = folder.GetSubFolder(prefix)
	}
	return folder
}

// TODO: something with that
// when provided multiple 'keys' in the config,
// this function will always return only one concrete 'folder'.
// Chosen folder depends only on 'StorageAdapters' order
func ConfigureFolderForSpecificConfig(config *viper.Viper) (storage.Folder, error) {
	skippedPrefixes := make([]string, 0)
	for _, adapter := range StorageAdapters {
		prefix, ok := getWaleCompatibleSettingFrom(adapter.prefixName, config)
		if !ok {
			skippedPrefixes = append(skippedPrefixes, "WALG_"+adapter.prefixName)
			continue
		}
		if adapter.prefixPreprocessor != nil {
			prefix = adapter.prefixPreprocessor(prefix)
		}

		settings := adapter.loadSettings(config)
		return adapter.configureFolder(prefix, settings)
	}
	return nil, newUnconfiguredStorageError(skippedPrefixes)
}

func getWalFolderPath() string {
	if !viper.IsSet(PgDataSetting) {
		return DefaultDataFolderPath
	}
	return getRelativeWalFolderPath(viper.GetString(PgDataSetting))
}

func getRelativeWalFolderPath(pgdata string) string {
	for _, walDir := range []string{"pg_wal", "pg_xlog"} {
		dataFolderPath := filepath.Join(pgdata, walDir)
		if _, err := os.Stat(dataFolderPath); err == nil {
			return dataFolderPath
		}
	}
	return DefaultDataFolderPath
}

func GetDataFolderPath() string {
	return filepath.Join(getWalFolderPath(), "walg_data")
}

// GetPgSlotName reads the slot name from the environment
func GetPgSlotName() (pgSlotName string) {
	pgSlotName = viper.GetString(PgSlotName)
	if pgSlotName == "" {
		pgSlotName = "walg"
	}
	return
}

// TODO : unit tests
func ConfigureCompressor() (compression.Compressor, error) {
	compressionMethod := viper.GetString(CompressionMethodSetting)
	if _, ok := compression.Compressors[compressionMethod]; !ok {
		return nil, newUnknownCompressionMethodError(compressionMethod)
	}
	return compression.Compressors[compressionMethod], nil
}

func ConfigureLogging() error {
	if viper.IsSet(LogLevelSetting) {
		return tracelog.UpdateLogLevel(viper.GetString(LogLevelSetting))
	}
	return nil
}

func getPGArchiveStatusFolderPath() string {
	return filepath.Join(getWalFolderPath(), "archive_status")
}

func getArchiveDataFolderPath() string {
	return filepath.Join(GetDataFolderPath(), "walg_archive_status")
}

func GetRelativeArchiveDataFolderPath() string {
	return filepath.Join(getRelativeWalFolderPath(""), "walg_data", "walg_archive_status")
}

// TODO : unit tests
func ConfigureArchiveStatusManager() (fsutil.DataFolder, error) {
	return fsutil.NewDiskDataFolder(getArchiveDataFolderPath())
}

func ConfigurePGArchiveStatusManager() (fsutil.DataFolder, error) {
	return fsutil.ExistingDiskDataFolder(getPGArchiveStatusFolderPath())
}

// ConfigureUploader connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func ConfigureUploader() (uploader Uploader, err error) {
	folder, err := ConfigureFolder()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure folder")
	}

	compressor, err := ConfigureCompressor()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure compression")
	}

	uploader = NewUploader(compressor, folder)
	return uploader, err
}

func ConfigureUploaderWithoutCompressor() (uploader Uploader, err error) {
	folder, err := ConfigureFolder()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure folder")
	}

	uploader = NewUploader(nil, folder)
	return uploader, err
}

func ConfigureSplitUploader() (Uploader, error) {
	uploader, err := ConfigureUploader()
	if err != nil {
		return nil, err
	}

	var partitions = viper.GetInt(StreamSplitterPartitions)
	var blockSize = viper.GetSizeInBytes(StreamSplitterBlockSize)
	var maxFileSize = viper.GetInt(StreamSplitterMaxFileSize)

	splitStreamUploader := NewSplitStreamUploader(uploader, partitions, int(blockSize), maxFileSize)
	return splitStreamUploader, nil
}

// ConfigureCrypter uses environment variables to create and configure a crypter.
// In case no configuration in environment variables found, return `<nil>` value.
func ConfigureCrypter() crypto.Crypter {
	loadPassphrase := func() (string, bool) {
		return GetSetting(PgpKeyPassphraseSetting)
	}

	// key can be either private (for download) or public (for upload)
	if viper.IsSet(PgpKeySetting) {
		return openpgp.CrypterFromKey(viper.GetString(PgpKeySetting), loadPassphrase)
	}

	// key can be either private (for download) or public (for upload)
	if viper.IsSet(PgpKeyPathSetting) {
		return openpgp.CrypterFromKeyPath(viper.GetString(PgpKeyPathSetting), loadPassphrase)
	}

	if keyRingID, ok := getWaleCompatibleSetting(GpgKeyIDSetting); ok {
		tracelog.WarningLogger.Printf(DeprecatedExternalGpgMessage)
		return openpgp.CrypterFromKeyRingID(keyRingID, loadPassphrase)
	}

	if viper.IsSet(CseKmsIDSetting) {
		return awskms.CrypterFromKeyID(viper.GetString(CseKmsIDSetting), viper.GetString(CseKmsRegionSetting))
	}

	if viper.IsSet(YcKmsKeyIDSetting) {
		return yckms.YcCrypterFromKeyIDAndCredential(viper.GetString(YcKmsKeyIDSetting), viper.GetString(YcSaKeyFileSetting))
	}

	if crypter := configureLibsodiumCrypter(); crypter != nil {
		return crypter
	}

	return nil
}

func GetMaxDownloadConcurrency() (int, error) {
	return GetMaxConcurrency(DownloadConcurrencySetting)
}

func GetMaxUploadConcurrency() (int, error) {
	return GetMaxConcurrency(UploadConcurrencySetting)
}

// This setting is intentionally undocumented in README. Effectively, this configures how many prepared tar Files there
// may be in uploading state during backup-push.
func getMaxUploadQueue() (int, error) {
	return GetMaxConcurrency(UploadQueueSetting)
}

// TODO : unit tests
func GetDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(DeltaMaxStepsSetting)
	if origin, hasOrigin := GetSetting(DeltaOriginSetting); hasOrigin {
		switch origin {
		case LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", DeltaOriginSetting, origin)
		}
	}
	return
}

func GetMaxUploadDiskConcurrency() (int, error) {
	if Turbo {
		return 4, nil
	}
	return GetMaxConcurrency(UploadDiskConcurrencySetting)
}

func GetMaxConcurrency(concurrencyType string) (int, error) {
	concurrency := viper.GetInt(concurrencyType)

	if concurrency < MinAllowedConcurrency {
		return MinAllowedConcurrency, newInvalidConcurrencyValueError(concurrencyType, concurrency)
	}
	return concurrency, nil
}

func GetSentinelUserData() (interface{}, error) {
	dataStr, ok := GetSetting(SentinelUserDataSetting)
	if !ok {
		return nil, nil
	}
	return UnmarshalSentinelUserData(dataStr)
}

func UnmarshalSentinelUserData(userDataStr string) (interface{}, error) {
	if len(userDataStr) == 0 {
		return nil, nil
	}

	var out interface{}
	err := json.Unmarshal([]byte(userDataStr), &out)
	if err != nil {
		return nil, errors.Wrapf(newUnmarshallingError(userDataStr, err), "failed to read the user data as a JSON object")
	}
	return out, nil
}

func GetCommandSettingContext(ctx context.Context, variableName string) (*exec.Cmd, error) {
	dataStr, ok := GetSetting(variableName)
	if !ok {
		tracelog.InfoLogger.Printf("command %s not configured", variableName)
		return nil, errors.New("command not configured")
	}
	if len(dataStr) == 0 {
		tracelog.ErrorLogger.Print(variableName + " expected.")
		return nil, errors.New(variableName + " not configured")
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	cmd := exec.CommandContext(ctx, shell, "-c", dataStr)
	// do not shut up subcommands by default
	cmd.Stderr = os.Stderr
	return cmd, nil
}

func GetCommandSetting(variableName string) (*exec.Cmd, error) {
	return GetCommandSettingContext(context.Background(), variableName)
}

func GetOplogArchiveAfterSize() (int, error) {
	oplogArchiveAfterSizeStr, _ := GetSetting(OplogArchiveAfterSize)
	oplogArchiveAfterSize, err := strconv.Atoi(oplogArchiveAfterSizeStr)
	if err != nil {
		return 0,
			fmt.Errorf("integer expected for %s setting but given '%s': %w",
				OplogArchiveAfterSize, oplogArchiveAfterSizeStr, err)
	}
	return oplogArchiveAfterSize, nil
}

func GetDurationSetting(setting string) (time.Duration, error) {
	intervalStr, ok := GetSetting(setting)
	if !ok {
		return 0, NewUnsetRequiredSettingError(setting)
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return 0, fmt.Errorf("duration expected for %s setting but given '%s': %w", setting, intervalStr, err)
	}
	return interval, nil
}

func GetOplogPITRDiscoveryIntervalSetting() (*time.Duration, error) {
	durStr, ok := GetSetting(OplogPITRDiscoveryInterval)
	if !ok {
		return nil, nil
	}
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return nil, fmt.Errorf("duration expected for %s setting but given '%s': %w", OplogPITRDiscoveryInterval, durStr, err)
	}
	return &dur, nil
}

func GetRequiredSetting(setting string) (string, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return "", NewUnsetRequiredSettingError(setting)
	}
	return val, nil
}

func GetBoolSettingDefault(setting string, def bool) (bool, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return def, nil
	}
	return strconv.ParseBool(val)
}

func GetBoolSetting(setting string) (val bool, ok bool, err error) {
	valstr, ok := GetSetting(setting)
	if !ok {
		return false, false, nil
	}
	val, err = strconv.ParseBool(valstr)
	return val, true, err
}
