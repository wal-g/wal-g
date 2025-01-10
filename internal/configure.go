package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/crypto/yckms"
	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/awskms"
	cachenvlpr "github.com/wal-g/wal-g/internal/crypto/envelope/enveloper/cached"
	yckmsenvlpr "github.com/wal-g/wal-g/internal/crypto/envelope/enveloper/yckms"
	envopenpgp "github.com/wal-g/wal-g/internal/crypto/envelope/openpgp"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/stats/cache"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

const (
	pgDefaultDatabasePageSize = 8192
	DefaultDataBurstRateLimit = 8 * pgDefaultDatabasePageSize
	DefaultDataFolderPath     = "/tmp"
	WaleFileHost              = "file://localhost"
)

var DeprecatedExternalGpgMessage = fmt.Sprintf(
	`You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables %s or %s.
`, conf.PgpKeySetting, conf.PgpKeyPathSetting)

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
	if conf.Turbo {
		return
	}
	if viper.IsSet(conf.DiskRateLimitSetting) {
		diskLimit := viper.GetInt64(conf.DiskRateLimitSetting)
		limiters.DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit),
			int(diskLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}

	if viper.IsSet(conf.NetworkRateLimitSetting) {
		netLimit := viper.GetInt64(conf.NetworkRateLimitSetting)
		limiters.NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit),
			int(netLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}
}

// TODO : unit tests
func ConfigureStorage() (storage.HashableStorage, error) {
	var rootWraps []storage.WrapRootFolder
	if limiters.NetworkLimiter != nil {
		rootWraps = append(rootWraps, func(prevFolder storage.Folder) (newFolder storage.Folder) {
			return NewLimitedFolder(prevFolder, limiters.NetworkLimiter)
		})
	}
	rootWraps = append(rootWraps, ConfigureStoragePrefix)

	st, err := ConfigureStorageForSpecificConfig(viper.GetViper(), rootWraps...)
	if err != nil {
		return nil, err
	}

	return st, nil
}

func ConfigureStoragePrefix(folder storage.Folder) storage.Folder {
	prefix := viper.GetString(conf.StoragePrefixSetting)
	if prefix != "" {
		folder = folder.GetSubFolder(prefix)
	}
	return folder
}

// TODO: something with that
// when provided multiple 'keys' in the config,
// this function will always return only one concrete 'storage'.
// Chosen folder depends only on 'StorageAdapters' order
func ConfigureStorageForSpecificConfig(
	config *viper.Viper,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	skippedPrefixes := make([]string, 0)
	for _, adapter := range StorageAdapters {
		prefix, ok := conf.GetWaleCompatibleSettingFrom(adapter.PrefixSettingKey(), config)
		if !ok {
			skippedPrefixes = append(skippedPrefixes, "WALG_"+adapter.PrefixSettingKey())
			continue
		}

		settings := adapter.loadSettings(config)
		st, err := adapter.configure(prefix, settings, rootWraps...)
		if err != nil {
			return nil, fmt.Errorf("configure storage with prefix %q: %w", prefix, err)
		}
		return st, nil
	}
	return nil, newUnconfiguredStorageError(skippedPrefixes)
}

func getWalFolderPath() string {
	if !viper.IsSet(conf.PgDataSetting) {
		return DefaultDataFolderPath
	}
	return getRelativeWalFolderPath(viper.GetString(conf.PgDataSetting))
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
	pgSlotName = viper.GetString(conf.PgSlotName)
	if pgSlotName == "" {
		pgSlotName = "walg"
	}
	return
}

func ConfigureCompressor() (compression.Compressor, error) {
	compressionMethod := viper.GetString(conf.CompressionMethodSetting)
	if _, ok := compression.Compressors[compressionMethod]; !ok {
		return nil, newUnknownCompressionMethodError(compressionMethod)
	}
	return compression.Compressors[compressionMethod], nil
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

// ConfigureUploader is like ConfigureUploaderToFolder, but configures the default storage.
func ConfigureUploader() (*RegularUploader, error) {
	st, err := ConfigureStorage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure storage")
	}

	uploader, err := ConfigureUploaderToFolder(st.RootFolder())
	return uploader, err
}

// ConfigureUploaderToFolder connects to storage with the specified folder and creates an uploader.
// It makes sure that a valid session has started; if invalid, returns AWS error and `<nil>` value.
func ConfigureUploaderToFolder(folder storage.Folder) (uploader *RegularUploader, err error) {
	compressor, err := ConfigureCompressor()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure compression")
	}

	uploader = NewRegularUploader(compressor, folder)
	return uploader, err
}

func ConfigureUploaderWithoutCompressor() (Uploader, error) {
	st, err := ConfigureStorage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure storage")
	}

	uploader := NewRegularUploader(nil, st.RootFolder())
	return uploader, err
}

func ConfigureSplitUploader() (Uploader, error) {
	uploader, err := ConfigureUploader()
	if err != nil {
		return nil, err
	}

	var partitions = viper.GetInt(conf.StreamSplitterPartitions)
	var blockSize = viper.GetSizeInBytes(conf.StreamSplitterBlockSize)
	var maxFileSize = viper.GetInt(conf.StreamSplitterMaxFileSize)

	splitStreamUploader := NewSplitStreamUploader(uploader, partitions, int(blockSize), maxFileSize)
	return splitStreamUploader, nil
}

func ConfigureCrypter() crypto.Crypter {
	crypter, err := ConfigureCrypterForSpecificConfig(viper.GetViper())
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("can't configure crypter: %v", err)
	}
	return crypter
}

func CrypterFromConfig(configFile string) crypto.Crypter {
	var config = viper.New()
	conf.SetDefaultValues(config)
	conf.ReadConfigFromFile(config, configFile)
	conf.CheckAllowedSettings(config)

	crypter, err := ConfigureCrypterForSpecificConfig(config)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("can't configure crypter: %v", err)
	}
	return crypter
}

// ConfigureCrypter uses environment variables to create and configure a crypter.
// In case no configuration in environment variables found, return `<nil>` crypter.
func ConfigureCrypterForSpecificConfig(config *viper.Viper) (crypto.Crypter, error) {
	pgpKey := config.IsSet(conf.PgpKeySetting)
	pgpKeyPath := config.IsSet(conf.PgpKeyPathSetting)
	legacyGpg := config.IsSet(conf.GpgKeyIDSetting)

	envelopePgpKey := config.IsSet(conf.PgpEnvelopKeyPathSetting)
	envelopePgpKeyPath := config.IsSet(conf.PgpEnvelopeKeySetting)

	libsodiumKey := config.IsSet(conf.LibsodiumKeySetting)
	libsodiumKeyPath := config.IsSet(conf.LibsodiumKeyPathSetting)

	isPgpKey := pgpKey || pgpKeyPath || legacyGpg
	isEnvelopePgpKey := envelopePgpKey || envelopePgpKeyPath
	isLibsodium := libsodiumKey || libsodiumKeyPath

	if isPgpKey && isEnvelopePgpKey {
		return nil, errors.New("there is no way to configure plain gpg and envelope gpg at the same time, please choose one")
	}

	switch {
	case isPgpKey:
		return configurePgpCrypter(config)
	case isEnvelopePgpKey:
		return configureEnvelopePgpCrypter(config)
	case config.IsSet(conf.CseKmsIDSetting):
		return awskms.CrypterFromKeyID(config.GetString(conf.CseKmsIDSetting), config.GetString(conf.CseKmsRegionSetting)), nil
	case config.IsSet(conf.YcKmsKeyIDSetting):
		return yckms.YcCrypterFromKeyIDAndCredential(config.GetString(conf.YcKmsKeyIDSetting), config.GetString(conf.YcSaKeyFileSetting)), nil
	case isLibsodium:
		return configureLibsodiumCrypter(config)
	default:
		return nil, nil
	}
}

func configurePgpCrypter(config *viper.Viper) (crypto.Crypter, error) {
	loadPassphrase := func() (string, bool) {
		return conf.GetSetting(conf.PgpKeyPassphraseSetting)
	}
	// key can be either private (for download) or public (for upload)
	if config.IsSet(conf.PgpKeySetting) {
		return openpgp.CrypterFromKey(config.GetString(conf.PgpKeySetting), loadPassphrase), nil
	}

	// key can be either private (for download) or public (for upload)
	if config.IsSet(conf.PgpKeyPathSetting) {
		return openpgp.CrypterFromKeyPath(config.GetString(conf.PgpKeyPathSetting), loadPassphrase), nil
	}

	if keyRingID, ok := conf.GetWaleCompatibleSetting(conf.GpgKeyIDSetting); ok {
		tracelog.WarningLogger.Printf(DeprecatedExternalGpgMessage)
		return openpgp.CrypterFromKeyRingID(keyRingID, loadPassphrase), nil
	}
	return nil, errors.New("there is no any supported gpg crypter configuration")
}

func configureEnvelopePgpCrypter(config *viper.Viper) (crypto.Crypter, error) {
	if !config.IsSet(conf.PgpEnvelopeYcKmsKeyIDSetting) {
		return nil, errors.New("yandex cloud KMS key for client-side encryption and decryption must be configured")
	}

	yckmsEnveloper, err := yckmsenvlpr.EnveloperFromKeyIDAndCredential(
		config.GetString(conf.PgpEnvelopeYcKmsKeyIDSetting),
		config.GetString(conf.PgpEnvelopeYcSaKeyFileSetting),
		config.GetString(conf.PgpEnvelopeYcEndpointSetting),
	)
	if err != nil {
		return nil, err
	}
	expiration, err := conf.GetDurationSetting(conf.PgpEnvelopeCacheExpiration)
	if err != nil {
		return nil, err
	}
	enveloper := cachenvlpr.EnveloperWithCache(yckmsEnveloper, expiration)

	if config.IsSet(conf.PgpEnvelopKeyPathSetting) {
		return envopenpgp.CrypterFromKeyPath(viper.GetString(conf.PgpEnvelopKeyPathSetting), enveloper), nil
	}
	if config.IsSet(conf.PgpEnvelopeKeySetting) {
		return envopenpgp.CrypterFromKey(viper.GetString(conf.PgpEnvelopeKeySetting), enveloper), nil
	}
	return nil, errors.New("there is no any supported envelope gpg crypter configuration")
}

// TODO : unit tests
func GetDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(conf.DeltaMaxStepsSetting)
	if origin, hasOrigin := conf.GetSetting(conf.DeltaOriginSetting); hasOrigin {
		switch origin {
		case LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", conf.DeltaOriginSetting, origin)
		}
	}
	return
}

func GetSentinelUserData() (interface{}, error) {
	dataStr, ok := conf.GetSetting(conf.SentinelUserDataSetting)
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

func GetCommandSettingContext(ctx context.Context, variableName string, args ...string) (*exec.Cmd, error) {
	dataStr, ok := conf.GetSetting(variableName)
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
	if args != nil { //trick to add args to command
		dataStr = fmt.Sprintf("%s %s", dataStr, strings.Join(args, " "))
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
	oplogArchiveAfterSizeStr, _ := conf.GetSetting(conf.OplogArchiveAfterSize)
	oplogArchiveAfterSize, err := strconv.Atoi(oplogArchiveAfterSizeStr)
	if err != nil {
		return 0,
			fmt.Errorf("integer expected for %s setting but given '%s': %w",
				conf.OplogArchiveAfterSize, oplogArchiveAfterSizeStr, err)
	}
	return oplogArchiveAfterSize, nil
}

// nolint: gocyclo
func ConfigureSettings(currentType string) {
	if len(conf.DefaultConfigValues) == 0 {
		conf.DefaultConfigValues = conf.CommonDefaultConfigValues
		dbSpecificDefaultSettings := map[string]string{}
		switch currentType {
		case conf.PG:
			dbSpecificDefaultSettings = conf.PGDefaultSettings
		case conf.MONGO:
			dbSpecificDefaultSettings = conf.MongoDefaultSettings
		case conf.REDIS:
			dbSpecificDefaultSettings = conf.RedisDefaultSettings
		case conf.MYSQL:
			dbSpecificDefaultSettings = conf.MysqlDefaultSettings
		case conf.SQLSERVER:
			dbSpecificDefaultSettings = conf.SQLServerDefaultSettings
		case conf.GP:
			dbSpecificDefaultSettings = conf.GPDefaultSettings
		}

		for k, v := range dbSpecificDefaultSettings {
			conf.DefaultConfigValues[k] = v
		}
	}

	if len(conf.AllowedSettings) == 0 {
		conf.AllowedSettings = conf.CommonAllowedSettings
		dbSpecificSettings := map[string]bool{}
		switch currentType {
		case conf.PG:
			dbSpecificSettings = conf.PGAllowedSettings
		case conf.GP:
			for setting := range conf.PGAllowedSettings {
				conf.GPAllowedSettings[setting] = true
			}
			dbSpecificSettings = conf.GPAllowedSettings
		case conf.MONGO:
			dbSpecificSettings = conf.MongoAllowedSettings
		case conf.MYSQL:
			dbSpecificSettings = conf.MysqlAllowedSettings
		case conf.SQLSERVER:
			dbSpecificSettings = conf.SQLServerAllowedSettings
		case conf.REDIS:
			dbSpecificSettings = conf.RedisAllowedSettings
		}

		for k, v := range dbSpecificSettings {
			conf.AllowedSettings[k] = v
		}

		for _, adapter := range StorageAdapters {
			for _, setting := range adapter.settingNames {
				conf.AllowedSettings[setting] = true
			}
			conf.AllowedSettings["WALG_"+adapter.PrefixSettingKey()] = true
		}
	}
}

// StorageFromConfig prefers the config parameters instead of the current environment variables
func StorageFromConfig(configFile string) (storage.Storage, error) {
	var config = viper.New()
	conf.SetDefaultValues(config)
	conf.ReadConfigFromFile(config, configFile)
	conf.CheckAllowedSettings(config)

	folder, err := ConfigureStorageForSpecificConfig(config)

	if err != nil {
		tracelog.ErrorLogger.Println("Failed configure folder according to config " + configFile)
		tracelog.ErrorLogger.FatalError(err)
	}
	return folder, err
}

func ConfigureFailoverStorages() (failovers map[string]storage.HashableStorage, err error) {
	storageConfigs := viper.GetStringMap(conf.FailoverStorages)

	if len(storageConfigs) == 0 {
		return nil, nil
	}

	// errClosers are needed to close already configured storages if failed to configure all of them.
	var errClosers []io.Closer
	defer func() {
		if err == nil {
			return
		}
		for _, closer := range errClosers {
			utility.LoggedClose(closer, "Failed to close storage")
		}
	}()

	storages := make(map[string]storage.HashableStorage, len(storageConfigs))
	for name := range storageConfigs {
		if name == "default" {
			return nil, fmt.Errorf("'%s' storage name is reserved", name)
		}

		cfg := viper.Sub(conf.FailoverStorages + "." + name)

		var rootWraps []storage.WrapRootFolder
		if limiters.NetworkLimiter != nil {
			rootWraps = append(rootWraps, func(prevFolder storage.Folder) (newFolder storage.Folder) {
				return NewLimitedFolder(prevFolder, limiters.NetworkLimiter)
			})
		}
		rootWraps = append(rootWraps, ConfigureStoragePrefix)

		st, err := ConfigureStorageForSpecificConfig(cfg, rootWraps...)
		if err != nil {
			return nil, fmt.Errorf("failover storage %s: %v", name, err)
		}
		errClosers = append(errClosers, st)

		storages[name] = st
	}

	return storages, nil
}

func AssertRequiredSettingsSet() error {
	if !isAnyStorageSet() {
		return errors.New("Failed to find any configured storage")
	}

	for setting, required := range conf.RequiredSettings {
		isSet := viper.IsSet(setting)

		if !isSet && required {
			message := "Required variable " + setting + " is not set. You can set is using --" + conf.ToFlagName(setting) +
				" flag or variable " + setting
			return errors.New(message)
		}
	}

	return nil
}

func isAnyStorageSet() bool {
	for _, adapter := range StorageAdapters {
		_, exists := conf.GetWaleCompatibleSetting(adapter.PrefixSettingKey())
		if exists {
			return true
		}
	}

	return false
}

// ConfigureMultiStorage is responsible for configuring the primary storage along with any failover storages.
// It creates a multi-storage that combines them.
//
// Key details:
//   - It also initializes a cache to store the results of storage aliveness checks.
//   - The function does not set any specific policies for the root folder of the multi-storage. Initially, the policies.Default are used.
//   - If operations involve writing to the storage, the `checkWrite` parameter should be set to `true`.
//     This determines whether the health check is read-only (R/O) or read-write (R/W).
func ConfigureMultiStorage(checkWrite bool) (ms *multistorage.Storage, err error) {
	// errClosers are needed to close already configured storages if a fatal error happens before they are delegated to multi-storage.
	var errClosers []io.Closer
	defer func() {
		if err == nil {
			return
		}
		for _, closer := range errClosers {
			utility.LoggedClose(closer, "Failed to close storage")
		}
	}()

	primary, err := ConfigureStorage()
	if err != nil {
		return nil, fmt.Errorf("configure primary storage: %w", err)
	}
	errClosers = append(errClosers, primary)

	failovers, err := ConfigureFailoverStorages()
	if err != nil {
		return nil, fmt.Errorf("configure failover storages: %w", err)
	}
	for _, fo := range failovers {
		errClosers = append(errClosers, fo)
	}

	config := &multistorage.Config{}

	aliveChecksDefault := len(failovers) > 0
	config.AliveChecks, err = conf.GetBoolSettingDefault(conf.FailoverStoragesCheck, aliveChecksDefault)
	if err != nil {
		return nil, fmt.Errorf("get failover storage check setting: %w", err)
	}

	if config.AliveChecks {
		config.StatusCache, err = configureStatusCache()
		if err != nil {
			return nil, fmt.Errorf("configure failover storages status cache: %w", err)
		}

		config.AliveCheckTimeout, err = conf.GetDurationSetting(conf.FailoverStoragesCheckTimeout)
		if err != nil {
			return nil, fmt.Errorf("get failover storage check timeout setting: %w", err)
		}

		config.CheckWrite = checkWrite

		if config.CheckWrite {
			config.AliveCheckWriteBytes = viper.GetSizeInBytes(conf.FailoverStoragesCheckSize)
		}
	}

	ms, err = multistorage.NewStorage(config, primary, failovers)
	if err != nil {
		return nil, err
	}
	return ms, nil
}

func configureStatusCache() (*cache.Config, error) {
	config := &cache.Config{}

	var err error
	config.TTL, err = conf.GetDurationSetting(conf.FailoverStorageCacheLifetime)
	if err != nil {
		return nil, fmt.Errorf("get cache lifetime setting: %w", err)
	}

	emaDefault := cache.DefaultEMAParams
	ema := &cache.EMAParams{}

	ema.AliveLimit, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMAAliveLimit, emaDefault.AliveLimit)
	if err != nil {
		return nil, fmt.Errorf("get EMA alive limit setting: %w", err)
	}

	ema.DeadLimit, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMADeadLimit, emaDefault.DeadLimit)
	if err != nil {
		return nil, fmt.Errorf("get EMA dead limit setting: %w", err)
	}

	ema.AlphaAlive.Min, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMAAlphaAliveMin, emaDefault.AlphaAlive.Min)
	if err != nil {
		return nil, fmt.Errorf("get EMA alpha alive min setting: %w", err)
	}

	ema.AlphaAlive.Max, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMAAlphaAliveMax, emaDefault.AlphaAlive.Max)
	if err != nil {
		return nil, fmt.Errorf("get EMA alpha alive max setting: %w", err)
	}

	ema.AlphaDead.Min, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMAAlphaDeadMin, emaDefault.AlphaDead.Min)
	if err != nil {
		return nil, fmt.Errorf("get EMA alpha dead min setting: %w", err)
	}

	ema.AlphaDead.Max, err = conf.GetFloatSettingDefault(conf.FailoverStorageCacheEMAAlphaDeadMax, emaDefault.AlphaDead.Max)
	if err != nil {
		return nil, fmt.Errorf("get EMA alpha dead max setting: %w", err)
	}

	config.EMAParams = ema

	return config, nil
}
