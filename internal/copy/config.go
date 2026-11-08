package copy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
)

type ExecuteOptions struct {
	MaxRetries             *int
	Concurrency            int
	UseServerSideCopy      bool
	ServerSideCopyFallback bool
	OverwriteConflicts     bool
	// SkipErrorsFor matches source paths whose transfer failures may be skipped. Nil disables skipping.
	SkipErrorsFor *regexp.Regexp
}

func OptionsFromConfigs(source, destination *viper.Viper) (ExecuteOptions, error) {
	retries, err := configuredCopyRetries(source)
	if err != nil {
		return ExecuteOptions{}, err
	}
	concurrency, err := configuredCopyConcurrency(source)
	if err != nil {
		return ExecuteOptions{}, err
	}
	return ExecuteOptions{
		MaxRetries:             &retries,
		Concurrency:            concurrency,
		UseServerSideCopy:      rawCopyConfigEligible(source) && rawCopyConfigEligible(destination),
		ServerSideCopyFallback: source.GetBool(conf.ServerSideCopyFallback) || destination.GetBool(conf.ServerSideCopyFallback),
		OverwriteConflicts:     disasterCopyEnabled(source, destination),
	}, nil
}

func sourceOrMainSetting(source *viper.Viper, setting string) string {
	value := conf.CommonDefaultConfigValues[setting]
	if viper.IsSet(setting) {
		value = viper.GetString(setting)
	}
	if source != nil && source.InConfig(setting) {
		value = source.GetString(setting)
	}
	return strings.TrimSpace(value)
}

func configuredCopyRetries(source *viper.Viper) (int, error) {
	retries, err := strconv.Atoi(sourceOrMainSetting(source, conf.DownloadFileRetriesSetting))
	if err != nil || retries < 0 {
		return 0, fmt.Errorf("%s must be a nonnegative integer", conf.DownloadFileRetriesSetting)
	}
	return retries, nil
}

func configuredCopyConcurrency(source *viper.Viper) (int, error) {
	value := sourceOrMainSetting(source, conf.CopyConcurrency)
	concurrency, err := strconv.Atoi(value)
	if err != nil || concurrency < conf.MinAllowedConcurrency {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", conf.CopyConcurrency, value)
	}
	return concurrency, nil
}

func (o ExecuteOptions) copyRetries() (int, error) {
	if o.MaxRetries == nil {
		return configuredCopyRetries(nil)
	}
	if *o.MaxRetries < 0 {
		return 0, fmt.Errorf("%s must be a nonnegative integer", conf.DownloadFileRetriesSetting)
	}
	return *o.MaxRetries, nil
}

func (o ExecuteOptions) copyConcurrency() (int, error) {
	if o.Concurrency == 0 {
		return configuredCopyConcurrency(nil)
	}
	if o.Concurrency < conf.MinAllowedConcurrency {
		return 0, fmt.Errorf("%s must be a positive integer, got %d", conf.CopyConcurrency, o.Concurrency)
	}
	return o.Concurrency, nil
}

func disasterCopyEnabled(source, destination *viper.Viper) bool {
	return viper.GetBool(conf.DisasterCopy) || source.GetBool(conf.DisasterCopy) || destination.GetBool(conf.DisasterCopy)
}

// DisasterCopyPattern reads the database-specific default or a configured override.
func DisasterCopyPattern(source, destination *viper.Viper) (*regexp.Regexp, error) {
	if !disasterCopyEnabled(source, destination) {
		return nil, nil
	}
	pattern := viper.GetString(conf.DisasterCopySkipFiles)
	if destination.InConfig(conf.DisasterCopySkipFiles) {
		pattern = destination.GetString(conf.DisasterCopySkipFiles)
	} else if source.InConfig(conf.DisasterCopySkipFiles) {
		pattern = source.GetString(conf.DisasterCopySkipFiles)
	}
	if pattern == "" {
		return nil, nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", conf.DisasterCopySkipFiles, err)
	}
	return compiled, nil
}

func rawCopyConfigEligible(config *viper.Viper) bool {
	if config == nil || !strings.EqualFold(strings.TrimSpace(config.GetString(conf.CompressionMethodSetting)), "none") {
		return false
	}
	return !hasClientSideEncryption(config)
}

func hasClientSideEncryption(config *viper.Viper) bool {
	settings := []string{
		conf.PgpKeySetting,
		conf.PgpKeyPathSetting,
		conf.PgpEnvelopeKeySetting,
		conf.PgpEnvelopKeyPathSetting,
		conf.CseKmsIDSetting,
		conf.YcKmsKeyIDSetting,
		conf.LibsodiumKeySetting,
		conf.LibsodiumKeyPathSetting,
	}
	for _, setting := range settings {
		if strings.TrimSpace(config.GetString(setting)) != "" {
			return true
		}
	}
	legacyGPG, configured := conf.GetWaleCompatibleSettingFrom(conf.GpgKeyIDSetting, config)
	return configured && strings.TrimSpace(legacyGPG) != ""
}
