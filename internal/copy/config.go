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
	StatConcurrency        int
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
	concurrency, err := configuredConcurrency(source, conf.CopyConcurrency)
	if err != nil {
		return ExecuteOptions{}, err
	}
	statConcurrency, err := configuredConcurrency(source, conf.CopyStatConcurrency)
	if err != nil {
		return ExecuteOptions{}, err
	}
	return ExecuteOptions{
		MaxRetries:             &retries,
		Concurrency:            concurrency,
		StatConcurrency:        statConcurrency,
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

func configuredConcurrency(source *viper.Viper, setting string) (int, error) {
	value := sourceOrMainSetting(source, setting)
	concurrency, err := strconv.Atoi(value)
	if err != nil || concurrency < conf.MinAllowedConcurrency {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", setting, value)
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
	return optionConcurrency(o.Concurrency, conf.CopyConcurrency)
}

func (o ExecuteOptions) copyStatConcurrency() (int, error) {
	return optionConcurrency(o.StatConcurrency, conf.CopyStatConcurrency)
}

func optionConcurrency(concurrency int, setting string) (int, error) {
	if concurrency == 0 {
		return configuredConcurrency(nil, setting)
	}
	if concurrency < conf.MinAllowedConcurrency {
		return 0, fmt.Errorf("%s must be a positive integer, got %d", setting, concurrency)
	}
	return concurrency, nil
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
