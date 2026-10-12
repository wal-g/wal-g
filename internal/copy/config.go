package copy

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
)

type ExecuteOptions struct {
	UseServerSideCopy      bool
	ServerSideCopyFallback bool
	// SkipErrorsFor matches source paths whose transfer failures may be skipped. Nil disables skipping.
	SkipErrorsFor *regexp.Regexp
}

func OptionsFromConfigs(source, destination *viper.Viper) ExecuteOptions {
	return ExecuteOptions{
		UseServerSideCopy:      rawCopyConfigEligible(source) && rawCopyConfigEligible(destination),
		ServerSideCopyFallback: source.GetBool(conf.ServerSideCopyFallback) || destination.GetBool(conf.ServerSideCopyFallback),
	}
}

// DisasterCopyPattern reads the database-specific default or a configured override.
func DisasterCopyPattern(source, destination *viper.Viper) (*regexp.Regexp, error) {
	if !viper.GetBool(conf.DisasterCopy) && !source.GetBool(conf.DisasterCopy) && !destination.GetBool(conf.DisasterCopy) {
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
