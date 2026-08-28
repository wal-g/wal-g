package copy

import (
	"strings"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
)

type ExecuteOptions struct {
	UseServerSideCopy bool
}

func OptionsFromConfigs(source, destination *viper.Viper) ExecuteOptions {
	return ExecuteOptions{UseServerSideCopy: rawCopyConfigEligible(source) && rawCopyConfigEligible(destination)}
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
