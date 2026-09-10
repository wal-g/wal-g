package copy_test

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	copyutil "github.com/wal-g/wal-g/internal/copy"
)

func TestOptionsFromConfigs(t *testing.T) {
	eligible := func() *viper.Viper {
		config := viper.New()
		config.Set("WALG_COMPRESSION_METHOD", "none")
		return config
	}

	t.Run("raw configs", func(t *testing.T) {
		source, destination := eligible(), eligible()
		destination.Set("S3_SSE", "aws:kms")
		destination.Set("S3_SSE_KMS_ID", "key-id")
		require.True(t, copyutil.OptionsFromConfigs(source, destination).UseServerSideCopy)
	})

	for _, test := range []struct {
		name    string
		setting string
	}{
		{"compression", "WALG_COMPRESSION_METHOD"},
		{"PGP key", "WALG_PGP_KEY"},
		{"PGP key path", "WALG_PGP_KEY_PATH"},
		{"envelope PGP key", "WALG_ENVELOPE_PGP_KEY"},
		{"envelope PGP key path", "WALG_ENVELOPE_PGP_KEY_PATH"},
		{"AWS CSE KMS", "WALG_CSE_KMS_ID"},
		{"YC CSE KMS", "YC_CSE_KMS_KEY_ID"},
		{"libsodium key", "WALG_LIBSODIUM_KEY"},
		{"libsodium key path", "WALG_LIBSODIUM_KEY_PATH"},
		{"legacy GPG key", "WALE_GPG_KEY_ID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, destination := eligible(), eligible()
			value := "configured"
			if test.setting == "WALG_COMPRESSION_METHOD" {
				value = "lz4"
			}
			source.Set(test.setting, value)
			require.False(t, copyutil.OptionsFromConfigs(source, destination).UseServerSideCopy)
		})
	}
}
