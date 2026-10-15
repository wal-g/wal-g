package copy_test

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
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
		options, err := copyutil.OptionsFromConfigs(source, destination)
		require.NoError(t, err)
		require.True(t, options.UseServerSideCopy)
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
			options, err := copyutil.OptionsFromConfigs(source, destination)
			require.NoError(t, err)
			require.False(t, options.UseServerSideCopy)
		})
	}
}

func TestCopyRetryConfigPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, main, source, destination string
		want                            int
		invalid                         bool
	}{
		{name: "default", want: 15},
		{name: "main", main: "4", want: 4},
		{name: "source wins", main: "4", source: "2", want: 2},
		{name: "source zero", main: "4", source: "0", want: 0},
		{name: "destination ignored", main: "4", destination: "9", want: 4},
		{name: "negative", source: "-1", invalid: true},
		{name: "invalid", source: "bad", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			if tc.main != "" {
				viper.Set("WALG_DOWNLOAD_FILE_RETRIES", tc.main)
			}
			source, destination := viper.New(), viper.New()
			source.SetConfigType("json")
			destination.SetConfigType("json")
			if tc.source != "" {
				require.NoError(t, source.ReadConfig(strings.NewReader(`{"WALG_DOWNLOAD_FILE_RETRIES":"`+tc.source+`"}`)))
			}
			if tc.destination != "" {
				require.NoError(t, destination.ReadConfig(strings.NewReader(`{"WALG_DOWNLOAD_FILE_RETRIES":"`+tc.destination+`"}`)))
			}
			options, err := copyutil.OptionsFromConfigs(source, destination)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, *options.MaxRetries)
		})
	}
}

func TestDisasterCopyConfigEnablement(t *testing.T) {
	for _, tc := range []struct {
		name, environment, main, source, destination string
		want                                         bool
	}{
		{name: "default"},
		{name: "explicitly disabled", environment: "false", main: "false", source: "false", destination: "false"},
		{name: "environment", environment: "true", want: true},
		{name: "main", main: "true", want: true},
		{name: "source", source: "true", want: true},
		{name: "destination", destination: "true", want: true},
		{name: "environment overrides main", environment: "true", main: "false", want: true},
		{name: "environment disables main", environment: "false", main: "true"},
		{name: "main enables despite storage configs", main: "true", source: "false", destination: "false", want: true},
		{name: "source enables despite main and destination", main: "false", source: "true", destination: "false", want: true},
		{name: "destination enables despite main and source", main: "false", source: "false", destination: "true", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv(conf.DisasterCopy, tc.environment)
			viper.AutomaticEnv()
			viper.SetDefault(conf.DisasterCopy, false)
			viper.Set(conf.DisasterCopySkipFiles, "^history$")
			source, destination := viper.New(), viper.New()
			for _, setting := range []struct {
				config *viper.Viper
				value  string
			}{
				{viper.GetViper(), tc.main},
				{source, tc.source},
				{destination, tc.destination},
			} {
				if setting.value != "" {
					require.NoError(t, setting.config.MergeConfigMap(map[string]any{conf.DisasterCopy: setting.value}))
				}
			}

			options, err := copyutil.OptionsFromConfigs(source, destination)
			require.NoError(t, err)
			require.Equal(t, tc.want, options.OverwriteConflicts)
			require.Nil(t, options.SkipErrorsFor, "shared options must not enable database-specific error skipping")
			pattern, err := copyutil.DisasterCopyPattern(source, destination)
			require.NoError(t, err)
			if tc.want {
				require.NotNil(t, pattern)
				require.True(t, pattern.MatchString("history"))
			} else {
				require.Nil(t, pattern)
			}
		})
	}
}
