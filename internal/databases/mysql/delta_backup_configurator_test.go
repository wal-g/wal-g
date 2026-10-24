package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func TestRegularDeltaBackupConfigurator_ServerUUID(t *testing.T) {
	tests := []struct {
		name            string
		prevUUID        string
		curUUID         string
		wantIncremental bool
	}{
		{"mariadb has no server uuid", "", "", true},
		{"mysql same uuid", "uuid-1", "uuid-1", true},
		{"mysql uuid changed", "uuid-1", "uuid-2", false},
		{"mysql previous backup without uuid", "", "uuid-1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Set(conf.DeltaMaxStepsSetting, 3)
			defer viper.Set(conf.DeltaMaxStepsSetting, 0)

			folder := memory.NewFolder("", memory.NewKVS())
			lsn := LSN(100)
			sentinel := StreamSentinelDto{
				Hostname:      "host",
				ServerUUID:    tt.prevUUID,
				ServerVersion: "11.4.10-MariaDB",
				LSN:           &lsn,
			}
			data, err := json.Marshal(sentinel)
			require.NoError(t, err)
			err = folder.PutObject(context.Background(), utility.BaseBackupPath+"base_000"+utility.SentinelSuffix, bytes.NewReader(data))
			require.NoError(t, err)

			configurator := NewRegularDeltaBackupConfigurator(folder, internal.NewLatestBackupSelector())
			prev, incrementCount, err := configurator.Configure(
				context.Background(), false, "host", tt.curUUID, "11.4.10-MariaDB")
			require.NoError(t, err)

			if tt.wantIncremental {
				assert.Equal(t, "base_000", prev.name)
				assert.Equal(t, 1, incrementCount)
			} else {
				assert.Equal(t, "", prev.name)
				assert.Equal(t, 0, incrementCount)
			}
		})
	}
}
