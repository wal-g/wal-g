package greenplum_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

const (
	walgPath           = "/usr/bin/wal-g"
	cfgPath            = "/etc/wal-g/wal-g.yaml"
	recoveryTargetName = "some_backup"
)

func TestGenerateRecoveryConf(t *testing.T) {
	recCfgMaker := greenplum.NewRecoveryConfigMaker(walgPath, cfgPath, recoveryTargetName, false)
	contentID := -1

	expectedCfg := `restore_command = '/usr/bin/wal-g seg wal-fetch "%f" "%p" --content-id=-1 --config /etc/wal-g/wal-g.yaml'
recovery_target_name = 'some_backup'`
	actualCfg := recCfgMaker.Make(contentID)
	assert.Equal(t, expectedCfg, actualCfg, "Actual recovery.conf does not match the expected one")
}

func TestGenerateRecoveryConfWithShutdown(t *testing.T) {
	recCfgMaker := greenplum.NewRecoveryConfigMaker(walgPath, cfgPath, recoveryTargetName, true)
	contentID := -1

	expectedCfg := `restore_command = '/usr/bin/wal-g seg wal-fetch "%f" "%p" --content-id=-1 --config /etc/wal-g/wal-g.yaml'
recovery_target_name = 'some_backup'
recovery_target_action = shutdown`
	actualCfg := recCfgMaker.Make(contentID)
	assert.Equal(t, expectedCfg, actualCfg, "Actual recovery.conf does not match the expected one")
}
