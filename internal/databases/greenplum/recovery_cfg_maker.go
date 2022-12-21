package greenplum

import (
	"fmt"
	"strings"
)

func NewRecoveryConfigMaker(walgBinaryPath, cfgPath, recoveryTargetName string) RecoveryConfigMaker {
	return RecoveryConfigMaker{
		walgBinaryPath:     walgBinaryPath,
		cfgPath:            cfgPath,
		recoveryTargetName: recoveryTargetName,
	}
}

type RecoveryConfigMaker struct {
	walgBinaryPath     string
	cfgPath            string
	recoveryTargetName string
}

func (m RecoveryConfigMaker) Make(contentID int) string {
	restoreCmd := fmt.Sprintf(
		"restore_command = '%s seg wal-fetch \"%%f\" \"%%p\" --content-id=%d --config %s'",
		m.walgBinaryPath, contentID, m.cfgPath)
	recoveryTargetName := fmt.Sprintf("recovery_target_name = '%s'", m.recoveryTargetName)
	recoveryTargetTli := "recovery_target_timeline = latest"

	return strings.Join([]string{restoreCmd, recoveryTargetName, recoveryTargetTli}, "\n")
}
