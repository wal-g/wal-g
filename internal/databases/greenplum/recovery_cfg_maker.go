package greenplum

import (
	"fmt"
	"strings"
)

func NewRecoveryConfigMaker(walgBinaryPath, cfgPath, recoveryTargetName string,
	shutdownOnRecoveryTarget bool) RecoveryConfigMaker {
	return RecoveryConfigMaker{
		walgBinaryPath:           walgBinaryPath,
		cfgPath:                  cfgPath,
		recoveryTargetName:       recoveryTargetName,
		shutdownOnRecoveryTarget: shutdownOnRecoveryTarget,
	}
}

type RecoveryConfigMaker struct {
	walgBinaryPath           string
	cfgPath                  string
	recoveryTargetName       string
	shutdownOnRecoveryTarget bool
}

func (m RecoveryConfigMaker) Make(contentID int) string {
	var lines []string
	restoreCmd := fmt.Sprintf(
		"restore_command = '%s seg wal-fetch \"%%f\" \"%%p\" --content-id=%d --config %s'",
		m.walgBinaryPath, contentID, m.cfgPath)
	recoveryTarget := fmt.Sprintf("recovery_target_name = '%s'", m.recoveryTargetName)

	lines = append(lines, restoreCmd, recoveryTarget)

	if m.shutdownOnRecoveryTarget {
		lines = append(lines, "recovery_target_action = shutdown")
	}

	return strings.Join(lines, "\n")
}
