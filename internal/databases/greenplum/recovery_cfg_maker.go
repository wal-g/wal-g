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

func (m RecoveryConfigMaker) Make(contentID int, pgVersion int) string {
	var lines []string
	lines = append(lines,
		fmt.Sprintf("restore_command = '%s seg wal-fetch \"%%f\" \"%%p\" --content-id=%d --config %s'", m.walgBinaryPath, contentID, m.cfgPath),
		fmt.Sprintf("recovery_target_name = '%s'", m.recoveryTargetName),
		"recovery_target_timeline = latest",
	)

	if pgVersion >= 120000 {
		lines = append(lines, "recovery_target_action = 'promote'")
	}

	return strings.Join(lines, "\n")
}
