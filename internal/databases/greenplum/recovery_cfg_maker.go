package greenplum

import (
	"fmt"
	"strings"
)

type RecoveryTargetAction string

var (
	RecoveryTargetActionShutdown RecoveryTargetAction = "shutdown"
	RecoveryTargetActionPromote  RecoveryTargetAction = "promote"
	RecoveryTargetActionPause    RecoveryTargetAction = "pause"
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

func (m RecoveryConfigMaker) Make(contentID int, pgVersion int, action RecoveryTargetAction) string {
	var lines []string
	lines = append(lines,
		fmt.Sprintf("restore_command = '%s seg wal-fetch \"%%f\" \"%%p\" --content-id=%d --config %s'", m.walgBinaryPath, contentID, m.cfgPath),
		fmt.Sprintf("recovery_target_name = '%s'", m.recoveryTargetName),
		"recovery_target_timeline = latest",
		// `recovery_target_action` is available since PostgreSQL 9.5,
		// However, it was backported to Greenplum 6.25+ and now supported by all opensource GPDBs
		fmt.Sprintf("recovery_target_action = '%s'", action),
	)

	return strings.Join(lines, "\n")
}
