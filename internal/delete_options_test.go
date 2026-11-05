package internal_test

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestRecoveryArgumentsRequireExplicitNames(t *testing.T) {
	for _, name := range []string{"LATEST", "FIND_FULL", "../B1", "a/b", "a\\b", "B*", "/B1", "..", "aosegments", "paxfiles"} {
		require.Error(t, internal.ValidateRecoveryName(name), name)
	}
	cmd := &cobra.Command{}
	o := internal.RecoveryOptions{}
	o.AddFlags(cmd)
	o.DeleteUnusedLogs = true
	require.Error(t, o.Validate(cmd, []string{"B1"}, cobra.ExactArgs(1)))
	o.AllowBroken = true
	require.NoError(t, o.Validate(cmd, []string{"B1", "B2"}, cobra.ExactArgs(1)))
	cmd.Flags().String(internal.DeleteTargetUserDataFlag, "data", "")
	require.Error(t, o.Validate(cmd, []string{"B1"}, cobra.ExactArgs(1)))
}
