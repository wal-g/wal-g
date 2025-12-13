package gpcluster

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

// GenerateSSHCommandForSegments - prepare ShellCommand for all Segments based on SegConfiguration.
// Return empty string to skip segment.
func GenerateSSHCommandForSegments(cl *cluster.Cluster, generator func(segment cluster.SegConfig) string) []cluster.ShellCommand {
	var commands []cluster.ShellCommand
	localHost := cl.GetHostForContent(-1)

	for _, segment := range cl.Segments { //nolint:gocritic // rangeValCopy
		cmd := generator(segment)
		if cmd == "" {
			continue
		}
		useLocal := segment.Hostname == localHost
		command := cluster.ConstructSSHCommand(useLocal, segment.Hostname, cmd)
		commands = append(commands, cluster.NewShellCommand(0, segment.ContentID, segment.Hostname, command))
	}
	return commands
}
