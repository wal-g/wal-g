package archive

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/hashicorp/go-version"
)

type VersionParser struct {
	processName string
}

func NewVersionParser(processName string) *VersionParser {
	return &VersionParser{
		processName: processName,
	}
}

func (p *VersionParser) Get() (string, error) {
	cmd := []string{p.processName, "--version"}
	dataBytes, err := exec.Command(cmd[0], cmd[1:]...).Output()
	if err != nil {
		return "", fmt.Errorf("error in getting %s version: %v", p.processName, err)
	}
	data := string(dataBytes)
	parts := strings.Split(data, " ")
	for _, part := range parts {
		if !strings.HasPrefix(part, "v=") {
			continue
		}
		return part[2:], nil
	}
	return "", fmt.Errorf("version not found in %s", data)
}

func EnsureRestoreCompatibility(backupVersion, restoreVersion string) (bool, error) {
	b, err := version.NewVersion(backupVersion)
	if err != nil {
		return false, fmt.Errorf("backup version error: %v", err)
	}

	r, err := version.NewVersion(restoreVersion)
	if err != nil {
		return false, fmt.Errorf("restore version error: %v", err)
	}

	return b.LessThanOrEqual(r), nil
}

func EnsureRedisStopped() error {
	outErr, err := exec.Command("bash", "-c", "ps aux | grep [r]edis-server").CombinedOutput()
	if _, ok := err.(*exec.ExitError); ok && err.Error() == "exit status 1" && len(outErr) == 0 {
		return nil
	}

	return fmt.Errorf("unexpected result of checking running redis: %T: %v: %s", err, err, outErr)
}
