package archive

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
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

type Version struct {
	Major int
	Minor int
	Patch int
}

func NewVersion(s string) (*Version, error) {
	partsStr := strings.Split(s, ".")
	if len(partsStr) != 3 {
		return nil, fmt.Errorf("unexpected version string format: %s", s)
	}

	var partsInt []int
	for _, partStr := range partsStr {
		partInt, err := strconv.Atoi(partStr)
		if err != nil {
			return nil, fmt.Errorf("unexpected version string part format: %s", s)
		}

		partsInt = append(partsInt, partInt)
	}
	return &Version{
		Major: partsInt[0],
		Minor: partsInt[1],
		Patch: partsInt[2],
	}, nil
}

func (v *Version) LessOrEqual(other *Version) bool {
	if v.Major <= other.Major {
		return true
	}
	if v.Minor <= other.Minor {
		return true
	}
	return v.Patch <= other.Patch
}

func EnsureRestoreCompatibility(backupVersion, restoreVersion string) (bool, error) {
	b, err := NewVersion(backupVersion)
	if err != nil {
		return false, fmt.Errorf("backup version error: %v", err)
	}

	r, err := NewVersion(restoreVersion)
	if err != nil {
		return false, fmt.Errorf("restore version error: %v", err)
	}

	return b.LessOrEqual(r), nil
}
