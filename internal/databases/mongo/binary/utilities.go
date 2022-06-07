package binary

import (
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/mod/semver"
)

// MajorMinorVersion return version in format '<MajorVersion>.<MinorVersion>' (without patch, prerelease, build, ...)
func MajorMinorVersion(version string) string {
	if len(version) == 0 {
		return version
	}

	if version[0] != 'v' {
		version = "v" + version
	}

	return semver.MajorMinor(version)
}

func EnsureCompatibilityToRestoreMongodVersions(backupMongodVersion, restoreMongodVersion string) error {
	tracelog.InfoLogger.Printf("Check compatibility backup version %v and current mongod version %v",
		backupMongodVersion, restoreMongodVersion)

	if semver.Compare(MajorMinorVersion(backupMongodVersion), MajorMinorVersion(restoreMongodVersion)) != 0 {
		return errors.Errorf("Backup's Mongo version (%s) is not compatible with Mongo %s",
			backupMongodVersion, restoreMongodVersion)
	}
	return nil
}
