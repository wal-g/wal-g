package binary

import (
	"strconv"

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

// MajorVersion return version in format '<MajorVersion>' (without minor, patch, prerelease, build, ...)
func MajorVersion(version string) string {
	if len(version) == 0 {
		return version
	}

	if version[0] != 'v' {
		version = "v" + version
	}

	return semver.Major(version)
}

// NextMajorVersion return next major version in format '<MajorVersion> + 1' (without minor, patch, prerelease, build, ...)
func NextMajorVersion(version string) string {
	if len(version) == 0 {
		return version
	}

	minorVersion := MajorMinorVersion(version)
	//For MongoDB 4.4 and previous, MongoDB versioning used a Production / Development versioning scheme,
	// and had the form X.Y.Z where X.Y refers to either a release series or development series.
	// Starting with MongoDB 5.0, MongoDB is released as two different release series: Major and Rapid releases
	if minorVersion == "v4.2" {
		return "v4"
	}

	majorVersionStr := MajorVersion(minorVersion)
	majorVersionInt, _ := strconv.Atoi(majorVersionStr[1:])
	return "v" + strconv.Itoa(majorVersionInt+1)
}

func EnsureCompatibilityToRestoreMongodVersions(backupMongodVersion, restoreMongodVersion string) error {
	tracelog.InfoLogger.Printf("Check compatibility backup version %v and current mongod version %v",
		backupMongodVersion, restoreMongodVersion)

	var nextMajorBackup = NextMajorVersion(backupMongodVersion)

	if semver.Compare(MajorMinorVersion(backupMongodVersion), MajorMinorVersion(restoreMongodVersion)) != 0 &&
		semver.Compare(MajorVersion(restoreMongodVersion), nextMajorBackup) != 0 {
		return errors.Errorf("Backup's Mongo version (%s) is not compatible with Mongo %s",
			backupMongodVersion, restoreMongodVersion)
	}
	return nil
}
