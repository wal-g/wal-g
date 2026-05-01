package greenplum

import (
	"regexp"

	"github.com/apache/cloudberry-go-libs/dbconn"
	"github.com/blang/semver"
)

// cloudberry-go-libs dbconn matches only "Greenplum Database" & "Apache Cloudberry".
// Greengage is Greenplum-compatible (wal-g PR #2324); legacy releases report "Cloudberry Database X.Y.Z"
var (
	greengagePattern  = regexp.MustCompile(`\(Greengage Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`)
	cbdbLegacyPattern = regexp.MustCompile(`\(Cloudberry Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`)
)

// ParseVersionInfo augments dbconn parser with Greengage & legacy "Cloudberry Database" product strings
func ParseVersionInfo(versionString string) dbconn.GPDBVersion {
	var v dbconn.GPDBVersion
	v.ParseVersionInfo(versionString)
	if v.Type != dbconn.Unknown {
		return v
	}
	if m := greengagePattern.FindStringSubmatch(versionString); m != nil {
		if ver, err := semver.Make(m[1]); err == nil {
			v.Type = dbconn.GPDB
			v.SemVer = ver
		}
	} else if m := cbdbLegacyPattern.FindStringSubmatch(versionString); m != nil {
		if ver, err := semver.Make(m[1]); err == nil {
			v.Type = dbconn.CBDB
			v.SemVer = ver
		}
	}
	return v
}

type Flavor string

func NewFlavor(t dbconn.DBType) Flavor {
	switch t {
	case dbconn.GPDB:
		return Greenplum
	case dbconn.CBDB:
		return Cloudberry
	default:
		return Unknown
	}
}

func (f Flavor) String() string {
	return string(f)
}

func (f Flavor) ToDBType() dbconn.DBType {
	switch f {
	case Greenplum:
		return dbconn.GPDB
	case Cloudberry:
		return dbconn.CBDB
	default:
		return dbconn.Unknown
	}
}

const (
	Greenplum  Flavor = "greenplum"
	Cloudberry Flavor = "cloudberry"
	Unknown    Flavor = "unknown"
)

func EstimatePostgreSQLVersion(v dbconn.GPDBVersion) int {
	if v.IsCBDB() {
		return 140000
	}
	if v.SemVer.Major == 7 {
		return 120000
	}
	if v.SemVer.Major == 6 {
		return 90400
	}
	return 0
}
