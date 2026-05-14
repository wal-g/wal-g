package greenplum

import (
	"github.com/apache/cloudberry-go-libs/dbconn"
)

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
