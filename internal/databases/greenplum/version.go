package greenplum

import (
	"strconv"
	"strings"

	"github.com/apache/cloudberry-go-libs/dbconn"
)

// dbconn parser matches only "Greenplum Database" & "Apache Cloudberry".
// Remap those product strings so dbconn fills SemVer & Type
var productRemap = strings.NewReplacer(
	"Greengage Database", "Greenplum Database",
	"Cloudberry Database", "Apache Cloudberry",
)

// ParseVersionInfo augments dbconn parser with Greengage & legacy "Cloudberry Database" product strings
func ParseVersionInfo(versionString string) dbconn.GPDBVersion {
	var v dbconn.GPDBVersion
	v.ParseVersionInfo(versionString)
	if v.Type == dbconn.Unknown {
		v.ParseVersionInfo(productRemap.Replace(versionString))
		v.VersionString = versionString
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

func EstimatePostgreSQLVersion(flavor Flavor, gpVersion string) int {
	if flavor == Cloudberry {
		return 140000
	}
	majorStr, _, _ := strings.Cut(gpVersion, ".")
	major, _ := strconv.Atoi(majorStr)
	switch major {
	case 7:
		return 120000
	case 6:
		return 90400
	}
	return 0
}
