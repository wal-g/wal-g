package greenplum

import (
	"fmt"
	"regexp"

	"github.com/hashicorp/go-version"
)

type Flavor string

func (f Flavor) String() string {
	return string(f)
}

const (
	Greenplum  Flavor = "greenplum"
	Cloudberry Flavor = "cloudberry"
)

type Version struct {
	*version.Version
	Major  int
	Flavor Flavor // Note: can be '' for old backups
}

func NewVersion(v *version.Version, flavor Flavor) Version {
	return Version{
		Version: v,
		Major:   v.Segments()[0],
		Flavor:  flavor,
	}
}

func parseGreenplumVersion(versionStr string) (Version, error) {
	pattern := regexp.MustCompile(`(Greenplum Database|Greengage Database|Cloudberry Database|Apache Cloudberry) (\d+\.\d+\.\d+)`)
	groups := pattern.FindStringSubmatch(versionStr)
	if groups == nil {
		return Version{}, fmt.Errorf("unknown flavor: %s", versionStr)
	}
	semVer, err := version.NewVersion(groups[2])
	if err != nil {
		return Version{}, err
	}

	var flavor Flavor
	switch groups[1] {
	case "Greenplum Database":
		flavor = Greenplum
	case "Greengage Database":
		flavor = Greenplum
	case "Cloudberry Database":
		flavor = Cloudberry
	case "Apache Cloudberry":
		flavor = Cloudberry
	default:
		return Version{}, fmt.Errorf("unknown flavor: %s", groups[1])
	}

	return NewVersion(semVer, flavor), nil
}

func (v Version) EstimatePostgreSQLVersion() int {
	if v.Flavor == Cloudberry {
		return 140000
	}
	if v.Major == 7 {
		return 120000
	}
	if v.Major == 6 {
		return 90400
	}
	return 0
}
