package greenplum

import (
	"fmt"
	"regexp"

	"github.com/blang/semver"
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
	semver.Version
	Flavor Flavor
}

func NewVersion(version semver.Version, flavor Flavor) Version {
	return Version{
		Version: version,
		Flavor:  flavor,
	}
}

func parseGreenplumVersion(version string) (Version, error) {
	pattern := regexp.MustCompile(`(Greenplum Database|Cloudberry Database) (\d+\.\d+\.\d+)`)
	groups := pattern.FindStringSubmatch(version)
	semVer, err := semver.Make(groups[2])
	if err != nil {
		return Version{}, err
	}

	var flavor Flavor
	if groups[1] == "Greenplum Database" {
		flavor = Greenplum
	} else if groups[1] == "Cloudberry Database" {
		flavor = Cloudberry
	} else {
		return Version{}, fmt.Errorf("unknown flavor: %s", groups[1])
	}

	return NewVersion(semVer, flavor), nil
}