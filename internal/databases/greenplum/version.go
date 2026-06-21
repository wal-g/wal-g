package greenplum

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

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
