package internal

import (
	"fmt"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
	"path/filepath"
)

const (
	BasePrefix  = "base_prefix"
	Tablespaces = "tablespaces"
)

// The mandatory keys for this map are "base_prefix" and "tablespaces".
// "base_prefix" contains Location of pg_data folder.
// "tablespaces" contains array of keys, which also happen to be names of tablespace folders.
// The rest keys should be these names of tablespace folders and values should be TablespaceLocation structs.
type TablespaceSpec map[string]interface{}

type TablespaceLocation struct {
	Location string `json:"loc"`
	Symlink  string `json:"link"`
}

func logInconsistentState(message string) {
	tracelog.WarningLogger.Printf("TablespaceSpecification has inconsistent state:\n%s", message)
}

func (spec TablespaceSpec) requireBasePrefix() (string, bool) {
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		logInconsistentState("Base prefix not set while working with tablespaces.")
	}
	return basePrefix, ok
}

func (spec TablespaceSpec) findTablespaceLocation(pathInsideTablespace string) (TablespaceLocation, bool) {
	for _, location := range spec.TablespaceLocations() {
		if utility.IsInDirectory(pathInsideTablespace, location.Location) {
			return location, true
		}
	}
	return TablespaceLocation{}, false
}

func (spec TablespaceSpec) Length() int {
	return len(spec.TablespaceNames())
}

func (spec TablespaceSpec) Empty() bool {
	return spec.Length() == 0
}

func (spec TablespaceSpec) TablespaceNames() []string {
	if _, ok := spec[Tablespaces]; !ok {
		spec[Tablespaces] = make([]string, 0)
	}
	names, ok := spec[Tablespaces].([]string)
	if ok {
		return names
	}

	//Need to restore type information after extraction
	interfaces := spec[Tablespaces].([]interface{})
	actualList := make([]string, 0, len(interfaces))
	for _, item := range interfaces {
		actualList = append(actualList, item.(string))
	}
	spec[Tablespaces] = actualList
	return actualList
}

func (spec TablespaceSpec) TablespaceLocations() []TablespaceLocation {
	locations := make([]TablespaceLocation, 0, spec.Length())
	for _, symlinkName := range spec.TablespaceNames() {
		location, ok := spec.Location(symlinkName)
		if !ok {
			logInconsistentState(fmt.Sprintf("No TablespaceLocation found for tablespace %s", symlinkName))
		}
		locations = append(locations, location)
	}
	return locations
}

func (spec TablespaceSpec) Location(symlinkName string) (TablespaceLocation, bool) {
	location, ok := spec[symlinkName].(TablespaceLocation)
	if ok {
		return location, true
	}

	//Need to restore type information after extraction
	specMap, ok := spec[symlinkName].(map[string]interface{})
	if !ok {
		return TablespaceLocation{}, false
	}
	location = TablespaceLocation{specMap["loc"].(string), specMap["link"].(string)}
	spec[symlinkName] = location
	return location, true
}

func (spec TablespaceSpec) SetBasePrefix(basePrefix string) {
	spec[BasePrefix] = utility.NormalizePath(basePrefix)
}

func (spec TablespaceSpec) BasePrefix() (string, bool) {
	if value, ok := spec[BasePrefix]; ok {
		return value.(string), true
	}
	return "", false
}

func (spec TablespaceSpec) AddTablespace(symlinkName string, actualLocation string) {
	actualLocation = utility.NormalizePath(actualLocation)
	names := spec.TablespaceNames()
	spec[Tablespaces] = append(names, symlinkName)
	spec[symlinkName] = TablespaceLocation{
		Location: actualLocation,
		Symlink:  filepath.Join(TablespaceFolder, symlinkName),
	}
}

func (spec TablespaceSpec) MakeTablespaceSymlinkPath(path string) (string, bool) {
	if basePrefix, ok := spec.requireBasePrefix(); ok {
		if !utility.IsInDirectory(path, basePrefix) {
			location, ok := spec.findTablespaceLocation(path)
			if !ok {
				return path, false
			}
			path = filepath.Join(basePrefix, location.Symlink, utility.GetSubdirectoryRelativePath(path, location.Location))
			return path, true
		}
		return path, true
	}
	return path, false
}

func (spec TablespaceSpec) IsTablespaceSymlink(path string) bool {
	for _, location := range spec.TablespaceLocations() {
		if basePrefix, ok := spec.requireBasePrefix(); ok {
			if utility.PathsEqual(path, filepath.Join(basePrefix, location.Symlink)) {
				return true
			}
		}
	}
	return false
}
