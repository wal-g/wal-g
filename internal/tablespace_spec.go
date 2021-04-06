package internal

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/wal-g/wal-g/utility"
)

const (
	BasePrefix  = "base_prefix"
	Tablespaces = "tablespaces"
)

var ErrorBasePrefixMissing = fmt.Errorf("base prefix not set while working with tablespaces")

// The mandatory keys for this map are "base_prefix" and "tablespaces".
// "base_prefix" contains Location of pg_data folder.
// "tablespaces" contains array of keys, which also happen to be names of tablespace folders.
// The rest keys should be these names of tablespace folders and values should be TablespaceLocation structs.
type TablespaceSpec struct {
	basePrefix            string
	tablespaceNames       []string
	tablespaceLocationMap map[string]TablespaceLocation
}

type TablespaceLocation struct {
	Location string `json:"loc"`
	Symlink  string `json:"link"`
}

func NewTablespaceSpec(basePrefix string) TablespaceSpec {
	spec := TablespaceSpec{
		"",
		make([]string, 0),
		make(map[string]TablespaceLocation),
	}
	spec.setBasePrefix(basePrefix)
	return spec
}

func (spec *TablespaceSpec) findTablespaceLocation(pathInsideTablespace string) (TablespaceLocation, bool) {
	for _, location := range spec.tablespaceLocations() {
		if utility.IsInDirectory(pathInsideTablespace, location.Location) {
			return location, true
		}
	}
	return TablespaceLocation{}, false
}

func (spec *TablespaceSpec) length() int {
	return len(spec.TablespaceNames())
}

func (spec *TablespaceSpec) empty() bool {
	return spec.length() == 0
}

func (spec *TablespaceSpec) TablespaceNames() []string {
	return spec.tablespaceNames
}

func (spec *TablespaceSpec) tablespaceLocations() []TablespaceLocation {
	locations := make([]TablespaceLocation, 0, spec.length())
	for _, location := range spec.tablespaceLocationMap {
		locations = append(locations, location)
	}
	return locations
}

func (spec *TablespaceSpec) location(symlinkName string) (TablespaceLocation, bool) {
	location, ok := spec.tablespaceLocationMap[symlinkName]
	if ok {
		return location, true
	}
	return TablespaceLocation{}, false
}

func (spec *TablespaceSpec) setBasePrefix(basePrefix string) {
	spec.basePrefix = utility.NormalizePath(basePrefix)
}

func (spec *TablespaceSpec) BasePrefix() (string, bool) {
	if spec.basePrefix != "" {
		return spec.basePrefix, true
	}
	return "", false
}

func (spec *TablespaceSpec) addTablespace(symlinkName string, actualLocation string) {
	actualLocation = utility.NormalizePath(actualLocation)
	spec.tablespaceNames = append(spec.tablespaceNames, symlinkName)
	spec.tablespaceLocationMap[symlinkName] = TablespaceLocation{
		Location: actualLocation,
		Symlink:  filepath.Join(TablespaceFolder, symlinkName),
	}
}

func (spec *TablespaceSpec) makeTablespaceSymlinkPath(path string) (string, error) {
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		return "", ErrorBasePrefixMissing
	}
	if utility.IsInDirectory(path, basePrefix) {
		return path, nil
	}
	location, ok := spec.findTablespaceLocation(path)
	if !ok {
		return path, fmt.Errorf("tablespace at path %s wasn't found", path)
	}
	path = filepath.Join(basePrefix, location.Symlink, utility.GetSubdirectoryRelativePath(path, location.Location))
	return path, nil
}

func (spec *TablespaceSpec) isTablespaceSymlink(path string) (bool, error) {
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		return false, ErrorBasePrefixMissing
	}

	for _, location := range spec.tablespaceLocations() {
		if utility.PathsEqual(path, filepath.Join(basePrefix, location.Symlink)) {
			return true, nil
		}
	}
	return false, nil
}

func (spec *TablespaceSpec) UnmarshalJSON(b []byte) error {
	jsonAsMap := make(map[string]interface{})
	err := json.Unmarshal(b, &jsonAsMap)
	if err != nil {
		return err
	}

	basePrefix, ok := jsonAsMap[BasePrefix].(string)
	if !ok {
		return ErrorBasePrefixMissing
	}
	spec.setBasePrefix(basePrefix)

	spec.tablespaceNames = make([]string, 0)
	if interfaces, ok := jsonAsMap[Tablespaces].([]interface{}); ok {
		for _, item := range interfaces {
			spec.tablespaceNames = append(spec.tablespaceNames, item.(string))
		}
	}

	spec.tablespaceLocationMap = make(map[string]TablespaceLocation)
	for _, symlinkName := range spec.tablespaceNames {
		specMap, ok := jsonAsMap[symlinkName].(map[string]interface{})
		if !ok {
			return fmt.Errorf("bad json structure. Couldn't find entry for symlink %s", symlinkName)
		}
		location := TablespaceLocation{specMap["loc"].(string), specMap["link"].(string)}
		spec.tablespaceLocationMap[symlinkName] = location
	}

	return nil
}

func (spec *TablespaceSpec) MarshalJSON() ([]byte, error) {
	toMarshal := make(map[string]interface{})
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		return nil, ErrorBasePrefixMissing
	}
	toMarshal[BasePrefix] = basePrefix
	toMarshal[Tablespaces] = spec.TablespaceNames()
	for symlinkName, location := range spec.tablespaceLocationMap {
		toMarshal[symlinkName] = location
	}
	return json.Marshal(toMarshal)
}
