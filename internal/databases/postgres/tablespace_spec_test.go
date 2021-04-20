package postgres

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/utility"
)

func addTablespaces(spec *TablespaceSpec, strs []TablespaceLocation) {
	for _, loc := range strs {
		spec.addTablespace(loc.Symlink, loc.Location)
	}
}

func marshalAndUnmarshal(t *testing.T, spec *TablespaceSpec) {
	bytes, err := json.Marshal(spec)
	assert.NoError(t, err)
	err = json.Unmarshal(bytes, spec)
	assert.NoError(t, err)
}

func requireLocation(t *testing.T, spec TablespaceSpec, symlinkName string) TablespaceLocation {
	location, ok := spec.location(symlinkName)
	if !ok {
		t.Fail()
	}
	return location
}

func requireIsTablespaceSymlink(t *testing.T, spec TablespaceSpec, path string) bool {
	isSymlink, err := spec.isTablespaceSymlink(path)
	assert.NoError(t, err)
	return isSymlink
}

func formatLocations(tablespaceLocations []TablespaceLocation) {
	for index, loc := range tablespaceLocations {
		tablespaceLocations[index].Symlink = filepath.Join(TablespaceFolder, loc.Symlink)
		tablespaceLocations[index].Location = utility.NormalizePath(loc.Location)
	}
}

func TestTablespaceNames(t *testing.T) {
	spec := NewTablespaceSpec("/psql/")
	tablespaceLocations := []TablespaceLocation{
		{Location: "", Symlink: "12"},
		{Location: "", Symlink: "100"},
		{Location: "", Symlink: "101"},
	}
	addTablespaces(&spec, tablespaceLocations)

	marshalAndUnmarshal(t, &spec)

	names := spec.TablespaceNames()
	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	assert.Equal(t, "100", names[0])
	assert.Equal(t, "101", names[1])
	assert.Equal(t, "12", names[2])
}

func TestTablespaceLocation(t *testing.T) {
	spec := NewTablespaceSpec("/psql/")
	tablespaceLocations := []TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
		{Location: "/home/ismirn0ff/space3/", Symlink: "2"},
	}
	addTablespaces(&spec, tablespaceLocations)

	marshalAndUnmarshal(t, &spec)
	formatLocations(tablespaceLocations)

	assert.Equal(t, tablespaceLocations[0], requireLocation(t, spec, "3"))
	assert.Equal(t, tablespaceLocations[1], requireLocation(t, spec, "1"))
	assert.Equal(t, tablespaceLocations[2], requireLocation(t, spec, "2"))
}

func TestBasePrefix(t *testing.T) {
	spec := NewTablespaceSpec("/psql/")

	marshalAndUnmarshal(t, &spec)

	val, ok := spec.BasePrefix()
	assert.Equal(t, ok, true)
	assert.Equal(t, "/psql", val)
}

func setUpIsTablespaceSymlink(t *testing.T) TablespaceSpec {
	spec := NewTablespaceSpec("/psql/")
	tablespaceLocations := []TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
	}
	addTablespaces(&spec, tablespaceLocations)

	marshalAndUnmarshal(t, &spec)

	return spec
}

func TestIsTablespaceSymlink_NotActualPath(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/home/ismirn0ff/space2"))
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/home/ismirn0ff/space1"))
}

func TestIsTablespaceSymlink_NotActualPathSlash(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/home/ismirn0ff/space2/"))
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/home/ismirn0ff/space1/"))
}

func TestIsTablespaceSymlink_NotActualPathSubdirectory(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/home/ismirn0ff/space1/folder"))
}

func TestIsTablespaceSymlink_NotPgTblspcRoot(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, requireIsTablespaceSymlink(t, spec, "/psql/pg_tblspc/"))
}

func TestIsTablespaceSymlink_Symlink(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.True(t, requireIsTablespaceSymlink(t, spec, "/psql/pg_tblspc/1"))
	assert.True(t, requireIsTablespaceSymlink(t, spec, "/psql/pg_tblspc/3"))
}

func TestIsTablespaceSymlink_SymlinkSlash(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.True(t, requireIsTablespaceSymlink(t, spec, "/psql/pg_tblspc/1/"))
	assert.True(t, requireIsTablespaceSymlink(t, spec, "/psql/pg_tblspc/3/"))
}

func TestMakeTablespaceSymlinkPath(t *testing.T) {
	spec := NewTablespaceSpec("/psql/")
	spec.addTablespace("1", "/home/ismirn0ff/space1/")

	marshalAndUnmarshal(t, &spec)

	path, err := spec.makeTablespaceSymlinkPath("/home/ismirn0ff/space1/folder")
	assert.NoError(t, err)
	assert.Equal(t, "/psql/pg_tblspc/1/folder", path)

	path, err = spec.makeTablespaceSymlinkPath("/home/ismirn0ff/space1")
	assert.NoError(t, err)
	assert.Equal(t, "/psql/pg_tblspc/1", path)

	// Invalid path
	path, err = spec.makeTablespaceSymlinkPath("/home/ismirn0ff/")
	assert.Error(t, err)

	// usual postgres path
	path, err = spec.makeTablespaceSymlinkPath("/psql/some_path")
	assert.NoError(t, err)
	assert.Equal(t, "/psql/some_path", path)
}

func TestTablespaceLocations(t *testing.T) {
	spec := NewTablespaceSpec("/psql/")
	tablespaceLocations := []TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
		{Location: "/home/ismirn0ff/space3/", Symlink: "2"},
	}
	addTablespaces(&spec, tablespaceLocations)

	marshalAndUnmarshal(t, &spec)
	formatLocations(tablespaceLocations)

	returnedLocations := spec.tablespaceLocations()
	sort.Slice(returnedLocations, func(i, j int) bool {
		return returnedLocations[i].Symlink < returnedLocations[j].Symlink
	})
	sort.Slice(tablespaceLocations, func(i, j int) bool {
		return tablespaceLocations[i].Symlink < tablespaceLocations[j].Symlink
	})

	assert.Equal(t, tablespaceLocations, returnedLocations)
}
