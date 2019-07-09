package test

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"path/filepath"
	"sort"
	"testing"
)

func addTablespaces(spec internal.TablespaceSpec, strs []internal.TablespaceLocation) {
	for _, loc := range strs {
		spec.AddTablespace(loc.Symlink, loc.Location)
	}
}

func marshalAndUnmarshal(t *testing.T, spec internal.TablespaceSpec) {
	bytes, err := json.Marshal(spec)
	assert.NoError(t, err)
	err = json.Unmarshal(bytes, &spec)
	assert.NoError(t, err)
}

func requireLocation(t *testing.T, spec internal.TablespaceSpec, symlinkName string) internal.TablespaceLocation {
	location, ok := spec.Location(symlinkName)
	if !ok {
		t.Fail()
	}
	return location
}

func formatLocations(tablespaceLocations []internal.TablespaceLocation) {
	for index, loc := range tablespaceLocations {
		tablespaceLocations[index].Symlink = filepath.Join(internal.TablespaceFolder, loc.Symlink)
		tablespaceLocations[index].Location = utility.NormalizePath(loc.Location)
	}
}

func TestTablespaceNames(t *testing.T) {
	spec := internal.TablespaceSpec{}
	tablespaceLocations := []internal.TablespaceLocation{
		{Location: "", Symlink: "12"},
		{Location: "", Symlink: "100"},
		{Location: "", Symlink: "101"},
	}
	addTablespaces(spec, tablespaceLocations)

	marshalAndUnmarshal(t, spec)

	names := spec.TablespaceNames()
	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	assert.Equal(t, "100", names[0])
	assert.Equal(t, "101", names[1])
	assert.Equal(t, "12", names[2])
}

func TestTablespaceLocation(t *testing.T) {
	spec := internal.TablespaceSpec{}
	tablespaceLocations := []internal.TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
		{Location: "/home/ismirn0ff/space3/", Symlink: "2"},
	}
	addTablespaces(spec, tablespaceLocations)

	marshalAndUnmarshal(t, spec)
	formatLocations(tablespaceLocations)

	assert.Equal(t, tablespaceLocations[0], requireLocation(t, spec, "3"))
	assert.Equal(t, tablespaceLocations[1], requireLocation(t, spec, "1"))
	assert.Equal(t, tablespaceLocations[2], requireLocation(t, spec, "2"))
}

func TestBasePrefix(t *testing.T) {
	spec := internal.TablespaceSpec{}
	spec.SetBasePrefix("/psql/")

	marshalAndUnmarshal(t, spec)

	val, ok := spec.BasePrefix()
	assert.Equal(t, ok, true)
	assert.Equal(t, "/psql", val)
}

func setUpIsTablespaceSymlink(t *testing.T) internal.TablespaceSpec {
	spec := internal.TablespaceSpec{}
	spec.SetBasePrefix("/psql/")
	tablespaceLocations := []internal.TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
	}
	addTablespaces(spec, tablespaceLocations)

	marshalAndUnmarshal(t, spec)

	return spec
}

func TestIsTablespaceSymlink_NotActualPath(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, spec.IsTablespaceSymlink("/home/ismirn0ff/space2"))
	assert.False(t, spec.IsTablespaceSymlink("/home/ismirn0ff/space1"))
}

func TestIsTablespaceSymlink_NotActualPathSlash(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, spec.IsTablespaceSymlink("/home/ismirn0ff/space2/"))
	assert.False(t, spec.IsTablespaceSymlink("/home/ismirn0ff/space1/"))
}

func TestIsTablespaceSymlink_NotActualPathSubdirectory(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, spec.IsTablespaceSymlink("/home/ismirn0ff/space1/folder"))
}

func TestIsTablespaceSymlink_NotPgTblspcRoot(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.False(t, spec.IsTablespaceSymlink("/psql/pg_tblspc/"))
}

func TestIsTablespaceSymlink_Symlink(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.True(t, spec.IsTablespaceSymlink("/psql/pg_tblspc/1"))
	assert.True(t, spec.IsTablespaceSymlink("/psql/pg_tblspc/3"))
}

func TestIsTablespaceSymlink_SymlinkSlash(t *testing.T) {
	spec := setUpIsTablespaceSymlink(t)
	assert.True(t, spec.IsTablespaceSymlink("/psql/pg_tblspc/1/"))
	assert.True(t, spec.IsTablespaceSymlink("/psql/pg_tblspc/3/"))
}

func TestMakeTablespaceSymlinkPath(t *testing.T) {
	spec := internal.TablespaceSpec{}
	spec.SetBasePrefix("/psql/")
	spec.AddTablespace("1", "/home/ismirn0ff/space1/")

	marshalAndUnmarshal(t, spec)

	path, ok := spec.MakeTablespaceSymlinkPath("/home/ismirn0ff/space1/folder")
	assert.True(t, ok)
	assert.Equal(t, "/psql/pg_tblspc/1/folder", path)

	path, ok = spec.MakeTablespaceSymlinkPath("/home/ismirn0ff/space1")
	assert.True(t, ok)
	assert.Equal(t, "/psql/pg_tblspc/1", path)

	// Invalid path
	path, ok = spec.MakeTablespaceSymlinkPath("/home/ismirn0ff/")
	assert.False(t, ok)

	// usual postgres path
	path, ok = spec.MakeTablespaceSymlinkPath("/psql/some_path")
	assert.True(t, ok)
	assert.Equal(t, "/psql/some_path", path)
}

func TestTablespaceLocations(t *testing.T) {
	spec := internal.TablespaceSpec{}
	tablespaceLocations := []internal.TablespaceLocation{
		{Location: "/home/ismirn0ff/space1/", Symlink: "3"},
		{Location: "/home/ismirn0ff/space2", Symlink: "1"},
		{Location: "/home/ismirn0ff/space3/", Symlink: "2"},
	}
	addTablespaces(spec, tablespaceLocations)

	marshalAndUnmarshal(t, spec)
	formatLocations(tablespaceLocations)

	returnedLocations := spec.TablespaceLocations()
	sort.Slice(returnedLocations, func(i, j int) bool {
		return returnedLocations[i].Symlink < returnedLocations[j].Symlink
	})
	sort.Slice(tablespaceLocations, func(i, j int) bool {
		return tablespaceLocations[i].Symlink < tablespaceLocations[j].Symlink
	})

	assert.Equal(t, tablespaceLocations, returnedLocations)
}
