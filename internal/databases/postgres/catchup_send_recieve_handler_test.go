package postgres

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWalkWithTablespacesFollowsSymlinkUsingLogicalPaths(t *testing.T) {
	root := t.TempDir()
	tablespace := filepath.Join(t.TempDir(), "pg_wal") // excluded physical basename
	const oid = "3690787"
	const relativeFile = "PG_17_202406281/16384/12345"

	physicalFile := filepath.Join(tablespace, filepath.FromSlash(relativeFile))
	if err := os.MkdirAll(filepath.Dir(physicalFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(physicalFile, make([]byte, DatabasePageSize), 0600); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(root, TablespaceFolder, oid)
	if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(tablespace, link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	wantRoot := filepath.ToSlash(filepath.Join(TablespaceFolder, oid))
	wantFile := wantRoot + "/" + relativeFile

	var foundRoot bool
	fileVisits := 0

	err := walkWithTablespaces(root, func(walkedPath string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		relPath, err := filepath.Rel(root, walkedPath)
		if err != nil {
			return err
		}
		logicalPath := filepath.ToSlash(relPath)

		if logicalPath == wantRoot {
			foundRoot = true
			if !info.IsDir() {
				t.Errorf("tablespace root %q was not reported as a directory", logicalPath)
			}
		}

		if logicalPath == wantFile {
			fileVisits++
			if !isPagedFile(info, logicalPath) {
				t.Errorf("logical tablespace path %q was not recognized as a paged file", logicalPath)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !foundRoot {
		t.Errorf("walk missed tablespace root %q", wantRoot)
	}
	if fileVisits != 1 {
		t.Errorf("tablespace file %q visited %d times, want 1", wantFile, fileVisits)
	}

	found := false
	for k := range receiveFileList(root) {
		if strings.TrimPrefix(k, "/") == wantFile {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("receiver file list is missing logical path %q", wantFile)
	}
}

func TestWalkWithTablespacesFailsWhenTargetIsMissing(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, TablespaceFolder, "3690787")

	if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing-tablespace"), link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	err := walkWithTablespaces(root, func(string, fs.FileInfo, error) error {
		return nil
	})

	if !os.IsNotExist(err) {
		t.Fatalf("walk error = %v, want a not-exist error", err)
	}
}