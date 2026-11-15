package postgres

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/wal-g/wal-g/utility"
)

func TestWalkWithTablespacesFollowsSymlinkUsingLogicalPaths(t *testing.T) {
	root := t.TempDir()
	tablespace := filepath.Join(t.TempDir(), "pg_wal")
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

	wantFile := filepath.ToSlash(filepath.Join(TablespaceFolder, oid, filepath.FromSlash(relativeFile)))
	var foundRoot, foundFile bool
	err := walkWithTablespaces(root, func(walkedPath string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		logicalPath := filepath.ToSlash(utility.GetSubdirectoryRelativePath(walkedPath, root))
		if logicalPath == filepath.ToSlash(filepath.Join(TablespaceFolder, oid)) {
			foundRoot = true
			if !info.IsDir() {
				t.Errorf("tablespace root %q was not reported as a directory", logicalPath)
			}
		}
		if logicalPath == wantFile {
			foundFile = true
			if !isPagedFile(info, logicalPath) {
				t.Errorf("logical tablespace path %q was not recognized as a paged file", logicalPath)
			}
			if isPagedFile(info, physicalFile) {
				t.Errorf("physical path %q unexpectedly controls paged-file detection", physicalFile)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !foundRoot || !foundFile {
		t.Fatalf("walk missed tablespace root or payload: root=%v file=%v", foundRoot, foundFile)
	}
	if _, ok := receiveFileList(root)[wantFile]; !ok {
		t.Fatalf("receiver file list is missing logical path %q", wantFile)
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

	err := walkWithTablespaces(root, func(string, fs.FileInfo, error) error { return nil })
	if !os.IsNotExist(err) {
		t.Fatalf("walk error = %v, want a not-exist error", err)
	}
}
