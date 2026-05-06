package pax_test

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

const (
	deduplicationAgeLimit = 720 * time.Hour
	newPaxFilesID         = "test"
	privateKeyFilePath    = "../../../../test/testdata/waleGpgKey"
)

type testFile struct {
	internal.ComposeFileInfo
	pax.RelFileMetadata
	pax.FileKey
}

type expected struct {
	StoragePath string
	IsSkipped   bool
	Kind        pax.FileKind
	BlockID     int64
}

func newUploader(baseFiles pax.BackupFiles, bundleFiles internal.BundleFiles) *pax.StorageUploader {
	storage := memory.NewKVS()
	mockUploader := testtools.NewStoringMockUploader(storage)
	crypter := openpgp.CrypterFromKeyPath(privateKeyFilePath, func() (string, bool) {
		return "", false
	})
	return pax.NewStorageUploader(mockUploader, baseFiles, crypter, bundleFiles, deduplicationAgeLimit, newPaxFilesID)
}

func generateData(t *testing.T, files map[string]testFile) (string, map[string]testFile) {
	cwd, _ := filepath.Abs("./")
	dir, err := os.MkdirTemp(cwd, "data")
	assert.NoError(t, err)

	sb := testtools.NewStrideByteReader(10)
	for name, tf := range files {
		lr := &io.LimitedReader{R: sb, N: 100}
		fullPath := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(fullPath)
		assert.NoError(t, err)
		_, _ = io.Copy(f, lr)

		info, err := f.Stat()
		assert.NoError(t, err)

		header, err := tar.FileInfoHeader(info, f.Name())
		assert.NoError(t, err)
		header.Name = name

		cfi := internal.NewComposeFileInfo(f.Name(), info, false, false, header)
		tf.ComposeFileInfo = *cfi
		files[name] = tf

		defer utility.LoggedClose(f, "")
	}
	return dir, files
}

func runUpload(t *testing.T, base pax.BackupFiles, bundleFiles *internal.RegularBundleFiles,
	files map[string]testFile, want map[string]expected) {
	uploader := newUploader(base, bundleFiles)
	testDir, files := generateData(t, files)
	defer os.RemoveAll(testDir)

	for _, tf := range files {
		cfi := tf.ComposeFileInfo
		err := uploader.AddFile(&cfi, tf.RelFileMetadata, tf.FileKey)
		assert.NoError(t, err)
	}

	gotMeta := uploader.GetFiles()
	assert.Equal(t, len(want), len(gotMeta.Files))
	for name, got := range gotMeta.Files {
		exp, ok := want[name]
		assert.True(t, ok, "unexpected file %s in metadata", name)
		assert.Equal(t, exp.StoragePath, got.StoragePath, "name=%s", name)
		assert.Equal(t, exp.IsSkipped, got.IsSkipped, "name=%s", name)
		assert.Equal(t, exp.Kind, got.Kind, "name=%s", name)
		assert.Equal(t, exp.BlockID, got.BlockID, "name=%s", name)
	}
}

func TestRegularUpload_NoBaseFiles(t *testing.T) {
	files := map[string]testFile{
		"base/13/16385_pax/3": {
			RelFileMetadata: pax.RelFileMetadata{
				RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindData,
			},
			FileKey: pax.FileKey{
				SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3",
			},
		},
		"base/13/16385_pax/3.toast": {
			RelFileMetadata: pax.RelFileMetadata{
				RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindToast,
			},
			FileKey: pax.FileKey{
				SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3.toast",
			},
		},
	}
	want := map[string]expected{
		"base/13/16385_pax/3": {
			StoragePath: "1009_13_md5val_16385_3_test_pax",
			IsSkipped:   false,
			Kind:        pax.FileKindData,
			BlockID:     3,
		},
		"base/13/16385_pax/3.toast": {
			StoragePath: "1009_13_md5val_16385_3_toast_test_pax",
			IsSkipped:   false,
			Kind:        pax.FileKindToast,
			BlockID:     3,
		},
	}
	runUpload(t, pax.BackupFiles{}, &internal.RegularBundleFiles{}, files, want)
}

func TestSkipUpload_FileAlreadyInStorage(t *testing.T) {
	now := time.Now()
	base := pax.BackupFiles{
		"base/13/16385_pax/3": {
			StoragePath:     "1009_13_md5val_16385_3_old_pax",
			RelNameMd5:      "md5val",
			IsSkipped:       false,
			MTime:           now,
			Kind:            pax.FileKindData,
			BlockID:         3,
			FileMode:        420,
			InitialUploadTS: now,
		},
	}
	files := map[string]testFile{
		"base/13/16385_pax/3": {
			RelFileMetadata: pax.RelFileMetadata{
				RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindData,
			},
			FileKey: pax.FileKey{
				SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3",
			},
		},
	}
	want := map[string]expected{
		"base/13/16385_pax/3": {
			StoragePath: "1009_13_md5val_16385_3_old_pax", // reuses old storage path
			IsSkipped:   true,
			Kind:        pax.FileKindData,
			BlockID:     3,
		},
	}
	runUpload(t, base, &internal.RegularBundleFiles{}, files, want)
}

func TestRegularUpload_DedupAgeLimitPassed(t *testing.T) {
	old := time.Now().Add(-(deduplicationAgeLimit + time.Minute))
	base := pax.BackupFiles{
		"base/13/16385_pax/3": {
			StoragePath:     "1009_13_md5val_16385_3_old_pax",
			RelNameMd5:      "md5val",
			MTime:           old,
			Kind:            pax.FileKindData,
			BlockID:         3,
			FileMode:        420,
			InitialUploadTS: old,
		},
	}
	files := map[string]testFile{
		"base/13/16385_pax/3": {
			RelFileMetadata: pax.RelFileMetadata{
				RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindData,
			},
			FileKey: pax.FileKey{
				SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3",
			},
		},
	}
	want := map[string]expected{
		"base/13/16385_pax/3": {
			StoragePath: "1009_13_md5val_16385_3_test_pax",
			IsSkipped:   false,
			Kind:        pax.FileKindData,
			BlockID:     3,
		},
	}
	runUpload(t, base, &internal.RegularBundleFiles{}, files, want)
}

func TestRegularUpload_IdentityMismatch(t *testing.T) {
	now := time.Now()
	// remote shows kind=data,blockid=3; local says kind=toast,blockid=3 — mismatch -> re-upload
	base := pax.BackupFiles{
		"base/13/16385_pax/3.toast": {
			StoragePath:     "1009_13_md5val_16385_3_toast_old_pax",
			RelNameMd5:      "md5val",
			MTime:           now,
			Kind:            pax.FileKindData, // wrong on purpose
			BlockID:         3,
			FileMode:        420,
			InitialUploadTS: now,
		},
	}
	files := map[string]testFile{
		"base/13/16385_pax/3.toast": {
			RelFileMetadata: pax.RelFileMetadata{
				RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindToast,
			},
			FileKey: pax.FileKey{
				SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3.toast",
			},
		},
	}
	want := map[string]expected{
		"base/13/16385_pax/3.toast": {
			StoragePath: "1009_13_md5val_16385_3_toast_test_pax",
			IsSkipped:   false,
			Kind:        pax.FileKindToast,
			BlockID:     3,
		},
	}
	runUpload(t, base, &internal.RegularBundleFiles{}, files, want)
}

func TestUpload_FileDeletedBetweenWalkAndOpen(t *testing.T) {
	uploader := newUploader(pax.BackupFiles{}, &internal.RegularBundleFiles{})

	name := "missing_file"
	f, err := os.Create(name)
	assert.NoError(t, err)
	info, err := f.Stat()
	assert.NoError(t, err)

	header, err := tar.FileInfoHeader(info, f.Name())
	assert.NoError(t, err)
	header.Name = name

	cfi := internal.NewComposeFileInfo(f.Name(), info, false, false, header)
	_ = os.Remove(f.Name())

	err = uploader.AddFile(cfi, pax.RelFileMetadata{}, pax.FileKey{})
	assert.NoError(t, err)
	assert.Empty(t, uploader.GetFiles().Files)
}

func TestUpload_StorageKeyShape(t *testing.T) {
	// sanity check: manual key construction matches what the uploader produces.
	// Dot in filename is replaced with `_` so the storage path has no extension.
	got := pax.MakeFileStorageKey("md5val",
		pax.FileKey{SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3.toast"},
		"backup-id")
	want := fmt.Sprintf("%d_%d_%s_%d_%s_%s_pax", 1009, 13, "md5val", 16385, "3_toast", "backup-id")
	assert.Equal(t, want, got)
}
