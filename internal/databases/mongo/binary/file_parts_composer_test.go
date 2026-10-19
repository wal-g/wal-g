package binary

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/none"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestFilePartsBackupRestore(t *testing.T) {
	for _, compressor := range []compression.Compressor{none.Compressor{}, lz4.Compressor{}} {
		for _, split := range []bool{false, true} {
			t.Run(fmt.Sprintf("compression=%s/split=%t", compressor.FileExtension(), split), func(t *testing.T) {
				const partSize = 1024
				const backupName = "binary_20261019T000000Z"
				setFilePartsTestConfig(t, map[string]any{
					conf.MongoDBBackupFilePartSize:    0,
					conf.UploadDiskConcurrencySetting: 3,
					conf.UploadQueueSetting:           2,
					conf.DownloadConcurrencySetting:   4,
					conf.TarSizeThresholdSetting:      1536,
					conf.TarDisableFsyncSetting:       true,
				})
				if split {
					viper.Set(conf.MongoDBBackupFilePartSize, partSize)
				}
				files := map[string][]byte{
					"db/collection-1.wt": filePartsTestData(partSize*11 + 17),
					"db/index-1.wt":      filePartsTestData(partSize * 5),
					"db/index-small.wt":  filePartsTestData(partSize),
					"db/collection-2.wt": filePartsTestData(partSize*3 + 1),
					"WiredTiger":         filePartsTestData(partSize*2 + 7),
				}
				modes := map[string]os.FileMode{"db/index-1.wt": 0400}
				source := t.TempDir()
				folder := fs.NewFolder(t.TempDir(), "")
				uploader := internal.NewRegularUploader(compressor, folder)
				maker := NewDirDatabaseTarBallComposerMaker()
				concurrent, err := internal.CreateConcurrentUploader(t.Context(), internal.CreateConcurrentUploaderArgs{
					Uploader: uploader, BackupName: backupName, Directory: source, TarBallComposerMaker: maker,
				})
				require.NoError(t, err)
				for name, data := range files {
					filename := filepath.Join(source, name)
					require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0755))
					mode, ok := modes[name]
					if !ok {
						mode = 0600
					}
					require.NoError(t, os.WriteFile(filename, data, mode))
					meta, err := internal.GetBackupFileMeta(filename)
					require.NoError(t, err)
					require.NoError(t, concurrent.Upload(meta))
				}
				sets, err := concurrent.Finalize()
				require.NoError(t, err)
				assert.Equal(t, int64(totalFilePartsTestSize(files)), concurrent.UncompressedSize)

				collector := NewPartialMetadataCollector()
				collector.HandleNsInfo(makeNsInfo("testdb.selected", "statistics:table:db/collection-1", map[string]string{
					"_id_": "statistics:table:db/index-1", "small": "statistics:table:db/index-small",
				}))
				collector.HandleNsInfo(makeNsInfo("testdb.excluded", "statistics:table:db/collection-2", nil))
				require.NoError(t, collector.EnrichWithTarPaths(sets.Get()))
				require.NoError(t, internal.UploadMetadata(t.Context(), uploader, collector.GetRoutes(), backupName))
				require.NoError(t, internal.UploadSentinel(t.Context(), uploader, &models.Backup{
					BackupName: backupName, BackupType: "binary", FileParts: maker.fileParts,
				}, backupName))

				archiveFolder := folder.GetSubFolder(backupName + internal.TarPartitionFolderName)
				objects, subfolders, err := archiveFolder.ListFolder(t.Context())
				require.NoError(t, err)
				require.Empty(t, subfolders)
				entries := make(map[string]int)
				for _, object := range objects {
					stream, err := archiveFolder.ReadObject(t.Context(), object.GetName())
					require.NoError(t, err)
					decoded, err := internal.DecryptAndDecompressTar(t.Context(), stream, object.GetName(), nil)
					require.NoError(t, err)
					reader := tar.NewReader(decoded)
					partEntries := 0
					archiveEntries := 0
					for {
						header, err := reader.Next()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						archiveEntries++
						name := strings.TrimPrefix(header.Name, "/")
						original, exists := files[name]
						require.True(t, exists, "unexpected archive member %s", header.Name)
						entries[name]++
						contents, err := io.ReadAll(reader)
						require.NoError(t, err)
						if offsetString, isPart := header.PAXRecords[filePartOffsetPAX]; isPart {
							partEntries++
							require.True(t, split)
							require.LessOrEqual(t, header.Size, int64(partSize))
							offset, err := strconv.ParseInt(offsetString, 10, 64)
							require.NoError(t, err)
							require.Equal(t, original[offset:offset+header.Size], contents)
						} else {
							require.Equal(t, original, contents)
						}
					}
					require.LessOrEqual(t, partEntries, 1, "each part must have its own archive")
					if partEntries > 0 {
						require.Equal(t, 1, archiveEntries, "part archives must not contain other files")
					}
					require.NoError(t, decoded.Close())
					require.NoError(t, stream.Close())
				}
				if split {
					require.Len(t, maker.fileParts, 3)
					require.Greater(t, entries["db/collection-1.wt"], 1)
					require.Greater(t, entries["db/index-1.wt"], 1)
					require.Greater(t, entries["db/collection-2.wt"], 1)
				} else {
					require.Empty(t, maker.fileParts)
					for name := range files {
						require.Equal(t, 1, entries[name])
					}
				}
				require.Equal(t, 1, entries["db/index-small.wt"])
				require.Equal(t, 1, entries["WiredTiger"])

				for _, partial := range []bool{false, true} {
					t.Run(fmt.Sprintf("partial=%t", partial), func(t *testing.T) {
						restored := t.TempDir()
						service := &RestoreService{Uploader: uploader, LocalStorage: CreateLocalStorage(restored)}
						args := RestoreArgs{BackupName: backupName, SkipChecks: true, SkipMongoReconfig: true}
						if partial {
							args.Whitelist = []string{"testdb.selected"}
						}
						require.NoError(t, service.DoRestore(t.Context(), RsConfig{}, ShConfig{}, MongoCfgConfig{}, ReplyOplogConfig{}, args))
						for name, original := range files {
							filename := filepath.Join(restored, name)
							if partial && name == "db/collection-2.wt" {
								require.NoFileExists(t, filename)
								continue
							}
							contents, err := os.ReadFile(filename)
							require.NoError(t, err)
							require.True(t, bytes.Equal(original, contents), "restored bytes differ for %s", name)
							info, err := os.Stat(filename)
							require.NoError(t, err)
							mode, ok := modes[name]
							if !ok {
								mode = 0600
							}
							require.Equal(t, mode, info.Mode().Perm())
						}
					})
				}
				if split {
					var excludedArchives []string
					for archive, names := range sets.Get() {
						for _, name := range names {
							if name == "/db/collection-2.wt" {
								excludedArchives = append(excludedArchives, archive)
							}
						}
					}
					require.Greater(t, len(excludedArchives), 1)
					for _, missing := range []string{"one part", "entire file"} {
						t.Run("missing "+missing, func(t *testing.T) {
							for index, archive := range excludedArchives {
								if missing == "one part" && index != 0 {
									continue
								}
								for _, object := range objects {
									if object.GetName() == archive {
										require.NoError(t, archiveFolder.DeleteObjects(t.Context(), []storage.Object{object}))
									}
								}
							}
							args := RestoreArgs{BackupName: backupName, SkipChecks: true, SkipMongoReconfig: true}
							service := &RestoreService{Uploader: uploader, LocalStorage: CreateLocalStorage(t.TempDir())}
							err := service.DoRestore(t.Context(), RsConfig{}, ShConfig{}, MongoCfgConfig{}, ReplyOplogConfig{}, args)
							require.ErrorContains(t, err, "/db/collection-2.wt")
							require.ErrorContains(t, err, "missing")

							args.Whitelist = []string{"testdb.selected"}
							service.LocalStorage = CreateLocalStorage(t.TempDir())
							require.NoError(t, service.DoRestore(t.Context(), RsConfig{}, ShConfig{}, MongoCfgConfig{}, ReplyOplogConfig{}, args))
						})
					}
				}
			})
		}
	}
}

func TestFilePartsComposerConfiguration(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   any
		split   bool
		invalid bool
	}{
		{name: "unset"},
		{name: "disabled", value: "0"},
		{name: "enabled", value: "1024", split: true},
		{name: "negative", value: "-1", invalid: true},
		{name: "noninteger", value: "small", invalid: true},
		{name: "fractional", value: "1.5", invalid: true},
		{name: "overflow", value: "9223372036854775808", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			setFilePartsTestConfig(t, map[string]any{conf.MongoDBBackupFilePartSize: test.value})
			maker := NewDirDatabaseTarBallComposerMaker()
			composer, err := maker.Make(t.Context(), internal.NewBundle(t.TempDir(), nil, 1024, nil))
			if test.invalid {
				require.ErrorContains(t, err, conf.MongoDBBackupFilePartSize)
				return
			}
			require.NoError(t, err)
			_, splitsFiles := composer.(*filePartsTarBallComposer)
			require.Equal(t, test.split, splitsFiles)
		})
	}
}

func setFilePartsTestConfig(t *testing.T, settings map[string]any) {
	t.Helper()
	for name, value := range settings {
		previous := viper.Get(name)
		viper.Set(name, value)
		t.Cleanup(func() { viper.Set(name, previous) })
	}
}

func filePartsTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + i/97) % 251)
	}
	return data
}

func totalFilePartsTestSize(files map[string][]byte) int {
	total := 0
	for _, data := range files {
		total += len(data)
	}
	return total
}
