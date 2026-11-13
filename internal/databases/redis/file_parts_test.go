package redis_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/none"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/databases/redis/ts"
	"github.com/wal-g/wal-g/internal/fileparts"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func TestRedisFilePartsBackupRestore(t *testing.T) {
	for _, kind := range []string{"aof", "ts", "attached_ts"} {
		for _, compressor := range []compression.Compressor{none.Compressor{}, lz4.Compressor{}} {
			for _, partSize := range []int64{0, 1024, 65536} {
				t.Run(fmt.Sprintf("%s/%s/partSize=%d", kind, compressor.FileExtension(), partSize), func(t *testing.T) {
					fixture := pushRedisFilePartsBackup(t, kind, compressor, partSize, false)
					var sentinel archive.Backup
					require.NoError(t, internal.FetchDto(t.Context(), fixture.folder, &sentinel,
						fixture.backupName+utility.SentinelSuffix))
					if partSize == 1024 {
						require.NotNil(t, sentinel.FileParts)
						require.Equal(t, partSize, sentinel.FileParts.PartSize)
						require.Len(t, sentinel.FileParts.Files, 2)
					} else {
						require.Nil(t, sentinel.FileParts)
					}
					fixture.checkArchives(t, partSize)
					setRedisFilePartsConfig(t, map[string]any{conf.BackupFilePartSize: 0})
					target := filepath.Join(t.TempDir(), "restored")
					require.NoError(t, fixture.restore(t.Context(), target))
					for name, expected := range fixture.files {
						actual, err := os.ReadFile(filepath.Join(target, name))
						require.NoError(t, err)
						require.Equal(t, expected, actual, name)
						info, err := os.Stat(filepath.Join(target, name))
						require.NoError(t, err)
						require.Equal(t, fixture.modes[name], info.Mode().Perm(), name)
					}
				})
			}
		}
	}
}

func TestRedisFilePartsDeferredSentinel(t *testing.T) {
	for _, kind := range []string{"aof", "attached_ts"} {
		t.Run(kind, func(t *testing.T) {
			fixture := pushRedisFilePartsBackup(t, kind, lz4.Compressor{}, 1024, true)
			exists, err := fixture.folder.Exists(t.Context(), fixture.backupName+utility.SentinelSuffix)
			require.NoError(t, err)
			require.False(t, exists)
			require.Equal(t, fixture.backupName, fixture.sentinel.BackupName)
			require.NotNil(t, fixture.sentinel.FileParts)
			var total int64
			for _, data := range fixture.files {
				total += int64(len(data))
			}
			require.Equal(t, total, fixture.sentinel.DataSize)
			require.Positive(t, fixture.sentinel.BackupSize)
			require.NoError(t, internal.UploadSentinel(t.Context(), fixture.uploader, fixture.sentinel, fixture.backupName))
			setRedisFilePartsConfig(t, map[string]any{conf.BackupFilePartSize: 0})
			target := filepath.Join(t.TempDir(), "restored")
			require.NoError(t, fixture.restore(t.Context(), target))
			for name, expected := range fixture.files {
				actual, err := os.ReadFile(filepath.Join(target, name))
				require.NoError(t, err)
				require.Equal(t, expected, actual, name)
			}
		})
	}
}

func TestRedisFilePartsRejectsIncompleteBackup(t *testing.T) {
	disableRedisFilePartsRetryWait(t)
	for _, kind := range []string{"aof", "ts", "attached_ts"} {
		for _, fault := range []string{"missing part", "missing file", "missing metadata", "invalid part size", "invalid file size"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				fixture := pushRedisFilePartsBackup(t, kind, none.Compressor{}, 1024, false)
				setRedisFilePartsConfig(t, map[string]any{
					conf.BackupFilePartSize: 0, conf.DownloadConcurrencySetting: 1, conf.DownloadFileRetriesSetting: 0,
				})
				name := "/appendonly.aof.1.base.rdb"
				if kind != "aof" {
					name = "/shard/large.data"
				}
				switch fault {
				case "missing part", "missing file":
					archives := fixture.folder.GetSubFolder(fixture.backupName + internal.TarPartitionFolderName)
					objects, _, err := archives.ListFolder(t.Context())
					require.NoError(t, err)
					var removed int
					for _, object := range objects {
						reader, err := archives.ReadObject(t.Context(), object.GetName())
						require.NoError(t, err)
						header, err := tar.NewReader(reader).Next()
						require.NoError(t, err)
						require.NoError(t, reader.Close())
						if header.Name == name && (fault == "missing file" || header.PAXRecords[fileparts.OffsetPAX] == "1024") {
							require.NoError(t, archives.DeleteObjects(t.Context(), []storage.Object{object}))
							removed++
						}
					}
					require.Positive(t, removed)
				case "missing metadata":
					fixture.sentinel.FileParts = nil
				case "invalid part size":
					fixture.sentinel.FileParts.PartSize = 0
				case "invalid file size":
					fixture.sentinel.FileParts.Files[name]++
				}
				require.NoError(t, internal.UploadSentinel(t.Context(), fixture.uploader, fixture.sentinel, fixture.backupName))
				err := fixture.restore(t.Context(), filepath.Join(t.TempDir(), "restored"))
				require.Error(t, err)
				if strings.HasPrefix(fault, "missing ") && fault != "missing metadata" {
					require.ErrorContains(t, err, name)
					require.ErrorContains(t, err, "missing")
				}
			})
		}
	}
}

func TestRedisFilePartsRestoreRetry(t *testing.T) {
	for _, kind := range []string{"aof", "ts"} {
		t.Run(kind, func(t *testing.T) {
			fixture := pushRedisFilePartsBackup(t, kind, none.Compressor{}, 1024, false)
			disableRedisFilePartsRetryWait(t)
			state := &redisFilePartsReadFault{reads: make(map[string]int)}
			fixture.folder = &redisFilePartsFaultFolder{Folder: fixture.folder, fault: state}
			fixture.uploader = internal.NewRegularUploader(none.Compressor{}, fixture.folder)
			target := filepath.Join(t.TempDir(), "restored")
			require.NoError(t, fixture.restore(t.Context(), target))
			require.NotEmpty(t, state.failed)
			for name, reads := range state.reads {
				expected := 1
				if name == state.failed {
					expected = 2
				}
				require.Equal(t, expected, reads, name)
			}
			for name, expected := range fixture.files {
				actual, err := os.ReadFile(filepath.Join(target, name))
				require.NoError(t, err)
				require.Equal(t, expected, actual, name)
			}
		})
	}
}

type redisFilePartsReadFault struct {
	mutex  sync.Mutex
	reads  map[string]int
	failed string
}

type redisFilePartsFaultFolder struct {
	storage.Folder
	fault *redisFilePartsReadFault
}

func (folder *redisFilePartsFaultFolder) GetSubFolder(name string) storage.Folder {
	return &redisFilePartsFaultFolder{Folder: folder.Folder.GetSubFolder(name), fault: folder.fault}
}

func (folder *redisFilePartsFaultFolder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := folder.Folder.ReadObject(ctx, name)
	if err != nil || !strings.HasSuffix(name, ".tar") {
		return reader, err
	}
	data, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	raw := bytes.NewReader(data)
	header, err := tar.NewReader(raw).Next()
	if err != nil {
		return nil, err
	}
	folder.fault.mutex.Lock()
	defer folder.fault.mutex.Unlock()
	folder.fault.reads[name]++
	if header.PAXRecords[fileparts.OffsetPAX] == "1024" && folder.fault.failed == "" {
		folder.fault.failed = name
		data = data[:len(data)-raw.Len()+int(header.Size)/2]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type redisFilePartsFixture struct {
	folder     storage.Folder
	rootFolder storage.Folder
	uploader   internal.Uploader
	backupName string
	kind       string
	files      map[string][]byte
	modes      map[string]os.FileMode
	sentinel   *archive.Backup
}

func pushRedisFilePartsBackup(t *testing.T, kind string, compressor compression.Compressor,
	partSize int64, deferSentinel bool,
) *redisFilePartsFixture {
	t.Helper()
	source := t.TempDir()
	rootFolder := fs.NewFolder(t.TempDir(), "")
	folder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	uploader := internal.NewRegularUploader(compressor, folder)
	setRedisFilePartsConfig(t, map[string]any{
		conf.BackupFilePartSize:           partSize,
		conf.UploadDiskConcurrencySetting: 3,
		conf.UploadQueueSetting:           2,
		conf.DownloadConcurrencySetting:   4,
		conf.TarSizeThresholdSetting:      1536,
		conf.TarDisableFsyncSetting:       true,
		conf.RedisDataThreshold:           100,
		conf.RedisDataTimeout:             1,
		conf.RedisDataPath:                source,
		conf.RedisAppendonlyFolder:        "appendonlydir",
		conf.RedisAppendonlyTmpFolder:     t.TempDir(),
		conf.RedisAppendonlyManifest:      "appendonly.aof.manifest",
	})
	fixture := &redisFilePartsFixture{
		folder: folder, rootFolder: rootFolder, uploader: uploader, kind: kind,
		backupName: "aof_20261109T000000Z",
		files: map[string][]byte{
			"appendonly.aof.1.base.rdb": redisFilePartsData(11*1024 + 17),
			"appendonly.aof.1.incr.aof": redisFilePartsData(5 * 1024),
			"appendonly.aof.2.incr.aof": {},
			"appendonly.aof.manifest": []byte("file appendonly.aof.1.base.rdb seq 1 type b\n" +
				"file appendonly.aof.1.incr.aof seq 1 type i\nfile appendonly.aof.2.incr.aof seq 2 type i\n"),
		},
		modes: make(map[string]os.FileMode),
	}
	if kind == "aof" {
		source = filepath.Join(source, "appendonlydir")
	} else {
		fixture.backupName = "ts_20261109T000000Z"
		if kind == "attached_ts" {
			fixture.backupName = archive.AttachedTSDataPrefix("aof_20261109T000000Z")
		}
		fixture.files = map[string][]byte{
			"shard/large.data": redisFilePartsData(11*1024 + 17),
			"other/index":      redisFilePartsData(5 * 1024),
			"at-threshold":     redisFilePartsData(1024),
			"manifest":         []byte("ts metadata"),
			"empty":            {},
		}
	}
	for name, data := range fixture.files {
		filename := filepath.Join(source, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0o700))
		mode := os.FileMode(0o600)
		if len(data) > 1024 {
			mode = 0o400
		} else if name == "appendonly.aof.manifest" {
			mode = 0o644
		}
		require.NoError(t, os.WriteFile(filename, data, mode))
		info, err := os.Stat(filename)
		require.NoError(t, err)
		fixture.modes[name] = info.Mode().Perm()
	}
	if kind == "aof" {
		meta := &redisFilePartsMeta{}
		require.NoError(t, redis.HandleAOFBackupPush(t.Context(), redis.AOFBackupPushArgs{
			BackupName: fixture.backupName, Uploader: uploader, MetaConstructor: meta, DeferSentinel: deferSentinel,
		}))
		fixture.sentinel = meta.MetaInfo().(*archive.Backup)
	} else {
		backup, err := ts.Push(t.Context(), ts.PushArgs{
			Uploader: uploader, SourceDir: source, PinFolder: t.TempDir(), DataPrefix: fixture.backupName,
			BackupID: "frozen-tree", DeferSentinel: deferSentinel,
		})
		require.NoError(t, err)
		fixture.sentinel = backup
	}
	return fixture
}

func (fixture *redisFilePartsFixture) restore(ctx context.Context, target string) error {
	if fixture.kind == "aof" {
		viper.Set(conf.RedisDataPath, filepath.Dir(target))
		viper.Set(conf.RedisAppendonlyFolder, filepath.Base(target))
		return redis.HandleAofFetchPush(ctx, fixture.rootFolder, fixture.uploader, fixture.backupName, "7.2.5", false, true)
	}
	return ts.Fetch(ctx, ts.FetchArgs{Folder: fixture.folder, DataPrefix: fixture.backupName, TargetDir: target})
}

func (fixture *redisFilePartsFixture) checkArchives(t *testing.T, partSize int64) {
	t.Helper()
	archives := fixture.folder.GetSubFolder(fixture.backupName + internal.TarPartitionFolderName)
	objects, _, err := archives.ListFolder(t.Context())
	require.NoError(t, err)
	counts := make(map[string]int)
	for _, object := range objects {
		stream, err := archives.ReadObject(t.Context(), object.GetName())
		require.NoError(t, err)
		decoded, err := internal.DecryptAndDecompressTar(t.Context(), stream, object.GetName(), nil)
		require.NoError(t, err)
		reader := tar.NewReader(decoded)
		entries, parts := 0, 0
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			entries++
			name := strings.TrimPrefix(header.Name, "/")
			data, ok := fixture.files[name]
			require.True(t, ok, name)
			counts[name]++
			contents, err := io.ReadAll(reader)
			require.NoError(t, err)
			if offsetString, split := header.PAXRecords[fileparts.OffsetPAX]; split {
				parts++
				require.Positive(t, partSize)
				require.Greater(t, int64(len(data)), partSize)
				offset, err := strconv.ParseInt(offsetString, 10, 64)
				require.NoError(t, err)
				require.Equal(t, min(partSize, int64(len(data))-offset), header.Size)
				require.Equal(t, strconv.Itoa(len(data)), header.PAXRecords[fileparts.FileSizePAX])
				require.Equal(t, data[offset:offset+header.Size], contents)
			} else {
				require.Equal(t, data, contents)
			}
		}
		if parts > 0 {
			require.Equal(t, 1, entries, "each part must have its own archive")
		}
		require.NoError(t, decoded.Close())
		require.NoError(t, stream.Close())
	}
	for name, data := range fixture.files {
		expected := 1
		if partSize > 0 && int64(len(data)) > partSize {
			expected = int((int64(len(data)) + partSize - 1) / partSize)
		}
		require.Equal(t, expected, counts[name], name)
	}
}

type redisFilePartsMeta struct {
	archive.RedisMetaConstructor
}

func (meta *redisFilePartsMeta) Init(context.Context) error {
	backup := meta.MetaInfo().(*archive.Backup)
	backup.BackupType = archive.AOFBackupType
	backup.Version = "7.2.5"
	return nil
}

func setRedisFilePartsConfig(t *testing.T, settings map[string]any) {
	t.Helper()
	for key, value := range settings {
		previous := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, previous) })
	}
}

func disableRedisFilePartsRetryWait(t *testing.T) {
	t.Helper()
	oldMin, oldMax := internal.MinExtractRetryWait, internal.MaxExtractRetryWait
	internal.MinExtractRetryWait, internal.MaxExtractRetryWait = 0, 0
	t.Cleanup(func() { internal.MinExtractRetryWait, internal.MaxExtractRetryWait = oldMin, oldMax })
}

func redisFilePartsData(size int) []byte {
	return bytes.Repeat([]byte("0123456789abcdef"), (size+15)/16)[:size]
}
