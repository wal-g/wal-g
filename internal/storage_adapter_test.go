package internal

import (
	"bufio"
	"context"
	"go/build/constraint"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func fakeStorageAdapter(storageType string) StorageAdapter {
	return StorageAdapter{
		storageType: storageType,
		configure: func(context.Context, string, map[string]string, ...storage.WrapRootFolder) (storage.HashableStorage, error) {
			return memory.NewStorage("", memory.NewKVS()), nil
		},
	}
}

// setStorageAdapters replaces the compiled-in storages for the duration of the test.
func setStorageAdapters(t *testing.T, adapters ...StorageAdapter) {
	saved := StorageAdapters
	StorageAdapters = adapters
	t.Cleanup(func() { StorageAdapters = saved })
}

func readBuildConstraint(t *testing.T, path string) constraint.Expr {
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "package ") {
			break
		}
		if constraint.IsGoBuild(line) {
			expr, err := constraint.Parse(line)
			require.NoError(t, err)
			return expr
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatalf("%s has no //go:build constraint", path)
	return nil
}

// TestStorageAdapterBuildConstraints checks every combination of storage_* build tags:
// with no tags all storages are built, otherwise exactly the selected ones.
func TestStorageAdapterBuildConstraints(t *testing.T) {
	files, err := filepath.Glob("storage_adapter_*.go")
	require.NoError(t, err)

	fileTags := make(map[string]string)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(file, "storage_adapter_"), ".go")
		fileTags[file] = "storage_" + name
	}

	tags := make([]string, 0, len(knownStorages))
	for _, s := range knownStorages {
		assert.Contains(t, fileTags, "storage_adapter_"+strings.TrimPrefix(s.buildTag, "storage_")+".go",
			"no adapter file for %s storage", s.storageType)
		tags = append(tags, s.buildTag)
	}
	require.Len(t, fileTags, len(knownStorages), "every storage_adapter_<name>.go must be listed in knownStorages")

	for file, fileTag := range fileTags {
		expr := readBuildConstraint(t, file)
		for mask := 0; mask < 1<<len(tags); mask++ {
			selected := make(map[string]bool)
			for i, tag := range tags {
				if mask&(1<<i) != 0 {
					selected[tag] = true
				}
			}
			built := expr.Eval(func(tag string) bool { return selected[tag] })
			assert.Equal(t, len(selected) == 0 || selected[fileTag], built, "%s with tags %v", file, selected)
		}
	}
}

func TestStorageAdaptersFollowKnownStoragesOrder(t *testing.T) {
	require.NotEmpty(t, StorageAdapters)
	for i := 1; i < len(StorageAdapters); i++ {
		assert.Less(t, knownStorageIndex(StorageAdapters[i-1].storageType), knownStorageIndex(StorageAdapters[i].storageType))
	}
}

func TestRegisterStorageAdapter_KeepsKnownStoragesOrder(t *testing.T) {
	setStorageAdapters(t)

	for i := len(knownStorages) - 1; i >= 0; i-- {
		registerStorageAdapter(fakeStorageAdapter(knownStorages[i].storageType))
	}

	registered := make([]string, 0, len(StorageAdapters))
	for _, adapter := range StorageAdapters {
		registered = append(registered, adapter.storageType)
	}
	expected := make([]string, 0, len(knownStorages))
	for _, s := range knownStorages {
		expected = append(expected, s.storageType)
	}
	assert.Equal(t, expected, registered)
}

func TestRegisterStorageAdapter_PanicsOnUnknownStorage(t *testing.T) {
	setStorageAdapters(t)

	assert.Panics(t, func() { registerStorageAdapter(fakeStorageAdapter("UNKNOWN")) })
}

func TestConfigureStorageForSpecificConfig_StorageNotCompiled(t *testing.T) {
	setStorageAdapters(t, fakeStorageAdapter("S3"))
	config := viper.New()
	config.Set("WALG_GS_PREFIX", "gs://bucket/path")

	st, err := ConfigureStorageForSpecificConfig(t.Context(), config)

	assert.Nil(t, st)
	assert.IsType(t, StorageNotCompiledError{}, err)
	assert.ErrorContains(t, err, "WALG_GS_PREFIX")
	assert.ErrorContains(t, err, "storage_gcs")
}

func TestConfigureStorageForSpecificConfig_CompiledStorageWins(t *testing.T) {
	setStorageAdapters(t, fakeStorageAdapter("FILE"))
	config := viper.New()
	config.Set("WALG_GS_PREFIX", "gs://bucket/path")
	config.Set("WALG_FILE_PREFIX", "/tmp")

	st, err := ConfigureStorageForSpecificConfig(t.Context(), config)

	assert.NoError(t, err)
	assert.NotNil(t, st)
}

func TestConfigureStorageForSpecificConfig_NothingConfigured(t *testing.T) {
	setStorageAdapters(t, fakeStorageAdapter("S3"))

	_, err := ConfigureStorageForSpecificConfig(t.Context(), viper.New())

	assert.IsType(t, UnconfiguredStorageError{}, err)
}

func TestAssertRequiredSettingsSet_StorageNotCompiled(t *testing.T) {
	setStorageAdapters(t, fakeStorageAdapter("S3"))
	resetToDefaults()
	defer resetToDefaults()
	viper.Set("WALG_AZ_PREFIX", "azure://container/path")

	err := AssertRequiredSettingsSet()

	assert.IsType(t, StorageNotCompiledError{}, err)
	assert.ErrorContains(t, err, "storage_azure")
}
