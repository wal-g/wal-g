package greenplum

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const recoveryMappingPath = "delete_recovery/greenplum/"

func pendingRecoveryMappings(ctx context.Context, root storage.Folder) ([]string, error) {
	objects, _, err := root.GetSubFolder(recoveryMappingPath).ListFolder(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, object := range objects {
		key := strings.TrimLeft(object.GetName(), "/")
		if strings.HasSuffix(key, ".json") {
			name := strings.TrimSuffix(key, ".json")
			if err := internal.ValidateRecoveryName(name); err != nil {
				return nil, err
			}
			names = append(names, name)
		}
	}
	return names, nil
}

func readClusterDeletionMetadata(ctx context.Context, root storage.Folder, name string,
	sentinel *BackupSentinelDto) (internal.DeletionMetadata, error) {
	doc, err := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(utility.BaseBackupPath),
		name+utility.SentinelSuffix, sentinel)
	var missing storage.ObjectNotFoundError
	if err != nil || !errors.As(doc.Problem, &missing) {
		return doc, err
	}
	retry, err := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(recoveryMappingPath), name+".json", sentinel)
	if errors.As(retry.Problem, &missing) {
		return doc, err
	}
	if err != nil || retry.Problem != nil {
		return retry, errors.Join(err, retry.Problem)
	}
	var version int
	var recordedName string
	if json.Unmarshal(retry.Fields["recovery_version"], &version) != nil || version != 1 ||
		json.Unmarshal(retry.Fields["recovery_backup_name"], &recordedName) != nil || recordedName != name {
		return retry, fmt.Errorf("invalid recovery mapping for %s", name)
	}
	return retry, nil
}

func (g *greenplumDeletionPlanner) prepareRetryMappings(ctx context.Context) error {
	folder := g.plan.Root.GetSubFolder(recoveryMappingPath)
	for _, name := range g.plan.TargetNames() {
		var sentinel BackupSentinelDto
		doc, err := readClusterDeletionMetadata(ctx, g.plan.Root, name, &sentinel)
		if err != nil {
			return err
		}
		if doc.Problem != nil {
			continue
		}
		data, err := retryMappingDocument(doc, name, g.mappings[name])
		if err != nil {
			return err
		}

		g.plan.Details = append(g.plan.Details, "save retry mapping before removing cluster sentinel "+name)
		g.plan.Before = append(g.plan.Before, func(ctx context.Context) error {
			return saveRetryMapping(ctx, folder, name+".json", data)
		})
	}
	return nil
}

func retryMappingDocument(doc internal.DeletionMetadata, name string, mapping map[int]string) ([]byte, error) {
	var segments []map[string]jsontext.Value
	if raw, ok := doc.Fields["segments"]; ok {
		if err := json.Unmarshal(raw, &segments); err != nil {
			return nil, err
		}
	}
	for _, segment := range segments {
		var id int
		if err := json.Unmarshal(segment["content_id"], &id); err != nil {
			return nil, err
		}
		resolved, ok := mapping[id]
		if !ok {
			continue
		}
		raw, err := json.Marshal(resolved)
		if err != nil {
			return nil, err
		}
		segment["backup_name"] = raw
	}
	raw, err := json.Marshal(segments, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	doc.Fields["segments"] = raw
	doc.Fields["recovery_version"] = []byte("1")
	doc.Fields["recovery_backup_name"], err = json.Marshal(name)
	if err != nil {
		return nil, err
	}
	return json.Marshal(doc.Fields, json.Deterministic(true))
}

func saveRetryMapping(ctx context.Context, base storage.Folder, key string, data []byte) error {
	reader, err := base.ReadObject(ctx, key)
	if err == nil {
		saved, err := readRetryMapping(reader)
		if err != nil {
			return err
		}
		if bytes.Equal(saved, data) {
			return nil
		}
		if !isRetryMappingEnrichment(saved, data) {
			return fmt.Errorf("conflicting or unconfirmed recovery mapping: %s", key)
		}
	} else {
		var missing storage.ObjectNotFoundError
		if !errors.As(err, &missing) {
			return err
		}
	}
	if err := base.PutObject(ctx, key, bytes.NewReader(data)); err != nil {
		return err
	}
	reader, err = base.ReadObject(ctx, key)
	if err != nil {
		return err
	}
	return verifyRetryMapping(reader, key, data)
}

func isRetryMappingEnrichment(saved, data []byte) bool {
	var oldDoc, newDoc map[string]jsontext.Value
	if json.Unmarshal(saved, &oldDoc) != nil || json.Unmarshal(data, &newDoc) != nil {
		return false
	}
	var oldSegments, newSegments []map[string]jsontext.Value
	if json.Unmarshal(oldDoc["segments"], &oldSegments) != nil ||
		json.Unmarshal(newDoc["segments"], &newSegments) != nil || len(oldSegments) != len(newSegments) {
		return false
	}
	for i, oldSegment := range oldSegments {
		oldRaw, present := oldSegment["backup_name"]
		oldName, err := retryMappingBackupName(oldRaw)
		if err != nil {
			return false
		}
		newName, err := retryMappingBackupName(newSegments[i]["backup_name"])
		if err != nil {
			return false
		}
		if oldName != "" || newName == "" {
			continue
		}
		if internal.ValidateRecoveryName(newName) != nil || !strings.HasPrefix(newName, "base_") {
			return false
		}
		if present {
			newSegments[i]["backup_name"] = oldRaw
		} else {
			delete(newSegments[i], "backup_name")
		}
	}
	oldDoc["segments"], _ = json.Marshal(oldSegments, json.Deterministic(true))
	newDoc["segments"], _ = json.Marshal(newSegments, json.Deterministic(true))
	oldNormalized, _ := json.Marshal(oldDoc, json.Deterministic(true))
	newNormalized, _ := json.Marshal(newDoc, json.Deterministic(true))
	return bytes.Equal(oldNormalized, newNormalized)
}

func retryMappingBackupName(raw jsontext.Value) (string, error) {
	var name string
	var err error
	if len(raw) != 0 {
		err = json.Unmarshal(raw, &name)
	}
	return name, err
}

func readRetryMapping(reader io.ReadCloser) ([]byte, error) {
	saved, readErr := io.ReadAll(reader)
	return saved, errors.Join(readErr, reader.Close())
}

func verifyRetryMapping(reader io.ReadCloser, key string, data []byte) error {
	saved, err := readRetryMapping(reader)
	if err != nil {
		return err
	}
	if !bytes.Equal(saved, data) {
		return fmt.Errorf("conflicting or unconfirmed recovery mapping: %s", key)
	}
	return nil
}

func (g *greenplumDeletionPlanner) finalizeRetryMappings(ctx context.Context) error {
	folder := storage.WithAllVersions(g.plan.Root.GetSubFolder(recoveryMappingPath))
	selected := func(object storage.Object) bool {
		key := strings.TrimLeft(object.GetName(), "/")
		return strings.HasSuffix(key, ".json") && g.plan.Selected[strings.TrimSuffix(key, ".json")]
	}
	final := &internal.DeletionPlan{Root: g.plan.Root, StorageName: g.plan.StorageName, Selected: g.plan.Selected}
	defer final.Close()
	objects, _, err := folder.ListFolder(ctx)
	if err != nil {
		return err
	}
	names := g.plan.TargetNames()
	depths := map[string]int{}
	var depth func(string) int
	depth = func(name string) int {
		if !g.plan.Selected[name] {
			return 0
		}
		if depths[name] == 0 {
			depths[name] = 1 + depth(g.plan.Backups[name].Parent)
		}
		return depths[name]
	}
	sort.SliceStable(names, func(i, j int) bool { return depth(names[i]) > depth(names[j]) })
	for _, name := range names {
		stage, err := final.NewStage("completed recovery mapping "+name, folder)
		if err != nil {
			return err
		}
		for _, object := range objects {
			if strings.TrimLeft(object.GetName(), "/") == name+".json" {
				if err := stage.Add(object); err != nil {
					return err
				}
			}
		}
	}
	if err := final.Execute(ctx, true); err != nil {
		return err
	}
	return storage.WalkObjects(ctx, folder, func(object storage.Object) error {
		if selected(object) {
			return fmt.Errorf("recovery mapping deletion unconfirmed: %s", object.GetName())
		}
		return nil
	})
}
