package greenplum

import (
	"context"
	"errors"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const deltaBackupNameDelimiter = "_D_"

// DeleteSentinelOnlyHandler implements "delete target --sentinel-only". It deletes only the cluster sentinels
// of the target backup and of its incremental descendants, so the backups disappear from the backup list.
// Segment backups and the other leftovers are removed later by "delete garbage" or by retention.
//
// The metadata of unrelated backups is never read, so damaged backups do not block the deletion:
// the dependencies are derived from the backup names, and only the sentinels being deleted are read
// to refuse deleting a permanent backup.
type DeleteSentinelOnlyHandler struct {
	internal.DeleteHandler
	args DeleteArgs
}

func NewDeleteSentinelOnlyHandler(ctx context.Context, folder storage.Folder, args DeleteArgs,
) (*DeleteSentinelOnlyHandler, error) {
	sentinels, err := internal.GetBackupSentinelObjects(ctx, folder)
	if err != nil {
		return nil, err
	}
	backups := makeBackupObjectsFromNames(sentinels)
	permanence := &sentinelPermanence{
		ctx:     ctx,
		folder:  folder.GetSubFolder(utility.BaseBackupPath),
		checked: make(map[string]bool),
	}
	// Same ordering as in NewDeleteHandler; DeleteTarget does not use it.
	lessFunc := func(obj1, obj2 storage.Object) bool {
		return obj1.GetLastModified().Before(obj2.GetLastModified())
	}
	return &DeleteSentinelOnlyHandler{
		DeleteHandler: *internal.NewDeleteHandler(folder, backups, lessFunc,
			internal.IsPermanentFunc(permanence.isPermanent)),
		args: args,
	}, nil
}

func (h *DeleteSentinelOnlyHandler) HandleDeleteTarget(ctx context.Context, targetSelector internal.BackupSelector) {
	err := h.deleteTargetSentinels(ctx, targetSelector)
	tracelog.ErrorLogger.FatalOnError(err)
	if h.args.Confirmed {
		tracelog.InfoLogger.Println("Run 'delete garbage' to clean up the remaining objects of the deleted backups")
	}
}

func (h *DeleteSentinelOnlyHandler) deleteTargetSentinels(ctx context.Context, targetSelector internal.BackupSelector) error {
	target, err := h.FindTargetBySelector(ctx, targetSelector)
	if err != nil {
		return err
	}
	if target == nil {
		return errors.New("requested backup was not found")
	}
	// Do not descend into the backup folders: only the cluster sentinels at the top level are deleted.
	topLevelOnly := func(string) bool { return false }
	return h.DeleteTarget(ctx, target, h.args.Confirmed, h.args.FindFull, topLevelOnly)
}

// makeBackupObjectsFromNames builds the backup objects without reading the sentinels:
// the dependencies between the backups are derived from their names.
func makeBackupObjectsFromNames(sentinels []storage.Object) []internal.BackupObject {
	// A full cluster backup is named backup_<time>, an increment is named backup_<time>_D_<base time>
	// (see configureDeltaBackupName):
	//   <time>      - the time the backup was started, in the 20060102T150405Z format;
	//   _D_         - marks an increment (delta backup);
	//   <base time> - the <time> of the backup this increment is based on.
	//
	// byTime maps _own_ backup <time> to whole backup name.
	// For example, the backups
	//   backup_20240101T000000Z
	//   backup_20240102T000000Z_D_20240101T000000Z
	// give 20240101T000000Z -> backup_20240101T000000Z
	// and  20240102T000000Z -> backup_20240102T000000Z_D_20240101T000000Z.
	byTime := make(map[string]string)

	for _, sentinel := range sentinels {
		name := utility.StripRightmostBackupName(sentinel.GetName())
		backupTime, _, ok := parseClusterBackupName(name)
		if !ok {
			continue
		}
		byTime[backupTime] = name
	}

	backups := make([]internal.BackupObject, 0, len(sentinels))
	for _, sentinel := range sentinels {
		name := utility.StripRightmostBackupName(sentinel.GetName())
		storageName := multistorage.GetStorage(sentinel)
		incrementFrom := incrementFromName(name, byTime)
		if incrementFrom == "" {
			backups = append(backups, newBackupObject("", "", true, sentinel, storageName))
			continue
		}
		// An increment is always created after its base, so walking up the names reaches the full backup.
		fullName := incrementFrom
		for base := incrementFromName(fullName, byTime); base != ""; base = incrementFromName(fullName, byTime) {
			fullName = base
		}
		backups = append(backups, newBackupObject(fullName, incrementFrom, false, sentinel, storageName))
	}
	return backups
}

// incrementFromName returns the name of the backup the given increment is based on, or "" for a full backup.
// If that backup is already gone, its full name is unknown and backup_<base time> is returned instead.
func incrementFromName(name string, byTime map[string]string) string {
	_, baseTime, ok := parseClusterBackupName(name)
	if !ok || baseTime == "" {
		return ""
	}
	if base, exists := byTime[baseTime]; exists {
		return base
	}
	return BackupNamePrefix + baseTime
}

// parseClusterBackupName splits a cluster backup name backup_<time>[_D_<base time>] into the backup time
// and the time of the backup it is based on.
func parseClusterBackupName(name string) (backupTime, baseTime string, ok bool) {
	if !strings.HasPrefix(name, BackupNamePrefix) || len(name) < BackupNameLength {
		return "", "", false
	}
	backupTime, suffix := name[len(BackupNamePrefix):BackupNameLength], name[BackupNameLength:]
	if suffix == "" {
		return backupTime, "", true
	}
	baseTime, isDelta := strings.CutPrefix(suffix, deltaBackupNameDelimiter)
	if !isDelta || baseTime == "" {
		return "", "", false
	}
	return backupTime, baseTime, true
}

// sentinelPermanence reads the permanence flag only from the sentinels of the backups being deleted:
// DeleteTarget checks only them. A backup whose sentinel cannot be read is considered impermanent.
type sentinelPermanence struct {
	ctx     context.Context
	folder  storage.Folder
	checked map[string]bool
}

func (p *sentinelPermanence) isPermanent(object storage.Object) bool {
	name := utility.StripRightmostBackupName(object.GetName())
	storageName := multistorage.GetStorage(object)
	key := storageName + "/" + name
	if permanent, checked := p.checked[key]; checked {
		return permanent
	}
	meta, err := NewGenericMetaFetcher().FetchFromStorage(p.ctx, name, p.folder, storageName)
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot read the sentinel of %s in storage %s, considering it impermanent: %v",
			name, storageName, err)
	}
	p.checked[key] = err == nil && meta.IsPermanent
	return p.checked[key]
}
