package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func PlanDeletionJournals(ctx context.Context, p *DeletionPlan, archivePath string) error {
	journals, err := loadDeletionJournals(ctx, p, archivePath)
	if err != nil {
		return err
	}
	if len(journals) == 0 {
		return nil
	}
	sort.Slice(journals, func(i, j int) bool { return journals[i].CurrentBackupEnd.Before(journals[j].CurrentBackupEnd) })
	for i := range journals {
		journals[i].PriorBackupEnd = time.Time{}
		journals[i].SizeToNextBackup = 0
		if i > 0 {
			if journals[i].CurrentBackupEnd.Equal(journals[i-1].CurrentBackupEnd) {
				p.Defer("journal accounting unchanged: ambiguous survivor order")
				return nil
			}
			journals[i].PriorBackupEnd = journals[i-1].CurrentBackupEnd
		}
	}
	if len(journals) > 1 {
		err = accountDeletionJournals(ctx, p, journals, archivePath)
		if err != nil {
			return err
		}
	}
	for _, j := range journals {
		p.Updates = append(p.Updates, func(ctx context.Context) error { return j.Upload(ctx, p.Root) })
	}
	return nil
}

func loadDeletionJournals(ctx context.Context, p *DeletionPlan, archivePath string) ([]JournalInfo, error) {
	base := p.Root.GetSubFolder(utility.BaseBackupPath)
	objects, _, err := base.ListFolder(ctx)
	if err != nil {
		return nil, err
	}
	var journals []JournalInfo
	for _, obj := range objects {
		name := strings.TrimLeft(obj.GetName(), "/")
		if !strings.HasPrefix(name, JournalPrefix) {
			continue
		}
		backupName := strings.TrimPrefix(name, JournalPrefix)
		if p.Selected[backupName] {
			continue
		}
		b, exists := p.Backups[backupName]
		var j JournalInfo
		doc, err := ReadDeletionMetadata(ctx, base, name, &j)
		if err != nil {
			return nil, err
		}
		if !exists || b.Problem != nil || doc.Problem != nil || j.CurrentBackupEnd.IsZero() ||
			j.PriorBackupEnd.After(j.CurrentBackupEnd) || j.SizeToNextBackup < 0 {
			p.Defer("journal accounting unchanged: unknown survivor interval for " + backupName)
			return nil, nil
		}
		j.JournalName, j.JournalDirectoryName = name, archivePath
		journals = append(journals, j)
	}
	return journals, nil
}

func accountDeletionJournals(ctx context.Context, p *DeletionPlan, journals []JournalInfo, archivePath string) error {
	archive := p.Root.GetSubFolder(archivePath)
	deleted, err := p.plannedArchiveObjects(ctx, archive)
	if err != nil {
		return err
	}
	defer deleted.Close()
	return storage.WalkObjects(ctx, archive, func(obj storage.Object) error {
		removed, err := deleted.Contains(strings.TrimLeft(obj.GetName(), "/"))
		if err != nil || removed {
			return err
		}
		when := obj.GetLastModified()
		i := sort.Search(len(journals), func(i int) bool { return !journals[i].CurrentBackupEnd.Before(when) })
		if i > 0 && i < len(journals) {
			if obj.GetSize() < 0 || journals[i-1].SizeToNextBackup > int64(^uint64(0)>>1)-obj.GetSize() {
				return fmt.Errorf("journal size overflow")
			}
			journals[i-1].SizeToNextBackup += obj.GetSize()
		}
		return nil
	})
}

func (p *DeletionPlan) plannedArchiveObjects(ctx context.Context, archive storage.Folder) (*DeletionReferences, error) {
	refs, err := NewDeletionReferences()
	if err != nil {
		return nil, err
	}
	for _, stage := range p.Stages {
		if stage.Folder.GetPath() != archive.GetPath() {
			continue
		}
		if err := stage.indexObjects(ctx, refs); err != nil {
			refs.Close()
			return nil, err
		}
	}
	if err := refs.Seal(); err != nil {
		refs.Close()
		return nil, err
	}
	return refs, nil
}

func (s *DeletionStage) indexObjects(ctx context.Context, refs *DeletionReferences) error {
	stat, err := s.file.Stat()
	if err != nil {
		return err
	}
	d := json.NewDecoder(io.NewSectionReader(s.file, 0, stat.Size()))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var object deletionObject
		if err := d.Decode(&object); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := refs.Add(object.Name); err != nil {
			return err
		}
	}
}
