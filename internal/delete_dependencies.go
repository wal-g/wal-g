package internal

import "github.com/wal-g/wal-g/utility"

type deletionBackupObject struct{ BackupObject }

func (b deletionBackupObject) GetName() string {
	return utility.BaseBackupPath + b.GetBackupName() + utility.SentinelSuffix
}

func ValidateBackupDependencies(backups []BackupObject) error {
	byStorage := map[string]*DeletionPlan{}
	for _, b := range backups {
		p := byStorage[b.GetStorage()]
		if p == nil {
			p = &DeletionPlan{Backups: map[string]DeletionBackup{}, Selected: map[string]bool{}}
			byStorage[b.GetStorage()] = p
		}
		item := DeletionBackup{Name: b.GetBackupName(), DependenciesKnown: true}
		if !b.IsFullBackup() {
			item.Parent, item.Base = b.GetIncrementFromName(), b.GetBaseBackupName()
		}
		p.Backups[item.Name] = item
	}
	for _, p := range byStorage {
		if err := p.validateGraph(); err != nil {
			return err
		}
	}
	return nil
}
