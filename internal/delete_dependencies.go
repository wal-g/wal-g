package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/wal-g/wal-g/utility"
)

func (m *DeletionMetadata) collectDependencyField(field string) {
	var previousKind jsontext.Kind
	var previousName string
	for key, raw := range m.Fields {
		if !strings.EqualFold(field, key) {
			continue
		}
		var name string
		if err := json.Unmarshal(raw, &name); err != nil {
			m.Problem = errors.Join(m.Problem, fmt.Errorf("invalid dependency field %s: %w", key, err))
			continue
		}
		kind := raw.Kind()
		if kind == '"' {
			m.dependencyRefs = append(m.dependencyRefs, name)
		}
		if previousKind != 0 && (previousKind != kind || previousName != name) {
			m.Problem = errors.Join(m.Problem, fmt.Errorf("conflicting values for dependency field %s", field))
		}
		previousKind, previousName = kind, name
	}
}

func partialDependencyRefs(data []byte, fields []string) []string {
	if len(fields) == 0 {
		return nil
	}
	decoder := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowDuplicateNames(true))
	start, err := decoder.ReadToken()
	if err != nil || start.Kind() != '{' {
		return nil
	}
	var refs []string
	for decoder.PeekKind() == '"' {
		key, err := decoder.ReadToken()
		if err != nil {
			break
		}
		name := key.String()
		if slices.ContainsFunc(fields, func(field string) bool { return strings.EqualFold(name, field) }) && decoder.PeekKind() == '"' {
			value, err := decoder.ReadToken()
			if err != nil {
				break
			}
			refs = append(refs, value.String())
		} else if err := decoder.SkipValue(); err != nil {
			break
		}
	}
	return refs
}

func (p *DeletionPlan) CheckDeltaParents(ctx context.Context, backupID func(string) string, delta *regexp.Regexp) error {
	selected := make(map[string]string, len(p.Selected))
	for _, name := range p.TargetNames() {
		selected[backupID(name)] = name
	}
	check := func(name string) error {
		match := delta.FindStringSubmatch(name)
		if match != nil {
			if parent := selected[match[1]]; parent != "" {
				return fmt.Errorf("retained delta %s names possible parent %s selected for deletion", name, parent)
			}
		}
		return nil
	}
	for backup := range p.Backups {
		if p.Selected[backup] {
			continue
		}
		b := p.Backups[backup]
		for _, name := range append([]string{backup, b.Parent}, b.uncertainReferences...) {
			if name == "" || p.Selected[name] || p.Backups[name].DependenciesKnown {
				continue
			}
			if err := check(name); err != nil {
				return err
			}
		}
	}
	for _, folder := range p.uncheckedPrefixes {
		conflict := check(path.Base(folder.GetPath()))
		if conflict == nil {
			continue
		}
		hasPayload, err := recoveryPrefixHasPayload(ctx, folder)
		if err != nil {
			return err
		}
		if hasPayload {
			return conflict
		}
	}
	return nil
}

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
		if err := p.validateGraph(true); err != nil {
			return err
		}
	}
	return nil
}
