package internal

import (
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const AllowBrokenFlag = "allow-broken"
const DeleteUnusedLogsFlag = "delete-unused-logs"

type RecoveryOptions struct {
	AllowBroken, DeleteUnusedLogs bool
	TargetStorage                 string
}

func (o *RecoveryOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.AllowBroken, AllowBrokenFlag, false, "Delete explicitly named damaged backups during a maintenance window")
	cmd.Flags().BoolVar(&o.DeleteUnusedLogs, DeleteUnusedLogsFlag, false,
		"Also delete logs proven unnecessary for retained backups (requires --allow-broken)")
	cmd.Flags().StringVar(&o.TargetStorage, "target-storage", "", "Recovery destination; required when several storages are configured")
}

func (o *RecoveryOptions) Validate(cmd *cobra.Command, args []string, normal cobra.PositionalArgs) error {
	if !o.AllowBroken {
		if o.DeleteUnusedLogs {
			return fmt.Errorf("--%s requires --%s", DeleteUnusedLogsFlag, AllowBrokenFlag)
		}
		if o.TargetStorage != "" {
			return fmt.Errorf("--target-storage on this command requires --allow-broken")
		}
		return normal(cmd, args)
	}
	if flag := cmd.Flags().Lookup(DeleteTargetUserDataFlag); flag != nil && flag.Value.String() != "" {
		return fmt.Errorf("recovery requires explicit backup names, not user data")
	}
	if len(args) == 0 {
		return fmt.Errorf("recovery requires at least one explicit backup name")
	}
	for _, name := range args {
		if err := ValidateRecoveryName(name); err != nil {
			return err
		}
	}
	return nil
}

var recoveryNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

func ValidateRecoveryName(name string) error {
	if !recoveryNamePattern.MatchString(name) || strings.Contains(name, "..") ||
		strings.EqualFold(name, "LATEST") || strings.EqualFold(name, "FIND_FULL") ||
		strings.EqualFold(name, "aosegments") || strings.EqualFold(name, "paxfiles") {
		return fmt.Errorf("invalid explicit backup name %q", name)
	}
	return nil
}

func ConfigureRecoveryFolder(ctx context.Context, o RecoveryOptions) (storage.Folder, string, error) {
	st, err := ConfigureMultiStorage(ctx, true)
	if err != nil {
		return nil, "", err
	}
	name := o.TargetStorage
	if name == "" {
		name = viper.GetString(conf.PgTargetStorage)
	}
	return multistorage.PinStorage(ctx, st.RootFolder(), name)
}

type RecoveryPlanner func(context.Context, storage.Folder, string, []string, bool) (*DeletionPlan, error)

func RunRecoveryDeletion(ctx context.Context, options RecoveryOptions, names []string, confirmed bool, build RecoveryPlanner) error {
	root, destination, err := ConfigureRecoveryFolder(ctx, options)
	if err != nil {
		return err
	}
	plan, err := build(ctx, root, destination, names, options.DeleteUnusedLogs)
	if plan != nil {
		defer plan.Close()
	}
	if err != nil {
		return err
	}
	return plan.Execute(ctx, confirmed)
}

type DeletionMetadata struct {
	Fields  map[string]json.RawMessage
	Problem error
}

type UnavailableDeletionMetadataError struct {
	Key string
	Err error
}

func (e *UnavailableDeletionMetadataError) Error() string {
	return fmt.Sprintf("metadata %s unavailable: %v", e.Key, e.Err)
}
func (e *UnavailableDeletionMetadataError) Unwrap() error { return e.Err }

func ReadDeletionMetadata(ctx context.Context, folder storage.Folder, key string, dto interface{}) (DeletionMetadata, error) {
	if err := ctx.Err(); err != nil {
		return DeletionMetadata{}, err
	}
	r, err := folder.ReadObject(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return DeletionMetadata{}, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return DeletionMetadata{}, err
		}
		var missing storage.ObjectNotFoundError
		if errors.As(err, &missing) {
			return DeletionMetadata{Problem: err}, nil
		}
		return DeletionMetadata{Problem: &UnavailableDeletionMetadataError{key, err}}, nil
	}
	data, err := io.ReadAll(r)
	closeErr := r.Close()
	if err = errors.Join(err, closeErr); err != nil {
		if ctx.Err() != nil {
			return DeletionMetadata{}, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return DeletionMetadata{}, err
		}
		return DeletionMetadata{Problem: &UnavailableDeletionMetadataError{key, err}}, nil
	}
	var result DeletionMetadata
	if err := jsonv2.Unmarshal(data, &result.Fields); err != nil {
		result.Problem = fmt.Errorf("invalid %s: %w", key, err)
		return result, nil
	}
	if len(result.Fields) == 0 {
		result.Problem = fmt.Errorf("empty metadata %s", key)
	} else if dto != nil {
		if err := json.Unmarshal(data, dto); err != nil {
			result.Problem = fmt.Errorf("invalid %s: %w", key, err)
		}
	}
	return result, nil
}

func (m DeletionMetadata) Bool(key string) (value, known bool) {
	raw, ok := m.Fields[key]
	if !ok || string(raw) == "null" {
		return false, false
	}
	if json.Unmarshal(raw, &value) != nil {
		return false, false
	}
	return value, true
}

type DeletionBackup struct {
	Name, Parent, Base         string
	DependenciesKnown          bool
	Permanent, ProtectionKnown bool
	Problem                    error
	FinishTime                 time.Time
	Metadata                   interface{}
}

type DeletionLoader func(context.Context, storage.Folder, string) (DeletionBackup, error)

type DeletionPlan struct {
	Root        storage.Folder
	StorageName string
	Backups     map[string]DeletionBackup
	Selected    map[string]bool
	Unresolved  []string
	Details     []string
	Stages      []*DeletionStage
	Before      []func(context.Context) error
	Updates     []func(context.Context) error
	Finalizers  []func(context.Context) error
	Results     []*DeletionResult
}

type DeletionResult struct {
	Storage, Prefix, Backup, Status string
}

func (p *DeletionPlan) Absorb(child *DeletionPlan) {
	p.Stages = append(p.Stages, child.Stages...)
	p.Before = append(p.Before, child.Before...)
	p.Updates = append(p.Updates, child.Updates...)
	p.Finalizers = append(p.Finalizers, child.Finalizers...)
	p.Unresolved = append(p.Unresolved, child.Unresolved...)
	p.Details = append(p.Details, child.Details...)
	p.Results = append(p.Results, child.Results...)
	child.Results = nil
	child.Before, child.Finalizers = nil, nil
	child.Stages, child.Updates, child.Unresolved, child.Details = nil, nil, nil, nil
}

type DeletionStage struct {
	Label  string
	Folder storage.Folder
	file   *os.File
	Count  int
	verify func(context.Context) error
}

type deletionObject struct {
	Name          string
	Modified      time.Time
	Size          int64
	Version, Info string
}

func NewDeletionPlan(ctx context.Context, root storage.Folder, storageName string, names []string,
	incremental bool, isBackupName func(string) bool, load DeletionLoader, additionalCatalogNames ...string) (*DeletionPlan, error) {
	if err := multistorage.EnsureSingleStorageIsUsed(root); err != nil {
		return nil, err
	}
	p := &DeletionPlan{Root: root, StorageName: storageName, Backups: map[string]DeletionBackup{}, Selected: map[string]bool{}}
	for _, name := range names {
		if err := ValidateRecoveryName(name); err != nil {
			return nil, err
		}
		p.Selected[name] = true
	}
	if len(p.Selected) == 0 {
		return nil, fmt.Errorf("no explicit recovery targets")
	}
	if err := p.discoverBackups(ctx, isBackupName, load, additionalCatalogNames); err != nil {
		return nil, err
	}
	if incremental {
		if err := p.selectDependents(); err != nil {
			return nil, err
		}
	}
	for name := range p.Selected {
		if p.Backups[name].Permanent {
			return nil, fmt.Errorf("cannot delete known permanent backup %s", name)
		}
		if problem := p.Backups[name].Problem; problem != nil {
			p.Details = append(p.Details, fmt.Sprintf("target=%s metadata unknown: %v", name, problem))
		}
	}
	return p, nil
}

func (p *DeletionPlan) discoverBackups(ctx context.Context, isBackupName func(string) bool, load DeletionLoader,
	additionalCatalogNames []string) error {
	base := p.Root.GetSubFolder(utility.BaseBackupPath)
	objects, folders, err := base.ListFolder(ctx)
	if err != nil {
		return err
	}
	catalog := map[string]bool{}
	for _, name := range additionalCatalogNames {
		catalog[name] = true
	}
	for name := range p.Selected {
		catalog[name] = true
	}
	for _, object := range objects {
		key := strings.TrimLeft(object.GetName(), "/")
		if strings.HasSuffix(key, utility.SentinelSuffix) {
			catalog[strings.TrimSuffix(key, utility.SentinelSuffix)] = true
		}
	}
	for _, folder := range folders {
		name := path.Base(strings.TrimSuffix(folder.GetPath(), "/"))
		if isBackupName(name) && !catalog[name] {
			found := errors.New("backup payload found")
			err := storage.WalkObjects(ctx, storage.WithAllVersions(folder), func(storage.Object) error { return found })
			if errors.Is(err, found) {
				catalog[name] = true
			} else if err != nil {
				return fmt.Errorf("inspect backup prefix %s: %w", name, err)
			}
		}
	}
	for _, name := range sortedDeletionNames(catalog) {
		if err := ValidateRecoveryName(name); err != nil {
			return err
		}
		b, err := load(ctx, p.Root, name)
		if err != nil {
			return fmt.Errorf("inspect backup %s: %w", name, err)
		}
		b.Name = name
		p.Backups[name] = b
	}
	return nil
}
func (p *DeletionPlan) selectDependents() error {
	for changed := true; changed; {
		changed = false
		for name, b := range p.Backups {
			if !p.Selected[name] && (p.Selected[b.Parent] || p.Selected[b.Base]) {
				p.Selected[name], changed = true, true
			}
		}
	}
	for name, b := range p.Backups {
		if !p.Selected[name] && !b.DependenciesKnown {
			return fmt.Errorf("cannot establish dependencies of retained backup %s: %v", name, b.Problem)
		}
	}
	if err := p.validateGraph(); err != nil {
		return err
	}
	return nil
}

func (p *DeletionPlan) validateGraph() error {
	colors := map[string]int{}
	var visit func(string) error
	visit = func(name string) error {
		if colors[name] == 1 {
			return fmt.Errorf("backup dependency cycle at %s", name)
		}
		if colors[name] == 2 {
			return nil
		}
		colors[name] = 1
		b := p.Backups[name]
		if b.Parent != "" {
			if err := validateDeletionLinkIdentity(b); err != nil {
				return err
			}
			if parent, ok := p.Backups[b.Parent]; ok {
				if err := validateDeletionLinkAgreement(b, parent); err != nil {
					return err
				}
				if err := visit(b.Parent); err != nil {
					return err
				}
			} else if !p.Selected[name] {
				return fmt.Errorf("unknown ancestor %s of retained backup %s", b.Parent, name)
			}
		}
		colors[name] = 2
		return nil
	}
	for name := range p.Backups {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func validateDeletionLinkIdentity(b DeletionBackup) error {
	if err := ValidateRecoveryName(b.Parent); err != nil {
		return err
	}
	if err := ValidateRecoveryName(b.Base); err != nil {
		return err
	}
	if b.Parent == b.Name || b.Base == b.Name {
		return fmt.Errorf("self dependency in %s", b.Name)
	}
	return nil
}
func validateDeletionLinkAgreement(b, parent DeletionBackup) error {
	if parent.DependenciesKnown && parent.Parent != "" && parent.Base != b.Base {
		return fmt.Errorf("conflicting full backup for %s", b.Name)
	}
	if parent.DependenciesKnown && parent.Parent == "" && b.Base != b.Parent {
		return fmt.Errorf("conflicting parent/full backup for %s", b.Name)
	}
	return nil
}

func sortedDeletionNames(names map[string]bool) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (p *DeletionPlan) TargetNames() []string { return sortedDeletionNames(p.Selected) }

func (p *DeletionPlan) Retained() []DeletionBackup {
	names := map[string]bool{}
	for name := range p.Backups {
		if !p.Selected[name] {
			names[name] = true
		}
	}
	result := make([]DeletionBackup, 0, len(names))
	for _, name := range sortedDeletionNames(names) {
		result = append(result, p.Backups[name])
	}
	return result
}

func (p *DeletionPlan) Defer(reason string) { p.Unresolved = append(p.Unresolved, reason) }

func (p *DeletionPlan) NewStage(label string, folder storage.Folder) (*DeletionStage, error) {
	f, err := os.CreateTemp("", "wal-g-delete-*")
	if err != nil {
		return nil, err
	}
	s := &DeletionStage{Label: label, Folder: folder, file: f}
	p.Stages = append(p.Stages, s)
	return s, nil
}

func (s *DeletionStage) Add(object storage.Object) error {
	key := strings.TrimLeft(object.GetName(), "/")
	if (key != "" && path.Clean(key) != strings.TrimSuffix(key, "/")) || key == "." || key == ".." || strings.HasPrefix(key, "../") {
		return fmt.Errorf("invalid deletion object key %q", key)
	}
	err := json.NewEncoder(s.file).Encode(deletionObject{key, object.GetLastModified(), object.GetSize(),
		object.GetVersionID(), object.GetAdditionalInfo()})
	if err == nil {
		s.Count++
	}
	return err
}

func (p *DeletionPlan) PlanObjects(ctx context.Context, label string, folder storage.Folder, eligible func(storage.Object) bool) error {
	return p.PlanObjectsChecked(ctx, label, folder, func(obj storage.Object) (bool, error) { return eligible(obj), nil })
}

func (p *DeletionPlan) PlanObjectsChecked(ctx context.Context, label string, folder storage.Folder,
	eligible func(storage.Object) (bool, error)) error {
	s, err := p.NewStage(label, folder)
	if err != nil {
		return err
	}
	folder = storage.WithAllVersions(folder)
	return storage.WalkObjects(ctx, folder, func(o storage.Object) error {
		selected, err := eligible(o)
		if err != nil {
			return err
		}
		if selected {
			return s.Add(o)
		}
		return nil
	})
}

func (p *DeletionPlan) PrepareTargets(ctx context.Context, sidecars []string) error {
	base := storage.WithAllVersions(p.Root.GetSubFolder(utility.BaseBackupPath))
	objects, _, err := base.ListFolder(ctx)
	if err != nil {
		return err
	}
	ownedSidecars := map[string]bool{}
	for _, key := range sidecars {
		ownedSidecars[key] = true
	}
	for name := range p.Selected {
		ownedSidecars[JournalPrefix+name] = true
	}
	if err := p.prepareSentinels(base, objects); err != nil {
		return err
	}
	for _, name := range p.TargetNames() {
		if err := p.preparePayload(ctx, base, name, objects); err != nil {
			return err
		}
	}
	sidecarStage, err := p.NewStage("owned sidecars and journals", base)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if ownedSidecars[strings.TrimLeft(obj.GetName(), "/")] {
			if err := sidecarStage.Add(obj); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *DeletionPlan) prepareSentinels(base storage.Folder, objects []storage.Object) error {
	sentinels, err := p.NewStage("sentinel versions", base)
	if err != nil {
		return err
	}
	markers, err := p.NewStage("sentinel delete markers", base)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		key := strings.TrimLeft(obj.GetName(), "/")
		if !strings.HasSuffix(key, utility.SentinelSuffix) || !p.Selected[strings.TrimSuffix(key, utility.SentinelSuffix)] {
			continue
		}
		stage := sentinels
		if strings.Contains(obj.GetAdditionalInfo(), "DELETE") {
			stage = markers
		}
		if err := stage.Add(obj); err != nil {
			return err
		}
	}
	markers.verify = func(ctx context.Context) error {
		left, _, err := base.ListFolder(ctx)
		if err != nil {
			return err
		}
		for _, obj := range left {
			key := strings.TrimLeft(obj.GetName(), "/")
			if strings.HasSuffix(key, utility.SentinelSuffix) && p.Selected[strings.TrimSuffix(key, utility.SentinelSuffix)] {
				return fmt.Errorf("sentinel deletion unconfirmed: %s", key)
			}
		}
		return nil
	}
	return nil
}
func (p *DeletionPlan) preparePayload(ctx context.Context, base storage.Folder, name string, objects []storage.Object) error {
	folder := storage.WithAllVersions(base.GetSubFolder(name))
	if err := p.PlanObjects(ctx, "payload "+name, folder, func(storage.Object) bool { return true }); err != nil {
		return err
	}
	stage := p.Stages[len(p.Stages)-1]
	status := "absent"
	if stage.Count > 0 {
		status = "planned"
	}
	for _, obj := range objects {
		key := strings.TrimLeft(obj.GetName(), "/")
		if key == name+utility.SentinelSuffix || key == JournalPrefix+name {
			status = "planned"
		}
	}
	p.Results = append(p.Results, &DeletionResult{p.StorageName, p.Root.GetPath(), name, status})
	stage.verify = func(ctx context.Context) error {
		return storage.WalkObjects(ctx, folder, func(obj storage.Object) error {
			return fmt.Errorf("payload deletion unconfirmed: %s%s", folder.GetPath(), obj.GetName())
		})
	}
	return nil
}

func (p *DeletionPlan) Close() {
	for _, s := range p.Stages {
		if s.file != nil {
			name := s.file.Name()
			_ = s.file.Close()
			_ = os.Remove(name)
		}
	}
}

func (p *DeletionPlan) Print() {
	tracelog.InfoLogger.Printf("Recovery inventory: storage=%s prefix=%s targets=%v (save this inventory for retries)",
		p.StorageName, p.Root.GetPath(), p.TargetNames())
	for _, detail := range p.Details {
		tracelog.InfoLogger.Println(detail)
	}
	for _, stage := range p.Stages {
		tracelog.InfoLogger.Printf("  %s: %d objects in %s", stage.Label, stage.Count, stage.Folder.GetPath())
	}
	for _, reason := range p.Unresolved {
		tracelog.WarningLogger.Printf("Unresolved: %s", reason)
	}
}

type IncompleteDeletionError struct{ Reasons []string }

func (e *IncompleteDeletionError) Error() string {
	return "incomplete cleanup: " + strings.Join(e.Reasons, "; ")
}

func (p *DeletionPlan) Execute(ctx context.Context, confirmed bool) (resultErr error) {
	p.Print()
	defer func() { p.reportResults(confirmed, resultErr) }()
	if confirmed {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, before := range p.Before {
			if err := before(ctx); err != nil {
				return fmt.Errorf("deletion preparation failed: %w", err)
			}
		}
		for _, s := range p.Stages {
			if err := s.execute(ctx); err != nil {
				return fmt.Errorf("%s failed; dependent stages stopped: %w", s.Label, err)
			}
		}
		for _, update := range p.Updates {
			if err := update(ctx); err != nil {
				return fmt.Errorf("accounting update failed: %w", err)
			}
		}
	} else {
		tracelog.InfoLogger.Println("Dry run; pass --confirm to execute during a maintenance window")
	}
	if len(p.Unresolved) != 0 {
		return &IncompleteDeletionError{p.Unresolved}
	}
	if confirmed {
		for _, finalize := range p.Finalizers {
			if err := finalize(ctx); err != nil {
				return fmt.Errorf("deletion finalization failed: %w", err)
			}
		}
	}
	return nil
}

func (p *DeletionPlan) reportResults(confirmed bool, resultErr error) {
	for _, result := range p.Results {
		if confirmed && result.Status != "absent" {
			result.Status = "deleted"
			if resultErr != nil {
				var incomplete *IncompleteDeletionError
				if errors.As(resultErr, &incomplete) {
					result.Status = "unresolved"
				} else {
					result.Status = "failed"
				}
			}
		}
		tracelog.InfoLogger.Printf("Recovery result: storage=%s prefix=%s backup=%s status=%s",
			result.Storage, result.Prefix, result.Backup, result.Status)
	}
}

func (s *DeletionStage) execute(ctx context.Context) error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	decoder := json.NewDecoder(s.file)
	batch := make([]storage.Object, 0, 500)
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		err := s.Folder.DeleteObjects(ctx, batch)
		batch = batch[:0]
		return err
	}
	for {
		var obj deletionObject
		err := decoder.Decode(&obj)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		batch = append(batch, storage.NewLocalObjectWithVersion(obj.Name, obj.Modified, obj.Size, obj.Version, obj.Info))
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if s.verify != nil {
		return s.verify(ctx)
	}
	return nil
}

func ValidateDeletionIncrement(name string, parent, base *string, count *int, hasLSN, requireLSN bool) error {
	if parent == nil && base == nil && count == nil && (!requireLSN || !hasLSN) {
		return nil
	}
	if !validDeletionIncrementFields(parent, base, count, hasLSN, requireLSN) {
		return fmt.Errorf("inconsistent incremental metadata for %s", name)
	}
	if err := ValidateRecoveryName(*parent); err != nil {
		return err
	}
	if err := ValidateRecoveryName(*base); err != nil {
		return err
	}
	if *parent == name || *base == name {
		return fmt.Errorf("self dependency in %s", name)
	}
	return nil
}

func validDeletionIncrementFields(parent, base *string, count *int, hasLSN, requireLSN bool) bool {
	return parent != nil && base != nil && count != nil && *count > 0 && (!requireLSN || hasLSN)
}
