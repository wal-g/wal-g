package multistorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// UseAllAliveStorages makes a copy of the Folder that uses all currently alive storages.
func UseAllAliveStorages(folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	storageNames, err := mf.statsCollector.AllAliveStorages()
	if err != nil {
		return nil, fmt.Errorf("select all alive storages in multistorage folder: %w", err)
	}
	if len(storageNames) == 0 {
		return nil, ErrNoAliveStorages
	}
	mf.usedFolders = make([]NamedFolder, len(storageNames))
	for i, name := range storageNames {
		root := mf.configuredRootFolders[name]
		mf.usedFolders[i] = NamedFolder{
			Folder:      root.GetSubFolder(mf.path),
			StorageName: name,
		}
	}
	return mf, nil
}

// UseFirstAliveStorage makes a copy of the Folder that uses a single storage, that is first alive in the list.
// This is an error if all storages are dead.
func UseFirstAliveStorage(folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	firstStorage, err := mf.statsCollector.FirstAliveStorage()
	if err != nil {
		return nil, fmt.Errorf("select first alive storage in multistorage folder: %w", err)
	}
	if firstStorage == nil {
		return nil, ErrNoAliveStorages
	}
	mf.usedFolders = []NamedFolder{
		{
			Folder:      mf.configuredRootFolders[*firstStorage].GetSubFolder(mf.path),
			StorageName: *firstStorage,
		},
	}
	return mf, nil
}

// UseSpecificStorage makes a copy of the Folder that uses storage with the specified name.
// This is an error if the storage is dead.
func UseSpecificStorage(name string, folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	alreadyUsed := len(mf.usedFolders) == 1 && mf.usedFolders[0].StorageName == name
	if alreadyUsed {
		return mf, nil
	}

	alive, err := mf.statsCollector.SpecificStorage(name)
	if err != nil {
		return nil, fmt.Errorf("select storage %q in multistorage folder: %w", name, err)
	}
	if !alive {
		return nil, ErrNoAliveStorages
	}
	mf.usedFolders = []NamedFolder{
		{
			Folder:      mf.configuredRootFolders[name].GetSubFolder(mf.path),
			StorageName: name,
		},
	}
	return mf, nil
}

func UsedStorages(folder storage.Folder) []string {
	mf, ok := folder.(Folder)
	if !ok {
		return []string{consts.DefaultStorage}
	}

	var storageNames []string
	for _, s := range mf.usedFolders {
		storageNames = append(storageNames, s.StorageName)
	}
	return storageNames
}

func EnsureSingleStorageIsUsed(folder storage.Folder) error {
	storages := UsedStorages(folder)
	if len(storages) != 1 {
		return fmt.Errorf("multi-storage folder is expected to use a single storage, but it uses %d", len(storages))
	}
	return nil
}

// SetPolicies makes a copy of the Folder that uses new policies.Policies.
func SetPolicies(folder storage.Folder, policies policies.Policies) storage.Folder {
	if mf, ok := folder.(Folder); ok {
		mf.policies = policies
		return mf
	}
	return folder
}

func NewFolder(specificFolders map[string]storage.Folder, statsCollector stats.Collector) storage.Folder {
	return Folder{
		statsCollector:        statsCollector,
		configuredRootFolders: specificFolders,
		usedFolders:           nil,
		path:                  "",
		policies:              policies.Default,
	}
}

// Folder represents a multi-storage folder that aggregates several folders from different storages with the same path.
// A specific behavior (should the folders be united, merged, etc.) is selected by policies.Policies.
type Folder struct {
	statsCollector        stats.Collector
	configuredRootFolders map[string]storage.Folder
	usedFolders           []NamedFolder
	path                  string
	policies              policies.Policies
}

// GetPath provides the base path that is common for all the storages.
func (mf Folder) GetPath() string {
	return mf.path
}

// GetSubFolder provides a multi-storage subfolder, which includes subfolders of all used storages.
func (mf Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	newPath := storage.AddDelimiterToPath(storage.JoinPath(mf.path, subFolderRelativePath))
	if newPath == "/" {
		newPath = ""
	}
	multiSubfolder := Folder{
		statsCollector:        mf.statsCollector,
		configuredRootFolders: mf.configuredRootFolders,
		path:                  newPath,
		policies:              mf.policies,
	}
	multiSubfolder.usedFolders = make([]NamedFolder, len(mf.usedFolders))
	for i := range mf.usedFolders {
		multiSubfolder.usedFolders[i] = mf.usedFolders[i].GetSubFolder(subFolderRelativePath)
	}
	return multiSubfolder
}

// Exists checks if the object exists in multiple storages. A specific implementation is selected using
// policies.Policies
func (mf Folder) Exists(objectRelativePath string) (bool, error) {
	exists, _, err := Exists(mf, objectRelativePath)
	return exists, err
}

// Exists is like storage.Folder.Exists, but it also provides the name of the storage where the file is found. If it's
// not found, storage name is empty. If it's found in all storages, provides "all" as the storage name.
func Exists(folder storage.Folder, objectRelativePath string) (found bool, storageName string, err error) {
	mf, ok := folder.(Folder)
	if !ok {
		exists, err := folder.Exists(objectRelativePath)
		return exists, consts.DefaultStorage, err
	}

	switch mf.policies.Exists {
	case policies.ExistsPolicyFirst:
		return mf.ExistsInFirst(objectRelativePath)
	case policies.ExistsPolicyAny:
		return mf.ExistsInAny(objectRelativePath)
	case policies.ExistsPolicyAll:
		return mf.ExistsInAll(objectRelativePath)
	default:
		panic(fmt.Sprintf("unknown exists policy %d", mf.policies.Exists))
	}
}

// ExistsInFirst checks if the object exists in the first storage.
func (mf Folder) ExistsInFirst(objectRelativePath string) (bool, string, error) {
	if len(mf.usedFolders) == 0 {
		return false, "", ErrNoUsedStorages
	}
	first := mf.usedFolders[0]
	exists, err := first.Exists(objectRelativePath)
	mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationExists, err == nil)
	if err != nil {
		return false, first.StorageName, fmt.Errorf("check file for existence in %q: %w", first.StorageName, err)
	}
	return exists, first.StorageName, nil
}

// ExistsInAny checks if the object exists in any storage.
func (mf Folder) ExistsInAny(objectRelativePath string) (bool, string, error) {
	for _, f := range mf.usedFolders {
		exists, err := f.Exists(objectRelativePath)
		mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationExists, err == nil)
		if err != nil {
			return false, f.StorageName, fmt.Errorf("check file for existence in %q: %w", f.StorageName, err)
		}
		if exists {
			return true, f.StorageName, nil
		}
	}
	return false, consts.AllStorages, nil
}

// ExistsInAll checks if the object exists in all used storages.
func (mf Folder) ExistsInAll(objectRelativePath string) (bool, string, error) {
	for _, f := range mf.usedFolders {
		exists, err := f.Exists(objectRelativePath)
		mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationExists, err == nil)
		if err != nil {
			return false, f.StorageName, fmt.Errorf("check file for existence in %q: %w", f.StorageName, err)
		}
		if !exists {
			return false, f.StorageName, nil
		}
	}
	return true, consts.AllStorages, nil
}

// ReadObject reads the object from multiple storages. A specific implementation is selected using policies.Policies.
func (mf Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	file, _, err := ReadObject(mf, objectRelativePath)
	return file, err
}

// ReadObject is like storage.Folder.ReadObject, but it also provides the name of storage where the file is read from.
func ReadObject(folder storage.Folder, objectRelativePath string) (io.ReadCloser, string, error) {
	mf, ok := folder.(Folder)
	if !ok {
		file, err := folder.ReadObject(objectRelativePath)
		return file, consts.DefaultStorage, err
	}

	switch mf.policies.Read {
	case policies.ReadPolicyFirst:
		return mf.ReadObjectFromFirst(objectRelativePath)
	case policies.ReadPolicyFoundFirst:
		return mf.ReadObjectFoundFirst(objectRelativePath)
	default:
		panic(fmt.Sprintf("unknown read object policy %d", mf.policies.Read))
	}
}

// ReadObjectFromFirst reads the object from the first storage.
func (mf Folder) ReadObjectFromFirst(objectRelativePath string) (io.ReadCloser, string, error) {
	if len(mf.usedFolders) == 0 {
		return nil, "", ErrNoUsedStorages
	}
	first := mf.usedFolders[0]
	file, err := first.ReadObject(objectRelativePath)
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationRead(0), true)
			return file, first.StorageName, err
		}
		mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationRead(0), false)
		return nil, first.StorageName, fmt.Errorf("read object from %q: %w", first.StorageName, err)
	}
	reportFile := newReportReadCloser(file, mf.statsCollector, first.StorageName)
	return reportFile, first.StorageName, nil
}

// ReadObjectFoundFirst reads the object from all used storages in order and returns the first one found.
func (mf Folder) ReadObjectFoundFirst(objectRelativePath string) (io.ReadCloser, string, error) {
	for _, f := range mf.usedFolders {
		exists, err := f.Exists(objectRelativePath)
		if err != nil {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationExists, false)
			return nil, f.StorageName, fmt.Errorf("check file for existence in %q: %w", f.StorageName, err)
		}
		if exists {
			file, err := f.ReadObject(objectRelativePath)
			if err != nil {
				if _, ok := err.(storage.ObjectNotFoundError); ok {
					mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationRead(0), true)
					return file, f.StorageName, err
				}
				mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationRead(0), false)
				return nil, f.StorageName, fmt.Errorf("read object from %q: %w", f.StorageName, err)
			}
			reportFile := newReportReadCloser(file, mf.statsCollector, f.StorageName)
			return reportFile, f.StorageName, nil
		}
	}
	return nil, consts.AllStorages, storage.NewObjectNotFoundError(objectRelativePath)
}

// ListFolder lists the folder in multiple storages. A specific implementation is selected using policies.Policies
func (mf Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	switch mf.policies.List {
	case policies.ListPolicyFirst:
		return mf.ListFolderInFirst()
	case policies.ListPolicyFoundFirst:
		return mf.ListFolderWhereFoundFirst()
	case policies.ListPolicyAll:
		return mf.ListFolderAll()
	default:
		panic(fmt.Sprintf("unknown list policy %d", mf.policies.List))
	}
}

// ListFolderInFirst lists the folder in the first storage.
func (mf Folder) ListFolderInFirst() (objects []storage.Object, subFolders []storage.Folder, err error) {
	if len(mf.usedFolders) == 0 {
		return nil, nil, ErrNoUsedStorages
	}
	return mf.listSpecificFolder(mf.usedFolders[0])
}

// ListFolderWhereFoundFirst lists the folder in all used storages and provides a result where each file is taken from
// the first storage in which it was found.
func (mf Folder) ListFolderWhereFoundFirst() (objects []storage.Object, subFolders []storage.Folder, err error) {
	metObjects := map[string]bool{}
	metSubFolders := map[string]bool{}

	for _, f := range mf.usedFolders {
		curObjects, curSubFolders, err := mf.listSpecificFolder(f)
		if err != nil {
			return nil, nil, err
		}
		for _, obj := range curObjects {
			name := obj.GetName()
			if metObjects[name] {
				continue
			}
			objects = append(objects, obj)
			metObjects[name] = true
		}
		for _, subf := range curSubFolders {
			name := subf.GetPath()
			if metSubFolders[name] {
				continue
			}
			subFolders = append(subFolders, subf)
			metSubFolders[name] = true
		}
	}

	return objects, subFolders, nil
}

// ListFolderAll lists every used storage and provides the union of all found files. Subfolders aren't listed twice.
func (mf Folder) ListFolderAll() (objects []storage.Object, subFolders []storage.Folder, err error) {
	metSubFolders := map[string]bool{}

	for _, f := range mf.usedFolders {
		curObjects, curSubFolders, err := mf.listSpecificFolder(f)
		if err != nil {
			return nil, nil, err
		}
		objects = append(objects, curObjects...)
		for _, subf := range curSubFolders {
			name := subf.GetPath()
			if metSubFolders[name] {
				continue
			}
			subFolders = append(subFolders, subf)
			metSubFolders[name] = true
		}
	}

	return objects, subFolders, nil
}

func (mf Folder) listSpecificFolder(folder NamedFolder) ([]storage.Object, []storage.Folder, error) {
	objects, subFolders, err := folder.ListFolder()
	if err != nil {
		mf.statsCollector.ReportOperationResult(folder.StorageName, stats.OperationList, false)
		return nil, nil, fmt.Errorf("list folder in storage %q: %w", folder.StorageName, err)
	}
	mf.statsCollector.ReportOperationResult(folder.StorageName, stats.OperationList, true)

	for i, obj := range objects {
		objects[i] = multiObject{
			Object:      obj,
			storageName: folder.StorageName,
		}
	}

	for i, subFolder := range subFolders {
		namedSubFolders := make([]NamedFolder, len(mf.usedFolders))
		for j, f := range mf.usedFolders {
			namedSubFolders[j] = f.GetSubFolder(path.Base(subFolder.GetPath()))
		}

		storageRoot := mf.configuredRootFolders[folder.StorageName]
		relPath := strings.TrimPrefix(subFolder.GetPath(), storageRoot.GetPath())
		relPath = strings.TrimPrefix(relPath, "/")
		subFolders[i] = Folder{
			statsCollector:        mf.statsCollector,
			configuredRootFolders: mf.configuredRootFolders,
			usedFolders:           namedSubFolders,
			path:                  relPath,
			policies:              mf.policies,
		}
	}

	return objects, subFolders, nil
}

func (mf Folder) PutObject(name string, content io.Reader) error {
	return mf.PutObjectWithContext(context.Background(), name, content)
}

// PutObjectWithContext puts the object to multiple storages.
// A specific implementation is selected using policies.Policies
func (mf Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	switch mf.policies.Put {
	case policies.PutPolicyFirst:
		return mf.PutObjectToFirst(ctx, name, content)
	case policies.PutPolicyUpdateFirstFound:
		return mf.PutObjectOrUpdateFirstFound(ctx, name, content)
	case policies.PutPolicyAll:
		return mf.PutObjectToAll(ctx, name, content)
	case policies.PutPolicyUpdateAllFound:
		return mf.PutObjectOrUpdateAllFound(ctx, name, content)
	default:
		panic(fmt.Sprintf("unknown put policy %d", mf.policies.Put))
	}
}

// PutObjectToFirst puts the object to the first storage.
func (mf Folder) PutObjectToFirst(ctx context.Context, name string, content io.Reader) error {
	if len(mf.usedFolders) == 0 {
		return ErrNoUsedStorages
	}
	return mf.usedFolders[0].PutObjectWithContext(ctx, name, content)
}

// PutObjectOrUpdateFirstFound updates the object in the first storage where it is found. If it's not found anywhere,
// uploads a new object to the first storage.
func (mf Folder) PutObjectOrUpdateFirstFound(ctx context.Context, name string, content io.Reader) error {
	if len(mf.usedFolders) == 0 {
		return ErrNoUsedStorages
	}
	countContent := newCountReader(content)
	for _, f := range mf.usedFolders {
		exists, err := f.Exists(name)
		if err != nil {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationExists, false)
			return fmt.Errorf("check file for existence in %q: %w", f.StorageName, err)
		}
		if exists {
			err = f.PutObjectWithContext(ctx, name, countContent)
			if err != nil {
				mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(countContent.ReadBytes()), false)
				return fmt.Errorf("put object to %q: %w", f.StorageName, err)
			}
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(countContent.ReadBytes()), true)
			return nil
		}
	}
	first := mf.usedFolders[0]
	err := first.PutObjectWithContext(ctx, name, countContent)
	if err != nil {
		mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationPut(countContent.ReadBytes()), false)
		return fmt.Errorf("put object to %q: %w", first.StorageName, err)
	}
	return nil
}

// PutObjectToAll puts the object to all used storages.
func (mf Folder) PutObjectToAll(ctx context.Context, name string, content io.Reader) error {
	var buffer []byte
	if len(mf.usedFolders) > 1 {
		var err error
		buffer, err = io.ReadAll(content)
		if err != nil {
			return fmt.Errorf("read file content to save in a temporary buffer: %w", err)
		}
	}
	for _, f := range mf.usedFolders {
		if buffer != nil {
			content = bytes.NewReader(buffer)
		}
		bufferSize := int64(len(buffer))
		err := f.PutObjectWithContext(ctx, name, content)
		if err != nil {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(bufferSize), false)
			return fmt.Errorf("put object to storage %q: %w", f.StorageName, err)
		}
		mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(bufferSize), true)
	}
	return nil
}

// PutObjectOrUpdateAllFound updates the object in all storages where it is found. If it's not found anywhere, uploads a
// new object to the first storage.
func (mf Folder) PutObjectOrUpdateAllFound(ctx context.Context, name string, content io.Reader) error {
	if len(mf.usedFolders) == 0 {
		return ErrNoUsedStorages
	}

	var buffer []byte
	if len(mf.usedFolders) > 1 {
		var err error
		buffer, err = io.ReadAll(content)
		if err != nil {
			return fmt.Errorf("read file content to save in a temporary buffer: %w", err)
		}
	}
	bufferSize := int64(len(buffer))

	var found bool
	for _, f := range mf.usedFolders {
		exists, err := f.Exists(name)
		if err != nil {
			return fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			if buffer != nil {
				content = bytes.NewReader(buffer)
			}
			err = f.PutObjectWithContext(ctx, name, content)
			if err != nil {
				mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(bufferSize), false)
				return fmt.Errorf("put object to storage %q: %w", f.StorageName, err)
			}
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationPut(bufferSize), true)
			found = true
		}
	}
	if !found {
		if buffer != nil {
			content = bytes.NewReader(buffer)
		}
		first := mf.usedFolders[0]
		err := first.PutObjectWithContext(ctx, name, content)
		if err != nil {
			mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationPut(bufferSize), false)
			return fmt.Errorf("put object to storage %q: %w", first.StorageName, err)
		}
		mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationPut(bufferSize), true)
		return nil
	}

	return nil
}

// DeleteObjects deletes the objects from multiple storages. A specific implementation is selected using
// policies.Policies
func (mf Folder) DeleteObjects(objectRelativePaths []string) error {
	switch mf.policies.Delete {
	case policies.DeletePolicyFirst:
		return mf.DeleteObjectsFromFirst(objectRelativePaths)
	case policies.DeletePolicyAll:
		return mf.DeleteObjectsFromAll(objectRelativePaths)
	default:
		panic(fmt.Sprintf("unknown delete policy %d", mf.policies.Delete))
	}
}

// DeleteObjectsFromFirst deletes the objects from the first storage.
func (mf Folder) DeleteObjectsFromFirst(objectRelativePaths []string) error {
	if len(mf.usedFolders) == 0 {
		return ErrNoUsedStorages
	}
	first := mf.usedFolders[0]
	filesNum := len(objectRelativePaths)
	err := first.DeleteObjects(objectRelativePaths)
	if err != nil {
		mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationDelete(filesNum), false)
		return fmt.Errorf("delete object from storage %q: %w", first.StorageName, err)
	}
	mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationDelete(filesNum), true)
	return nil
}

// DeleteObjectsFromAll deletes the objects from all used storages.
func (mf Folder) DeleteObjectsFromAll(objectRelativePaths []string) error {
	filesNum := len(objectRelativePaths)
	for _, f := range mf.usedFolders {
		err := f.DeleteObjects(objectRelativePaths)
		if err != nil {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationDelete(filesNum), false)
			return fmt.Errorf("delete objects from storage %q: %w", f.StorageName, err)
		}
		mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationDelete(filesNum), true)
	}
	return nil
}

// CopyObject copies the object in multiple storages. A specific implementation is selected using policies.Policies
func (mf Folder) CopyObject(srcPath string, dstPath string) error {
	switch mf.policies.Copy {
	case policies.CopyPolicyFirst:
		return mf.CopyObjectInFirst(srcPath, dstPath)
	case policies.CopyPolicyAll:
		return mf.CopyObjectInAll(srcPath, dstPath)
	default:
		panic(fmt.Sprintf("unknown copy policy %d", mf.policies.Copy))
	}
}

// CopyObjectInFirst copies the object in the first storage.
func (mf Folder) CopyObjectInFirst(srcPath string, dstPath string) error {
	if len(mf.usedFolders) == 0 {
		return ErrNoUsedStorages
	}
	first := mf.usedFolders[0]
	err := first.CopyObject(srcPath, dstPath)
	if err != nil {
		_, notFound := err.(storage.ObjectNotFoundError)
		if !notFound {
			mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationCopy, false)
			return fmt.Errorf("copy object in storage %q: %w", first.StorageName, err)
		}
	}
	mf.statsCollector.ReportOperationResult(first.StorageName, stats.OperationCopy, true)
	return err
}

// CopyObjectInAll copies the object in all used storages. If no storages have the object, an error is returned.
func (mf Folder) CopyObjectInAll(srcPath string, dstPath string) error {
	found := false
	for _, f := range mf.usedFolders {
		err := f.CopyObject(srcPath, dstPath)
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationCopy, true)
			continue
		}
		if err != nil {
			mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationCopy, false)
			return fmt.Errorf("copy object in storage %q: %w", f.StorageName, err)
		}
		mf.statsCollector.ReportOperationResult(f.StorageName, stats.OperationCopy, true)
		found = true
	}
	if !found {
		return storage.NewObjectNotFoundError(srcPath)
	}
	return nil
}

func (mf Folder) Validate() error {
	errs := make([]error, 0)
	for _, folder := range mf.usedFolders {
		err := folder.Validate()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == len(mf.usedFolders) {
		return ErrNoAliveStorages
	}
	tracelog.WarningLogger.Printf("Some storages can`t be accessed %v", errors.Join(errs...))
	return nil
}

var (
	ErrNoUsedStorages  = fmt.Errorf("no storages are used")
	ErrNoAliveStorages = fmt.Errorf("no alive storages")
)
