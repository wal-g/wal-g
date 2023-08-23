package multistorage

import (
	"bytes"
	"fmt"
	"io"
	"path"

	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// UseAllAliveStorages makes a copy of the Folder that uses all currently alive storages.
func UseAllAliveStorages(folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	storages, err := mf.cache.AllAliveStorages()
	if err != nil {
		return nil, fmt.Errorf("select all alive storages in multistorage folder: %w", err)
	}
	if len(storages) == 0 {
		return nil, ErrNoAliveStorages
	}
	mf.storages = changeDirectory(mf.path, storages...)
	return mf, nil
}

// UseFirstAliveStorage makes a copy of the Folder that uses a single storage, that is first alive in the list.
// This is an error if all storages are dead.
func UseFirstAliveStorage(folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	firstStorage, err := mf.cache.FirstAliveStorage()
	if err != nil {
		return nil, fmt.Errorf("select first alive storage in multistorage folder: %w", err)
	}
	if firstStorage == nil {
		return nil, ErrNoAliveStorages
	}
	mf.storages = changeDirectory(mf.path, *firstStorage)
	return mf, nil
}

// UseSpecificStorage makes a copy of the Folder that uses storage with the specified name.
// This is an error if the storage is dead.
func UseSpecificStorage(name string, folder storage.Folder) (storage.Folder, error) {
	mf, ok := folder.(Folder)
	if !ok {
		return folder, nil
	}

	alreadyUsed := len(mf.storages) == 1 && mf.storages[0].Name == name
	if alreadyUsed {
		return mf, nil
	}

	specificStorage, err := mf.cache.SpecificStorage(name)
	if err != nil {
		return nil, fmt.Errorf("select storage %q in multistorage folder: %w", name, err)
	}
	if specificStorage == nil {
		return nil, ErrNoAliveStorages
	}
	mf.storages = changeDirectory(mf.path, *specificStorage)
	return mf, nil
}

func UsedStorages(folder storage.Folder) []string {
	mf, ok := folder.(Folder)
	if !ok {
		return []string{consts.DefaultStorage}
	}

	var storageNames []string
	for _, s := range mf.storages {
		storageNames = append(storageNames, s.Name)
	}
	return storageNames
}

func EnsureSingleStorageIsUsed(folder storage.Folder) error {
	storages := UsedStorages(folder)
	if len(storages) != 1 {
		return fmt.Errorf("multi-storage folder must use a single storage, but it uses %d", len(storages))
	}
	return nil
}

func changeDirectory(path string, storages ...cache.NamedFolder) []cache.NamedFolder {
	newStorages := make([]cache.NamedFolder, len(storages))
	for i := range storages {
		newStorages[i] = cache.NamedFolder{
			Name:   storages[i].Name,
			Folder: storages[i].Folder.GetSubFolder(path),
		}
	}
	return newStorages
}

// SetPolicies makes a copy of the Folder that uses new policies.Policies.
func SetPolicies(folder storage.Folder, policies policies.Policies) storage.Folder {
	if mf, ok := folder.(Folder); ok {
		mf.policies = policies
		return mf
	}
	return folder
}

func NewFolder(cache cache.StatusCache) storage.Folder {
	return Folder{
		cache:    cache,
		policies: policies.Default,
	}
}

// Folder represents a multi-storage folder that aggregates several folders from different storages with the same path.
// A specific behavior (should the folders be united, merged, etc.) is selected by policies.Policies.
type Folder struct {
	cache    cache.StatusCache
	storages []cache.NamedFolder
	path     string
	policies policies.Policies
}

// GetPath provides the base path that is common for all the storages.
func (mf Folder) GetPath() string {
	return mf.path
}

// GetSubFolder provides a multi-storage subfolder, which includes subfolders of all used storages.
func (mf Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	subfolder := Folder{
		cache:    mf.cache,
		path:     path.Join(mf.path, subFolderRelativePath) + "/",
		policies: mf.policies,
	}
	subfolder.storages = changeDirectory(subFolderRelativePath, mf.storages...)
	return subfolder
}

// Exists checks if the object exists in multiple storages. A specific implementation is selected using
// FolderPolicies.Exists.
func (mf Folder) Exists(objectRelativePath string) (bool, error) {
	exists, _, err := Exists(mf, objectRelativePath)
	return exists, err
}

// Exists is like storage.Folder.Exists, but it also provides the name of the storage where the file is found. If it's
// not found, storage name is empty. If it's found in all storages, provides "all" as the storage name.
func Exists(folder storage.Folder, objectRelativePath string) (found bool, storage string, err error) {
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
	if len(mf.storages) == 0 {
		return false, "", ErrNoUsedStorages
	}
	first := mf.storages[0]
	exists, err := first.Exists(objectRelativePath)
	return exists, first.Name, err
}

// ExistsInAny checks if the object exists in any storage.
func (mf Folder) ExistsInAny(objectRelativePath string) (bool, string, error) {
	for _, s := range mf.storages {
		exists, err := s.Exists(objectRelativePath)
		if err != nil {
			return false, s.Name, fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			return true, s.Name, nil
		}
	}
	return false, consts.AllStorages, nil
}

// ExistsInAll checks if the object exists in all used storages.
func (mf Folder) ExistsInAll(objectRelativePath string) (bool, string, error) {
	for _, s := range mf.storages {
		exists, err := s.Exists(objectRelativePath)
		if err != nil {
			return false, s.Name, fmt.Errorf("check for existence: %w", err)
		}
		if !exists {
			return false, s.Name, nil
		}
	}
	return true, consts.AllStorages, nil
}

// ReadObject reads the object from multiple storages. A specific implementation is selected using FolderPolicies.Read.
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
	if len(mf.storages) == 0 {
		return nil, "", ErrNoUsedStorages
	}
	first := mf.storages[0]
	file, err := first.ReadObject(objectRelativePath)
	return file, first.Name, err
}

// ReadObjectFoundFirst reads the object from all used storages in order and returns the first one found.
func (mf Folder) ReadObjectFoundFirst(objectRelativePath string) (io.ReadCloser, string, error) {
	for _, s := range mf.storages {
		exists, err := s.Exists(objectRelativePath)
		if err != nil {
			return nil, s.Name, fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			file, err := s.ReadObject(objectRelativePath)
			return file, s.Name, err
		}
	}
	return nil, consts.AllStorages, storage.NewObjectNotFoundError(objectRelativePath)
}

// ListFolder lists the folder in multiple storages. A specific implementation is selected using FolderPolicies.List.
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
	if len(mf.storages) == 0 {
		return nil, nil, ErrNoUsedStorages
	}
	return mf.listStorageFolder(mf.storages[0])
}

// ListFolderWhereFoundFirst lists the folder in all used storages and provides a result where each file is taken from
// the first storage in which it was found.
func (mf Folder) ListFolderWhereFoundFirst() (objects []storage.Object, subFolders []storage.Folder, err error) {
	metObjects := map[string]bool{}
	metSubFolders := map[string]bool{}

	for _, st := range mf.storages {
		curObjects, curSubFolders, err := mf.listStorageFolder(st)
		if err != nil {
			return nil, nil, fmt.Errorf("list storage %q folder %q: %w", st.Name, mf.path, err)
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

	for _, st := range mf.storages {
		curObjects, curSubFolders, err := mf.listStorageFolder(st)
		if err != nil {
			return nil, nil, fmt.Errorf("list storage %q folder %q: %w", st.Name, mf.path, err)
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

func (mf Folder) listStorageFolder(storage cache.NamedFolder) ([]storage.Object, []storage.Folder, error) {
	objects, subFolders, err := storage.ListFolder()
	if err != nil {
		return nil, nil, err
	}

	for i, obj := range objects {
		objects[i] = multiObject{
			Object:      obj,
			storageName: storage.Name,
		}
	}

	for i, subFolder := range subFolders {
		newStorages := make([]cache.NamedFolder, len(mf.storages))
		for j, st := range mf.storages {
			newStorages[j] = cache.NamedFolder{
				Name:   st.Name,
				Folder: st.GetSubFolder(path.Base(subFolder.GetPath())),
			}
		}

		subFolders[i] = Folder{
			cache:    mf.cache,
			storages: newStorages,
			path:     subFolder.GetPath(),
			policies: mf.policies,
		}
	}

	return objects, subFolders, nil
}

// PutObject puts the object to multiple storages. A specific implementation is selected using FolderPolicies.Put.
func (mf Folder) PutObject(name string, content io.Reader) error {
	switch mf.policies.Put {
	case policies.PutPolicyFirst:
		return mf.PutObjectToFirst(name, content)
	case policies.PutPolicyUpdateFirstFound:
		return mf.PutObjectOrUpdateFirstFound(name, content)
	case policies.PutPolicyAll:
		return mf.PutObjectToAll(name, content)
	case policies.PutPolicyUpdateAllFound:
		return mf.PutObjectOrUpdateAllFound(name, content)
	default:
		panic(fmt.Sprintf("unknown put policy %d", mf.policies.Put))
	}
}

// PutObjectToFirst puts the object to the first storage.
func (mf Folder) PutObjectToFirst(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return ErrNoUsedStorages
	}
	return mf.storages[0].PutObject(name, content)
}

// PutObjectOrUpdateFirstFound updates the object in the first storage where it is found. If it's not found anywhere,
// uploads a new object to the first storage.
func (mf Folder) PutObjectOrUpdateFirstFound(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return ErrNoUsedStorages
	}
	for _, s := range mf.storages {
		exists, err := s.Exists(name)
		if err != nil {
			return fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			return s.PutObject(name, content)
		}
	}
	return mf.storages[0].PutObject(name, content)
}

// PutObjectToAll puts the object to all used storages.
func (mf Folder) PutObjectToAll(name string, content io.Reader) error {
	var buffer []byte
	if len(mf.storages) > 1 {
		var err error
		buffer, err = io.ReadAll(content)
		if err != nil {
			return fmt.Errorf("read file content to save in a temporary buffer: %w", err)
		}
	}
	for _, s := range mf.storages {
		if buffer != nil {
			content = bytes.NewReader(buffer)
		}
		err := s.PutObject(name, content)
		if err != nil {
			return fmt.Errorf("put object to storage %q: %w", s.Name, err)
		}
	}
	return nil
}

// PutObjectOrUpdateAllFound updates the object in all storages where it is found. If it's not found anywhere, uploads a
// new object to the first storage.
func (mf Folder) PutObjectOrUpdateAllFound(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return ErrNoUsedStorages
	}

	var buffer []byte
	if len(mf.storages) > 1 {
		var err error
		buffer, err = io.ReadAll(content)
		if err != nil {
			return fmt.Errorf("read file content to save in a temporary buffer: %w", err)
		}
	}

	var found bool
	for _, s := range mf.storages {
		exists, err := s.Exists(name)
		if err != nil {
			return fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			if buffer != nil {
				content = bytes.NewReader(buffer)
			}
			err = s.PutObject(name, content)
			if err != nil {
				return fmt.Errorf("put object to storage %q: %w", s.Name, err)
			}
			found = true
		}
	}
	if !found {
		if buffer != nil {
			content = bytes.NewReader(buffer)
		}
		return mf.storages[0].PutObject(name, content)
	}

	return nil
}

// DeleteObjects deletes the objects from multiple storages. A specific implementation is selected using
// FolderPolicies.Delete.
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
	if len(mf.storages) == 0 {
		return ErrNoUsedStorages
	}
	return mf.storages[0].DeleteObjects(objectRelativePaths)
}

// DeleteObjectsFromAll deletes the objects from all used storages.
func (mf Folder) DeleteObjectsFromAll(objectRelativePaths []string) error {
	for _, s := range mf.storages {
		err := s.DeleteObjects(objectRelativePaths)
		if err != nil {
			return fmt.Errorf("delete objects from storage %q: %w", s.Name, err)
		}
	}
	return nil
}

// CopyObject copies the object in multiple storages. A specific implementation is selected using FolderPolicies.Copy.
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
	if len(mf.storages) == 0 {
		return ErrNoUsedStorages
	}
	return mf.storages[0].CopyObject(srcPath, dstPath)
}

// CopyObjectInAll copies the object in all used storages. If no storages have the object, an error is returned.
func (mf Folder) CopyObjectInAll(srcPath string, dstPath string) error {
	found := false
	for _, s := range mf.storages {
		err := s.CopyObject(srcPath, dstPath)
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			continue
		}
		if err != nil {
			return fmt.Errorf("copy object in storage %q: %w", s.Name, err)
		}
		found = true
	}
	if !found {
		return storage.NewObjectNotFoundError(srcPath)
	}
	return nil
}

var (
	ErrNoUsedStorages  = fmt.Errorf("no storages are used")
	ErrNoAliveStorages = fmt.Errorf("no alive storages")
)
