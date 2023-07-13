package multistorage

import (
	"fmt"
	"io"
	"path"

	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func UseAllAliveStorages(folder storage.Folder) error {
	mf, ok := folder.(*multiFolder)
	if !ok {
		return nil
	}

	storages, err := mf.cache.AllAliveStorages()
	if err != nil {
		return fmt.Errorf("select all alive storages in multistorage folder: %w", err)
	}
	if len(storages) == 0 {
		return fmt.Errorf("no alive storages")
	}
	mf.storages = changeDirectory(mf.path, storages...)
	return nil
}

func UseFirstAliveStorage(folder storage.Folder) error {
	mf, ok := folder.(*multiFolder)
	if !ok {
		return nil
	}

	firstStorage, err := mf.cache.FirstAliveStorage()
	if err != nil {
		return fmt.Errorf("select first alive storage in multistorage folder: %w", err)
	}
	if firstStorage == nil {
		return fmt.Errorf("no alive storages")
	}
	mf.storages = changeDirectory(mf.path, *firstStorage)
	return nil
}

func UseSpecificStorage(name string, folder storage.Folder) error {
	mf, ok := folder.(*multiFolder)
	if !ok {
		return nil
	}

	specificStorage, err := mf.cache.SpecificStorage(name)
	if err != nil {
		return fmt.Errorf("select storage %q in multistorage folder: %w", name, err)
	}
	mf.storages = changeDirectory(mf.path, specificStorage)
	return nil
}

func SetPolicies(folder storage.Folder, policies policies.Policies) {
	if mf, ok := folder.(*multiFolder); ok {
		mf.policies = policies
	}
}

func changeDirectory(path string, storages ...cache.NamedFolder) []cache.NamedFolder {
	for _, s := range storages {
		s.Folder = s.Folder.GetSubFolder(path)
	}
	return storages
}

type Folder interface {
	storage.Folder

	ExistsInFirst(objectRelativePath string) (bool, string, error)
	ExistsInAny(objectRelativePath string) (bool, string, error)
	ExistsInAll(objectRelativePath string) (bool, string, error)

	ReadObjectFromFirst(objectRelativePath string) (io.ReadCloser, string, error)
	ReadObjectFoundFirst(objectRelativePath string) (io.ReadCloser, string, error)

	ListFolderInFirst() (objects []storage.Object, subFolders []storage.Folder, err error)
	ListFolderWhereFoundFirst() (objects []storage.Object, subFolders []storage.Folder, err error)
	ListFolderAll() (objects []storage.Object, subFolders []storage.Folder, err error)

	PutObjectToFirst(name string, content io.Reader) error
	PutObjectOrUpdateFirstFound(name string, content io.Reader) error
	PutObjectToAll(name string, content io.Reader) error
	PutObjectOrUpdateAllFound(name string, content io.Reader) error

	DeleteObjectsFromFirst(objectRelativePaths []string) error
	DeleteObjectsFromAll(objectRelativePaths []string) error

	CopyObjectInFirst(srcPath string, dstPath string) error
	CopyObjectInAll(srcPath string, dstPath string) error
}

func NewFolder(cache *cache.StatusCache) Folder {
	return &multiFolder{
		cache:    cache,
		policies: policies.Default,
	}
}

type multiFolder struct {
	cache    *cache.StatusCache
	storages []cache.NamedFolder
	path     string
	policies policies.Policies
}

// GetPath provides the base path that is common for all the storages.
func (mf *multiFolder) GetPath() string {
	return mf.storages[0].GetPath()
}

// GetSubFolder provides a multi-storage subfolder, which includes subfolders of all used storages.
func (mf *multiFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	subfolder := &multiFolder{
		cache:    mf.cache,
		storages: mf.storages,
		path:     path.Join(mf.path, subFolderRelativePath),
		policies: mf.policies,
	}
	mf.storages = changeDirectory(subFolderRelativePath, mf.storages...)
	return subfolder
}

// Exists checks if the object exists in multiple storages. A specific implementation is selected using
// FolderPolicies.Exists.
func (mf *multiFolder) Exists(objectRelativePath string) (bool, error) {
	exists, _, err := Exists(mf, objectRelativePath)
	return exists, err
}

// Exists is like storage.Folder.Exists, but it also provides the name of the storage where the file is found. If it's
// not found, storage name is empty. If it's found in all storages, provides "all" as the storage name.
func Exists(folder storage.Folder, objectRelativePath string) (found bool, storage string, err error) {
	mf, ok := folder.(*multiFolder)
	if !ok {
		exists, err := folder.Exists(objectRelativePath)
		return exists, DefaultStorage, err
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
func (mf *multiFolder) ExistsInFirst(objectRelativePath string) (bool, string, error) {
	if len(mf.storages) == 0 {
		return false, "", fmt.Errorf("no storages are used")
	}
	first := mf.storages[0]
	exists, err := first.Exists(objectRelativePath)
	return exists, first.Name, err
}

// ExistsInAny checks if the object exists in any storage.
func (mf *multiFolder) ExistsInAny(objectRelativePath string) (bool, string, error) {
	for _, s := range mf.storages {
		exists, err := s.Exists(objectRelativePath)
		if err != nil {
			return false, s.Name, fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			return true, s.Name, nil
		}
	}
	return false, "all", nil
}

// ExistsInAll checks if the object exists in all used storages.
func (mf *multiFolder) ExistsInAll(objectRelativePath string) (bool, string, error) {
	for _, s := range mf.storages {
		exists, err := s.Exists(objectRelativePath)
		if err != nil {
			return false, s.Name, fmt.Errorf("check for existence: %w", err)
		}
		if !exists {
			return false, s.Name, nil
		}
	}
	return true, "all", nil
}

// ReadObject reads the object from multiple storages. A specific implementation is selected using FolderPolicies.Read.
func (mf *multiFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	file, _, err := ReadObject(mf, objectRelativePath)
	return file, err
}

// ReadObject is like storage.Folder.ReadObject, but it also provides the name of storage where the file is read from.
func ReadObject(folder storage.Folder, objectRelativePath string) (io.ReadCloser, string, error) {
	mf, ok := folder.(*multiFolder)
	if !ok {
		file, err := folder.ReadObject(objectRelativePath)
		return file, DefaultStorage, err
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
func (mf *multiFolder) ReadObjectFromFirst(objectRelativePath string) (io.ReadCloser, string, error) {
	if len(mf.storages) == 0 {
		return nil, "", fmt.Errorf("no storages are used")
	}
	first := mf.storages[0]
	file, err := first.ReadObject(objectRelativePath)
	return file, first.Name, err
}

// ReadObjectFoundFirst reads the object from all used storages in order and returns the first one found.
func (mf *multiFolder) ReadObjectFoundFirst(objectRelativePath string) (io.ReadCloser, string, error) {
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
	return nil, "all", storage.NewObjectNotFoundError(objectRelativePath)
}

// ListFolder lists the folder in multiple storages. A specific implementation is selected using FolderPolicies.List.
func (mf *multiFolder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
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
func (mf *multiFolder) ListFolderInFirst() (objects []storage.Object, subFolders []storage.Folder, err error) {
	if len(mf.storages) == 0 {
		return nil, nil, fmt.Errorf("no storages are used")
	}
	objects, subFolders, err = mf.storages[0].ListFolder()
	if err != nil {
		return nil, nil, err
	}
	for i := range subFolders {
		subFolders[i] = &multiFolder{
			cache:    mf.cache,
			storages: mf.storages,
			path:     subFolders[i].GetPath(),
			policies: mf.policies,
		}
	}
	return objects, subFolders, nil
}

// ListFolderWhereFoundFirst lists the folder in all used storages and provides a result where each file is taken from
// the first storage in which it was found.
func (mf *multiFolder) ListFolderWhereFoundFirst() (objects []storage.Object, subFolders []storage.Folder, err error) {
	objects, subFolders, err = mf.storages[0].ListFolder()
	if err != nil {
		return nil, nil, err
	}
	for i := range subFolders {
		subFolders[i] = &multiFolder{
			cache:    mf.cache,
			storages: mf.storages,
			path:     subFolders[i].GetPath(),
			policies: mf.policies,
		}
	}
	return objects, subFolders, nil
}

// ListFolderAll lists every used storage and provides the union of all found files.
func (mf *multiFolder) ListFolderAll() (objects []storage.Object, subFolders []storage.Folder, err error) {
	for _, s := range mf.storages {
		curObjects, curSubFolders, err := s.ListFolder()
		if err != nil {
			return nil, nil, fmt.Errorf("list folder: %w", err)
		}
		objects = append(objects, curObjects...)
		for _, sf := range curSubFolders {
			subFolders = append(subFolders, &multiFolder{
				cache:    mf.cache,
				storages: mf.storages,
				path:     sf.GetPath(),
				policies: mf.policies,
			})
		}
	}
	return objects, subFolders, nil
}

// PutObject puts the object to multiple storages. A specific implementation is selected using FolderPolicies.Put.
func (mf *multiFolder) PutObject(name string, content io.Reader) error {
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
func (mf *multiFolder) PutObjectToFirst(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return fmt.Errorf("no storages are used")
	}
	return mf.storages[0].PutObject(name, content)
}

// PutObjectOrUpdateFirstFound updates the object in the first storage where it is found. If it's not found anywhere,
// uploads a new object to the first storage.
func (mf *multiFolder) PutObjectOrUpdateFirstFound(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return fmt.Errorf("no storages are used")
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
func (mf *multiFolder) PutObjectToAll(name string, content io.Reader) error {
	for _, s := range mf.storages {
		err := s.PutObject(name, content)
		if err != nil {
			return fmt.Errorf("put object to storage %q: %w", s.Name, err)
		}
	}
	return nil
}

// PutObjectOrUpdateAllFound updates the object in all storages where it is found. If it's not found anywhere, uploads a
// new object to the first storage.
func (mf *multiFolder) PutObjectOrUpdateAllFound(name string, content io.Reader) error {
	if len(mf.storages) == 0 {
		return fmt.Errorf("no storages are used")
	}
	var found bool
	for _, s := range mf.storages {
		exists, err := s.Exists(name)
		if err != nil {
			return fmt.Errorf("check for existence: %w", err)
		}
		if exists {
			err = s.PutObject(name, content)
			if err != nil {
				return fmt.Errorf("put object to storage %q: %w", s.Name, err)
			}
			found = true
		}
	}
	if !found {
		return mf.storages[0].PutObject(name, content)
	}
	return nil
}

// DeleteObjects deletes the objects from multiple storages. A specific implementation is selected using
// FolderPolicies.Delete.
func (mf *multiFolder) DeleteObjects(objectRelativePaths []string) error {
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
func (mf *multiFolder) DeleteObjectsFromFirst(objectRelativePaths []string) error {
	if len(mf.storages) == 0 {
		return fmt.Errorf("no storages are used")
	}
	return mf.storages[0].DeleteObjects(objectRelativePaths)
}

// DeleteObjectsFromAll deletes the objects from all used storages.
func (mf *multiFolder) DeleteObjectsFromAll(objectRelativePaths []string) error {
	for _, s := range mf.storages {
		err := s.DeleteObjects(objectRelativePaths)
		if err != nil {
			return fmt.Errorf("delete objects from storage %q: %w", s.Name, err)
		}
	}
	return nil
}

// CopyObject copies the object in multiple storages. A specific implementation is selected using FolderPolicies.Copy.
func (mf *multiFolder) CopyObject(srcPath string, dstPath string) error {
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
func (mf *multiFolder) CopyObjectInFirst(srcPath string, dstPath string) error {
	if len(mf.storages) == 0 {
		return fmt.Errorf("no storages are used")
	}
	return mf.storages[0].CopyObject(srcPath, dstPath)
}

// CopyObjectInAll copies the object in all used storages.
func (mf *multiFolder) CopyObjectInAll(srcPath string, dstPath string) error {
	for _, s := range mf.storages {
		err := s.CopyObject(srcPath, dstPath)
		if err != nil {
			return fmt.Errorf("copy object in storage %q: %w", s.Name, err)
		}
	}
	return nil
}
