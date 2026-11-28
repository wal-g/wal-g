package storage

import "context"

// ReadAccessChecker is implemented by folders that can verify list/read access with a bounded request whose cost
// does not depend on how many objects the folder contains.
type ReadAccessChecker interface {
	CheckReadAccess(ctx context.Context) error
}

// CheckReadAccess verifies that the folder can be listed. Folders implementing ReadAccessChecker use a bounded
// probe; others fall back to a full ListFolder.
func CheckReadAccess(ctx context.Context, folder Folder) error {
	if checker, ok := folder.(ReadAccessChecker); ok {
		return checker.CheckReadAccess(ctx)
	}
	_, _, err := folder.ListFolder(ctx)
	return err
}
