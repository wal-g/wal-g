package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
)

type ObjectWalker interface {
	WalkObjects(context.Context, func(Object) error) error
}

func WalkObjects(ctx context.Context, folder Folder, visit func(Object) error) error {
	if walker, ok := folder.(ObjectWalker); ok {
		return walker.WalkObjects(ctx, visit)
	}
	var walk func(Folder) error
	walk = func(current Folder) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		objects, children, err := current.ListFolder(ctx)
		if err != nil {
			return err
		}
		prefix := strings.TrimPrefix(current.GetPath(), folder.GetPath())
		for _, object := range objects {
			name := path.Join(prefix, strings.TrimLeft(object.GetName(), "/"))
			if name == ".." || strings.HasPrefix(name, "../") {
				return fmt.Errorf("listing escaped folder: %q", name)
			}
			if err := visit(NewLocalObjectWithVersion(name, object.GetLastModified(), object.GetSize(),
				object.GetVersionID(), object.GetAdditionalInfo())); err != nil {
				return err
			}
		}
		for _, child := range children {
			if child.GetPath() == current.GetPath() || !strings.HasPrefix(child.GetPath(), current.GetPath()) {
				return fmt.Errorf("invalid subfolder %q of %q", child.GetPath(), current.GetPath())
			}
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(folder)
}
