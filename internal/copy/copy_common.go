package copy

import (
	"sort"
	"strings"
	"sync"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
)

type InfoProvider struct {
	From storage.Folder
	To   storage.Folder

	SrcObj     storage.Object
	targetName string
}

func Infos(chs []InfoProvider) error {
	maxParallelJobsCount := 8

	tickets := make(chan interface{}, maxParallelJobsCount)

	for t := 0; t < maxParallelJobsCount; t++ {
		tickets <- nil
	}

	errors := make(chan error, maxParallelJobsCount*2)
	var wg sync.WaitGroup

	for _, ch := range chs {

		// do we have any errs yet?
		for len(errors) > 0 {
			if err := <-errors; err != nil {
				return err
			}
		}

		// block here
		_ = <-tickets
		wg.Add(1)

		go func(handler InfoProvider) {
			defer wg.Done()
			err := handler.copyObject()
			tickets <- nil
			errors <- err
		}(ch)
	}

	wg.Wait()

	for len(errors) > 0 {
		if err := <-errors; err != nil {
			return err
		}
	}

	return nil
}

func (ch *InfoProvider) copyObject() error {

	readCloser, err := ch.From.ReadObject(ch.SrcObj.GetName())
	if err != nil {
		return err
	}

	err = ch.To.PutObject(ch.targetName, readCloser)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("Copied '%s' from folder '%s' to '%s' in fodler '%s'.", ch.SrcObj.GetName(), ch.From.GetPath(), ch.targetName, ch.To.GetPath())
	return nil
}

var NoopRenameFunc = func(o storage.Object) string {
	if o == nil {
		return ""
	}
	return o.GetName()
}

func BuildCopyingInfos(from storage.Folder, to storage.Folder, objects []storage.Object,
	condition func(storage.Object) bool, renameFunc func(object storage.Object) string, forceOverrite bool) (infos []InfoProvider) {

	sort.Slice(objects, func(i, j int) bool {
		// sort files in the order that guarantees that sentinel would be uploaded strictly after backup
		if strings.Contains(objects[i].GetName(), "json") {
			return false
		}
		if strings.Contains(objects[j].GetName(), "json") {
			return false
		}

		return objects[i].GetName() < objects[j].GetName()
	})

	for _, object := range objects {
		if condition(object) {
			if exits, err := to.Exists(object.GetName()); !forceOverrite && exits && err == nil {
				// do not overwrite files, if not explicitly requested to
				continue
			}
			infos = append(infos, InfoProvider{
				From:       from,
				To:         to,
				SrcObj:     object,
				targetName: renameFunc(object),
			})
		}
	}
	return
}
