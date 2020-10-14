package copy

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"path"
	"sync"
)

type InfoProvider struct {
	From storage.Folder
	To   storage.Folder

	Obj storage.Object
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
	var objectName = ch.Obj.GetName()

	readCloser, err := ch.From.ReadObject(objectName)
	if err != nil {
		return err
	}

	var filename = path.Join(ch.From.GetPath(), objectName)

	err = ch.To.PutObject(filename, readCloser)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", objectName, ch.From.GetPath(), ch.To.GetPath())
	return nil
}

func BuildCopyingInfos(from storage.Folder, to storage.Folder, objects []storage.Object,
	condition func(storage.Object) bool) (infos []InfoProvider) {
	for _, object := range objects {
		if condition(object) {
			infos = append(infos, InfoProvider{
				From: from,
				To:   to,
				Obj:  object,
			})
		}
	}
	return
}