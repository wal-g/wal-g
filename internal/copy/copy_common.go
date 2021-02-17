package copy

import (
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
		<-tickets
		wg.Add(1)

		go func(handler InfoProvider) {
			defer wg.Done()
			err := handler.copyObject()
			tracelog.DebugLogger.PrintOnError(err)
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

	tracelog.DebugLogger.Printf("fetched object %s reader\n", ch.SrcObj.GetName())

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
	filter func(storage.Object) bool, renameFunc func(object storage.Object) string) (infos []InfoProvider) {
	tracelog.DebugLogger.Println("processing copy infos filtering")

	for _, object := range objects {
		if filter(object) {
			infos = append(infos, InfoProvider{
				From:       from,
				To:         to,
				SrcObj:     object,
				targetName: renameFunc(object),
			})
			tracelog.DebugLogger.Printf("add copy info %s-%s \n", object.GetName(), renameFunc(object))
		}
	}
	return
}
