package internal

import (
	"context"
	"sync"
)

type ExecFunc func(ctx context.Context) OpInfo

func ExecStage(ctx context.Context, fc <-chan ExecFunc, workers, resSize int, wg *sync.WaitGroup) <-chan OpInfo {
	resc := make(chan OpInfo, resSize)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(resc)

		wgwrk := sync.WaitGroup{}
		wgwrk.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wgwrk.Done()
				for f := range fc {
					select {
					case resc <- f(ctx):
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		wgwrk.Wait()
	}()

	return resc
}
