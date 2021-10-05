package storage

import (
	"container/heap"
	"github.com/wal-g/tracelog"
	"sync"
)

// This code is mostly copy of https://github.com/tejzpr/ordered-concurrently (APACHE v2 License)
// with slightly changed interfaces. Waiting for generics...

// Runnable interface
type Runnable = func() []byte

type orderedInput struct {
	work Runnable
	idx  int
}

type orderedOutput struct {
	result []byte
	idx    int
}

func Process(in <-chan Runnable, out chan<- []byte, concurrencyFactor int) {
	if concurrencyFactor <= 0 {
		concurrencyFactor = 1
	}

	workQueue := make(chan orderedInput)
	aggregateQueue := make(chan orderedOutput)

	go func() {
		// start working goroutine pool:
		wg := sync.WaitGroup{}
		wg.Add(concurrencyFactor)
		for i := 0; i < concurrencyFactor; i++ {
			go func() {
				for {
					input, ok := <-workQueue
					if !ok {
						wg.Add(1)
						return
					}
					result := input.work()
					aggregateQueue <- orderedOutput{
						result: result,
						idx:    input.idx,
					}
				}
			}()
		}
		go func() {
			wg.Wait()
			close(aggregateQueue)
		}()

		// start aggregator goroutine:
		go func() {
			outHeap := &orderedOutputHeap{}
			currentIdx := 0
			for output := range aggregateQueue {
				heap.Push(outHeap, output)
				for top, ok := outHeap.Peek(); ok && top.idx == currentIdx; {
					tracelog.InfoLogger.Printf("Aggregator publishes #%d, size=%d", top.idx, len(top.result))
					out <- heap.Pop(outHeap).(orderedOutput).result
					currentIdx++
				}
			}
			for outHeap.Len() > 0 {
				output := heap.Pop(outHeap).(orderedOutput)
				tracelog.InfoLogger.Printf("Aggregator publishes #%d, size=%d", output.idx, len(output.result))
				out <- output.result
			}
		}()

		// feed working tasks to working pool:
		i := 0
		for {
			runnable, ok := <-in
			if !ok {
				break
			}
			workQueue <- orderedInput{
				work: runnable,
				idx:  i,
			}
			i += 1
		}
		close(workQueue)

	}()
}

// min-heap of orderedOutput
// https://pkg.go.dev/container/heap
type orderedOutputHeap []orderedOutput

func (h orderedOutputHeap) Len() int           { return len(h) }
func (h orderedOutputHeap) Less(i, j int) bool { return h[i].idx < h[j].idx }
func (h orderedOutputHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *orderedOutputHeap) Push(x interface{}) {
	*h = append(*h, x.(orderedOutput))
}
func (h *orderedOutputHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h orderedOutputHeap) Peek() (orderedOutput, bool) {
	if len(h) > 0 {
		return h[0], true
	}
	return orderedOutput{}, false
}
