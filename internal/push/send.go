package push

import (
	"context"
	"sync"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// run sends the jobs inline and returns their results in request order. Each provider group of the call has its own
// fixed worker pool. The pools end with the call, so no goroutine outlives it except the provider calls, each bounded by
// its request timeout.
func (s *Sender) run(ctx context.Context, jobs []*provider.Job) []*apiproto.PushItemResult {
	results := make([]*apiproto.PushItemResult, len(jobs))

	// syncCtx ends the wait for provider slots: no call starts after it. The calls themselves are not stopped by it.
	syncCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.syncTimeout)
	defer cancel()

	groups := make(map[string][]int)
	for index, job := range jobs {
		name := job.Target.GetProvider()
		groups[name] = append(groups[name], index)
	}

	var wg sync.WaitGroup
	for name, indexes := range groups {
		wg.Add(1)
		go func(size int, indexes []int) {
			defer wg.Done()
			s.pool(syncCtx, jobs, results, indexes, size)
		}(s.poolSizes[name], indexes)
	}
	wg.Wait()
	return results
}

// pool sends the jobs at indexes with size workers. Each worker takes one index at a time and writes results[index], so
// the results need no sort. The number of workers is bounded by the size and by the jobs of the group.
func (s *Sender) pool(syncCtx context.Context, jobs []*provider.Job, results []*apiproto.PushItemResult, indexes []int, size int) {
	if size > len(indexes) {
		size = len(indexes)
	}

	queue := make(chan int, len(indexes))
	for _, index := range indexes {
		queue <- index
	}
	close(queue)

	var wg sync.WaitGroup
	for worker := 0; worker < size; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range queue {
				results[index] = s.set.Dispatch(syncCtx, s.requestTimeout, jobs[index])
			}
		}()
	}
	wg.Wait()
}
