package push

import (
	"context"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/push/provider"
)

// run sends the jobs inline and returns their results in request order. Each provider group of the call has its own
// fixed worker pool. The pools end with the call, so no goroutine outlives it except the provider calls, each bounded by
// its request timeout and by the call deadline.
func (s *Sender) run(ctx context.Context, jobs []*provider.Job, callDeadline time.Time) []*apiproto.PushItemResult {
	results := make([]*apiproto.PushItemResult, len(jobs))
	shutdown := s.set.ShutdownContext()

	// callCtx ends at the call deadline, or when the node shuts down. The provider calls of the call end with it.
	callCtx, cancelCall := context.WithDeadline(context.WithoutCancel(ctx), callDeadline)
	defer cancelCall()
	stopCall := context.AfterFunc(shutdown, cancelCall)
	defer stopCall()

	// syncCtx ends the wait for provider slots at the sync timeout or the call deadline, whichever is first: no call
	// starts after it. The calls themselves are not stopped by it.
	syncCtx, cancelSync := context.WithDeadline(
		context.WithoutCancel(ctx),
		earlierOf(callDeadline, time.Now().Add(s.syncTimeout)),
	)
	defer cancelSync()
	stopSync := context.AfterFunc(shutdown, cancelSync)
	defer stopSync()

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
			s.pool(syncCtx, callCtx, jobs, results, indexes, size)
		}(s.poolSizes[name], indexes)
	}
	wg.Wait()
	return results
}

// earlierOf returns the earlier of two times.
func earlierOf(first time.Time, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}

// pool sends the jobs at indexes with size workers. Each worker takes one index at a time and writes results[index], so
// the results need no sort. The number of workers is bounded by the size and by the jobs of the group.
func (s *Sender) pool(
	syncCtx context.Context,
	callCtx context.Context,
	jobs []*provider.Job,
	results []*apiproto.PushItemResult,
	indexes []int,
	size int,
) {
	size = min(size, len(indexes))

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
				results[index] = s.set.DispatchWindow(syncCtx, callCtx, s.requestTimeout, jobs[index])
			}
		}()
	}
	wg.Wait()
}
