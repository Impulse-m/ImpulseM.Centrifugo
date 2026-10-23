package provider

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
)

// initPushMetricsForTest registers the push metrics in a fresh registry, as a node with push enabled does.
func initPushMetricsForTest(t *testing.T) {
	t.Helper()

	err := metrics.Init(metrics.Config{Registerer: prometheus.NewRegistry(), PushEnabled: true})
	require.NoError(t, err)
}

// gaugeSender reads the in-flight gauge of its provider while its call runs.
type gaugeSender struct {
	provider string
	seen     float64
}

func (s *gaugeSender) Send(_ context.Context, job *Job) *apiproto.PushItemResult {
	s.seen = testutil.ToFloat64(metrics.PushDirectInflight.WithLabelValues(s.provider))
	return NewResult(job, StatusSent)
}

// failingSender fails the test when it is called.
type failingSender struct {
	t *testing.T
}

func (s failingSender) Send(_ context.Context, job *Job) *apiproto.PushItemResult {
	s.t.Errorf("a provider was called in dry run")
	return NewResult(job, StatusRejected)
}

// recordingSender counts its calls. When hold is set, it answers only when its context ends, recording the start and
// the end of the call.
type recordingSender struct {
	mu      sync.Mutex
	calls   int
	hold    bool
	entered chan struct{}
	release chan struct{}
	starts  []time.Time
	ends    []time.Time
}

func (s *recordingSender) Send(ctx context.Context, job *Job) *apiproto.PushItemResult {
	s.mu.Lock()
	s.calls++
	s.starts = append(s.starts, time.Now())
	s.mu.Unlock()

	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.hold {
		<-ctx.Done()
	} else if s.release != nil {
		<-s.release
	}

	s.mu.Lock()
	s.ends = append(s.ends, time.Now())
	s.mu.Unlock()

	return NewResult(job, StatusSent)
}

func (s *recordingSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newTestJob(index int, provider string) *Job {
	return &Job{
		Index: index,
		Target: &apiproto.PushTarget{
			Provider: provider,
			Token:    "token",
			Ref:      "ref",
		},
	}
}

func TestDispatchPendingAfterSyncDeadline(t *testing.T) {
	sender := &recordingSender{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	set := NewSet()
	set.Add(ProviderFCM, sender, 1)

	done := make(chan *apiproto.PushItemResult, 1)
	go func() {
		done <- set.Dispatch(context.Background(), time.Minute, newTestJob(0, ProviderFCM))
	}()
	<-sender.entered

	// The only slot is held, and the sync context has already ended: the second job must not call the provider.
	syncCtx, cancel := context.WithCancel(context.Background())
	cancel()
	result := set.Dispatch(syncCtx, time.Minute, newTestJob(1, ProviderFCM))

	require.Equal(t, StatusPending, result.GetStatus())
	require.Equal(t, uint32(1), result.GetIndex())
	require.Equal(t, "ref", result.GetRef())
	require.Equal(t, 1, sender.callCount())

	close(sender.release)
	require.Equal(t, StatusSent, (<-done).GetStatus())
}

func TestLateStartGetsFullTimeout(t *testing.T) {
	const requestTimeout = 300 * time.Millisecond

	sender := &recordingSender{hold: true}
	set := NewSet()
	set.Add(ProviderAPNs, sender, 1)

	syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			set.Dispatch(syncCtx, requestTimeout, newTestJob(index, ProviderAPNs))
		}(index)
	}
	wg.Wait()

	require.Equal(t, 2, sender.callCount())
	require.Len(t, sender.ends, 2)

	// The second call starts when the first one ends, and must still run for its own full timeout.
	for index := 0; index < 2; index++ {
		duration := sender.ends[index].Sub(sender.starts[index])
		require.GreaterOrEqual(t, duration, requestTimeout-50*time.Millisecond, "call %d", index)
	}
}

func TestDispatchRejectsUnknownProvider(t *testing.T) {
	set := NewSet()

	result := set.Dispatch(context.Background(), time.Minute, newTestJob(0, "hms"))

	require.Equal(t, StatusRejected, result.GetStatus())
	require.Empty(t, result.GetCode())
}

func TestDryRunResult(t *testing.T) {
	result := DryRun(newTestJob(3, ProviderAPNs))

	require.Equal(t, StatusDryRun, result.GetStatus())
	require.Equal(t, uint32(3), result.GetIndex())
	require.Equal(t, "ref", result.GetRef())
	require.Zero(t, result.GetHttpStatus())
	require.Empty(t, result.GetProviderMessageId())
}

func TestDryRunMakesNoRequest(t *testing.T) {
	initPushMetricsForTest(t)

	// The dry run goes through Dispatch, the send path of the set. The sender fails the test if it is ever called.
	set := NewDryRunSet()
	set.Add(ProviderAPNs, failingSender{t: t}, 1)

	result := set.Dispatch(context.Background(), time.Minute, newTestJob(0, ProviderAPNs))

	require.Equal(t, StatusDryRun, result.GetStatus())
	require.Equal(t, uint32(0), result.GetIndex())
	require.Zero(t, result.GetHttpStatus())
	require.Empty(t, result.GetProviderMessageId())
	require.Zero(t, testutil.ToFloat64(metrics.PushDirectInflight.WithLabelValues(ProviderAPNs)))
	require.Zero(t, testutil.CollectAndCount(metrics.PushNotificationCount))
}

func TestDryRunStillRefusesExpiredBeforeAnswer(t *testing.T) {
	// Server checks still run in dry run: an expired target is refused and no provider is called.
	set := NewDryRunSet()
	set.Add(ProviderAPNs, failingSender{t: t}, 1)
	job := newTestJob(1, ProviderAPNs)
	job.Notification = &apiproto.PushNotification{ExpireAt: time.Now().Unix() - 1}

	result := set.Dispatch(context.Background(), time.Minute, job)

	require.Equal(t, StatusRejected, result.GetStatus())
	require.Equal(t, CodeExpired, result.GetCode())
}

func TestDispatchInflightGaugeReturnsToZero(t *testing.T) {
	initPushMetricsForTest(t)

	sender := &gaugeSender{provider: ProviderFCM}
	set := NewSet()
	set.Add(ProviderFCM, sender, 1)

	result := set.Dispatch(context.Background(), time.Minute, newTestJob(0, ProviderFCM))

	require.Equal(t, StatusSent, result.GetStatus())
	require.Equal(t, float64(1), sender.seen, "the slot is counted while the call runs")
	require.Zero(t, testutil.ToFloat64(metrics.PushDirectInflight.WithLabelValues(ProviderFCM)))
}

func TestDispatchRefusesExpiredBeforeCall(t *testing.T) {
	// The sender fails the test if it is called: an expired target is refused before any call.
	set := NewSet()
	set.Add(ProviderAPNs, failingSender{t: t}, 1)
	job := newTestJob(2, ProviderAPNs)
	job.Notification = &apiproto.PushNotification{ExpireAt: time.Now().Unix() - 1}

	result := set.Dispatch(context.Background(), time.Minute, job)

	require.Equal(t, StatusRejected, result.GetStatus())
	require.Equal(t, CodeExpired, result.GetCode())
	require.Zero(t, result.GetHttpStatus())
	require.Equal(t, uint32(2), result.GetIndex())
}
