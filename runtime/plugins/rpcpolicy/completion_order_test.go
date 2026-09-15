package rpcpolicy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The work reads its context BEFORE it stamps its instant. A deadline that
// fires between the two reads therefore cannot be blamed for a completion that
// preceded it: the captured state is nil, the stamp is the completion, and the
// attempt is a completion -- msim's, at D_s - 1 ms, with the downstream's own
// code and a complete occupancy. Read the other way round, the same gap turned
// it into a cut at D_s.
func TestServeAnExpiryBetweenTheTwoReadsIsNotTheCauseOfTheCompletion(t *testing.T) {
	const doc = "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n"
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test", doc, nil, clock.Now().UnixMilli(), nil)

	deadline := clock.Now().Add(time.Hour)
	completedAt := deadline.Add(-time.Millisecond)
	ctx := newDescheduledCtx(deadline)

	read, proceed := make(chan struct{}), make(chan struct{})
	s.betweenCompletionReads = func() {
		close(read)
		<-proceed
	}

	served := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			clock.Advance(time.Hour - time.Millisecond)
			return nil, status.Error(codes.DeadlineExceeded, "the downstream's own deadline, which is nearer")
		})
		served <- err
	}()
	<-read
	// The parent's context ends in the gap between the two reads; the clock
	// does not move, so the stamp that follows is still D_s - 1 ms.
	ctx.expire(context.DeadlineExceeded)
	close(proceed)
	err := <-served

	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "the downstream's status, passed through")
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
	assert.Equal(t, false, recs[0]["occupancy_censored"])
	assert.Equal(t, epochOf(completedAt), recs[0]["end_epoch"])
	assert.Equal(t, epochOf(completedAt), recs[0]["permit_released_at"])
	assert.Equal(t, 0, s.registry.Load().Station().Busy())
}

// The belt to that brace: even if the captured state DID say DeadlineExceeded,
// a stamp strictly before the deadline is a completion -- the deadline cannot
// have stopped work that stopped before it. Only a fake clock can produce the
// pair; a real one reads the state first.
func TestServeACapturedExpiryAheadOfItsOwnDeadlineIsNotACause(t *testing.T) {
	const doc = "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n"
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test", doc, nil, clock.Now().UnixMilli(), nil)

	deadline := clock.Now().Add(time.Hour)
	completedAt := deadline.Add(-time.Millisecond)
	ctx := newDescheduledCtx(deadline)

	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		clock.Advance(time.Hour - time.Millisecond)
		// The context reports its deadline while the clock says it has not
		// arrived: the state the work captures is DeadlineExceeded, the stamp
		// is D_s - 1 ms.
		ctx.expire(context.DeadlineExceeded)
		return nil, status.Error(codes.DeadlineExceeded, "the downstream's own deadline")
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"],
		"a stop strictly before the deadline is a completion, whatever the context reports")
	assert.Equal(t, false, recs[0]["occupancy_censored"])
	assert.Equal(t, epochOf(completedAt), recs[0]["end_epoch"])
	assert.Equal(t, 0, s.registry.Load().Station().Busy())
}
