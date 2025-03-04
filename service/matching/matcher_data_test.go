// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MatcherDataSuite struct {
	suite.Suite
	ts *clock.EventTimeSource
	md matcherData
}

func TestMatcherDataSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(MatcherDataSuite))
}

func (s *MatcherDataSuite) SetupTest() {
	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"nsname",
	)
	s.ts = clock.NewEventTimeSource().Update(time.Now())
	s.ts.UseAsyncTimers(true)
	s.md = newMatcherData(cfg, true, s.ts)
}

func (s *MatcherDataSuite) now() time.Time {
	return s.ts.Now()
}

func (s *MatcherDataSuite) pollRealTime(timeout time.Duration) *matchResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.pollContext(ctx)
}

func (s *MatcherDataSuite) pollFakeTime(timeout time.Duration) *matchResult {
	ctx, cancel := clock.ContextWithTimeout(context.Background(), timeout, s.ts)
	defer cancel()
	return s.pollContext(ctx)
}

func (s *MatcherDataSuite) pollContext(ctx context.Context) *matchResult {
	return s.md.EnqueuePollerAndWait([]context.Context{ctx}, &waitingPoller{
		startTime:    s.now(),
		forwardCtx:   ctx,
		pollMetadata: &pollMetadata{},
	})
}

func (s *MatcherDataSuite) newSyncTask(fwdInfo *taskqueuespb.TaskForwardInfo) *internalTask {
	t := &persistencespb.TaskInfo{
		CreateTime: timestamppb.New(s.now()),
	}
	return newInternalTaskForSyncMatch(t, fwdInfo)
}

func (s *MatcherDataSuite) newBacklogTask(id int64, age time.Duration, f func(*internalTask, taskResponse)) *internalTask {
	return s.newBacklogTaskWithPriority(id, age, f, nil)
}

func (s *MatcherDataSuite) newBacklogTaskWithPriority(id int64, age time.Duration, f func(*internalTask, taskResponse), pri *commonpb.Priority) *internalTask {
	t := &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.New(s.now().Add(-age)),
			Priority:   pri,
		},
		TaskId: id,
	}
	return newInternalTaskFromBacklog(t, f)
}

func (s *MatcherDataSuite) waitForPollers(n int) {
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.pollers.Len() >= n
	}, time.Second, time.Millisecond)
}

func (s *MatcherDataSuite) waitForTasks(n int) {
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.tasks.Len() >= n
	}, time.Second, time.Millisecond)
}

func (s *MatcherDataSuite) TestMatchBacklogTask() {
	poller := &waitingPoller{startTime: s.now()}

	// no task yet, context should time out
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres := s.md.EnqueuePollerAndWait([]context.Context{ctx}, poller)
	s.Error(context.DeadlineExceeded, pres.ctxErr)
	s.Equal(0, pres.ctxErrIdx)

	// add a task
	gotResponse := false
	done := func(t *internalTask, tres taskResponse) {
		gotResponse = true
		s.NoError(tres.startErr)
	}
	t := s.newBacklogTask(123, 0, done)
	s.md.EnqueueTaskNoWait(t)

	// now should match with task
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres = s.md.EnqueuePollerAndWait([]context.Context{ctx}, poller)
	s.NoError(pres.ctxErr)
	s.Equal(t, pres.task)

	// finish task
	pres.task.finish(nil, true)
	s.True(gotResponse)

	// one more, context should time out again. note two contexts this time.
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres = s.md.EnqueuePollerAndWait([]context.Context{context.Background(), ctx}, poller)
	s.Error(context.DeadlineExceeded, pres.ctxErr)
	s.Equal(1, pres.ctxErrIdx, "deadline context was index 1")
}

func (s *MatcherDataSuite) TestMatchTaskImmediately() {
	t := s.newSyncTask(nil)

	// no match yet
	canSyncMatch, gotSyncMatch := s.md.MatchTaskImmediately(t)
	s.True(canSyncMatch)
	s.False(gotSyncMatch)

	// poll in a goroutine
	ch := make(chan *matchResult, 1)
	go func() {
		poller := &waitingPoller{startTime: s.now()}
		ch <- s.md.EnqueuePollerAndWait(nil, poller)
	}()

	// wait until poller queued
	s.waitForPollers(1)

	// should match this time
	canSyncMatch, gotSyncMatch = s.md.MatchTaskImmediately(t)
	s.True(canSyncMatch)
	s.True(gotSyncMatch)

	// check match
	pres := <-ch
	s.NoError(pres.ctxErr)
	s.Equal(t, pres.task)
}

func (s *MatcherDataSuite) TestMatchTaskImmediatelyDisabledBacklog() {
	// register some backlog with old tasks
	s.md.EnqueueTaskNoWait(s.newBacklogTask(123, 10*time.Minute, nil))

	t := s.newSyncTask(nil)
	canSyncMatch, gotSyncMatch := s.md.MatchTaskImmediately(t)
	s.False(canSyncMatch)
	s.False(gotSyncMatch)
}

func (s *MatcherDataSuite) TestRateLimitedBacklog() {
	// 10 tasks/sec with burst of 3
	s.md.UpdateRateLimit(10, 300*time.Millisecond)

	// register some backlog with old tasks
	for i := range 100 {
		s.md.EnqueueTaskNoWait(s.newBacklogTask(123+int64(i), 0, nil))
	}

	start := s.ts.Now()

	// start 10 poll loops to poll them
	var running atomic.Int64
	var lastTask atomic.Int64
	for range 10 {
		running.Add(1)
		go func() {
			defer running.Add(-1)
			for {
				if pres := s.pollFakeTime(time.Second); pres.ctxErr != nil {
					return
				}
				lastTask.Store(s.now().UnixNano())
			}
		}()
	}

	// advance fake time until done
	for running.Load() > 0 {
		s.ts.Advance(time.Duration(rand.Int63n(int64(10 * time.Millisecond))))
		runtime.Gosched()
		runtime.Gosched()
		runtime.Gosched()
	}

	elapsed := time.Unix(0, lastTask.Load()).Sub(start)
	s.Greater(elapsed, 9*time.Second)
	// with very unlucky scheduling, we might end up taking longer to poll the tasks
	s.Less(elapsed, 20*time.Second)
}

func (s *MatcherDataSuite) TestOrder() {
	t1 := s.newBacklogTaskWithPriority(1, 0, nil, &commonpb.Priority{PriorityKey: 1})
	t2 := s.newBacklogTaskWithPriority(2, 0, nil, &commonpb.Priority{PriorityKey: 2})
	t3 := s.newBacklogTaskWithPriority(3, 0, nil, &commonpb.Priority{PriorityKey: 3})
	tf := &internalTask{isPollForwarder: true}

	s.md.EnqueueTaskNoWait(t3)
	s.md.EnqueueTaskNoWait(tf)
	s.md.EnqueueTaskNoWait(t1)
	s.md.EnqueueTaskNoWait(t2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
	s.Equal(t3, s.pollFakeTime(time.Second).task)
	// poll forwarder is last to match, but it does a half-match so we won't see it here
}

func (s *MatcherDataSuite) TestPollForwardSuccess() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, &internalTask{isPollForwarder: true})
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// forward succeeded, pass back task
		s.md.FinishMatchAfterPollForward(tres.poller, t2)
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
}

func (s *MatcherDataSuite) TestPollForwardFailed() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, &internalTask{isPollForwarder: true})
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// there's a new task in the meantime
		s.md.EnqueueTaskNoWait(t2)
		// forward failed, re-enqueue poller so it can match again
		s.md.ReenqueuePollerIfNotMatched(tres.poller)
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
}

func (s *MatcherDataSuite) TestPollForwardFailedTimedOut() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, &internalTask{isPollForwarder: true})
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// there's a new task in the meantime
		s.md.EnqueueTaskNoWait(t2)
		time.Sleep(11 * time.Millisecond)
		// but we waited too long, poller timed out, so this does nothing (but doesn't crash or assert)
		s.md.ReenqueuePollerIfNotMatched(tres.poller)
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollRealTime(10*time.Millisecond).task)
	s.Error(s.pollRealTime(10 * time.Millisecond).ctxErr)
}

// simple limiter tests

func TestSimpleLimiter(t *testing.T) {
	var sl simpleLimiter
	sl.set(10, time.Second)

	base := time.Now().UnixNano()
	now := base

	// can consume 11 tokens immediately (1 since we're starting from 0 and 10 burst)
	for range 11 {
		require.GreaterOrEqual(t, now, sl.ready)
		sl.consume(now, 1)
	}
	// now not ready anymore
	require.Less(t, now, sl.ready)

	// after 100 ms, we can consume one more
	now += int64(99 * time.Millisecond)
	require.Less(t, now, sl.ready)
	now += int64(1 * time.Millisecond)
	require.GreaterOrEqual(t, now, sl.ready)
	sl.consume(now, 1)
}

func TestSimpleLimiterOverTime(t *testing.T) {
	var sl simpleLimiter
	sl.set(10, time.Second)

	base := time.Now().UnixNano()
	now := base

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, average < 100ms, so we are limited on average
		// but have some gaps too.
		now += (70 + rand.Int63n(50)) * int64(time.Millisecond)

		if now >= sl.ready {
			sl.consume(now, 1)
			consumed++
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}

func TestSimpleLimiterRecycle(t *testing.T) {
	var sl simpleLimiter
	sl.set(10, time.Second)

	base := time.Now().UnixNano()
	now := base

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, always < 100ms, so we are always limited
		now += (30 + rand.Int63n(30)) * int64(time.Millisecond)

		if now >= sl.ready {
			sl.consume(now, 1)
			consumed++

			// 20% of the time, recycle the token we took
			if rand.Intn(100) < 20 {
				now += int64(5 * time.Millisecond)
				sl.consume(now, -1)
				consumed--
			}
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}
