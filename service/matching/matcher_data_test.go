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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*
match task + task fwdr

match task to poller over task fwdr

match task rate limited

don't match with fwdr if not allow forwarding

put in a bunch of tasks and reprocess them

*/

type MatcherDataSuite struct {
	suite.Suite
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
	s.md = newMatcherData(cfg, true, clock.NewEventTimeSource())
}

func (s *MatcherDataSuite) now() time.Time {
	return s.md.timeSource.Now()
}

func (s *MatcherDataSuite) newSyncTask(fwdInfo *taskqueuespb.TaskForwardInfo) *internalTask {
	t := &persistencespb.TaskInfo{
		CreateTime: timestamppb.New(s.now()),
	}
	return newInternalTaskForSyncMatch(t, fwdInfo)
}

func (s *MatcherDataSuite) newBacklogTask(id int64, age time.Duration, f func(*internalTask, taskResponse)) *internalTask {
	t := &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.New(s.now().Add(-age)),
		},
		TaskId: id,
	}
	return newInternalTaskFromBacklog(t, f)
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
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.pollers.Len() > 0
	}, time.Second, time.Millisecond)

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
