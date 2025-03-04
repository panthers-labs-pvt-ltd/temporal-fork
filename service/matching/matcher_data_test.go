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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
