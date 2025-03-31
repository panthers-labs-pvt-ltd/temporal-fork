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

package stamp

import (
	"testing"

	"go.temporal.io/server/common/log"
)

type (
	Suite struct {
		TB            testing.TB
		newLoggerFunc func(testing.TB) log.Logger
	}
)

func NewSuite(
	tb testing.TB,
	newLoggerFn func(testing.TB) log.Logger,
) *Suite {
	if t, ok := tb.(*testing.T); ok {
		t.Parallel() // enforce parallel execution
	}
	return &Suite{
		TB:            tb,
		newLoggerFunc: newLoggerFn,
	}
}

func (s *Suite) NewScenario(
	t *testing.T,
	opts ...ScenarioOption,
) *Scenario {
	t.Parallel() // enforce parallel execution
	if _, ok := s.TB.(*testing.F); ok {
		// allow randomness in fuzz tests
		opts = append(opts, allowRandomOption{})
	}
	return newScenario(t, s.newLoggerFunc(t), opts...)
}

func (s *Suite) RunScenarioMacro(
	t *testing.T,
	fn func(scenario *MacroScenario),
	opts ...ScenarioOption,
) {
	t.Parallel() // enforce parallel execution
	newScenarioMacro(t, s.newLoggerFunc, fn, append(opts)...).Run()
}
