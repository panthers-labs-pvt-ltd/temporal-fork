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

package testenv

import (
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv/action"
	"go.temporal.io/server/tests/testcore"
)

var (
	temporalMdl = stamp.NewModelSet()
)

type suite struct {
	*stamp.Suite
	cluster         *Cluster
	grpcInterceptor *grpcInterceptor
	setup           sharedResource[*testcore.FunctionalTestBase]
}

func init() {
	stamp.RegisterModel[*model.Cluster](temporalMdl)
	stamp.RegisterModel[*model.Namespace](temporalMdl)
	stamp.RegisterModel[*model.TaskQueue](temporalMdl)
	stamp.RegisterModel[*model.WorkflowClient](temporalMdl)
	stamp.RegisterModel[*model.WorkflowWorker](temporalMdl)
	stamp.RegisterModel[*model.Workflow](temporalMdl)
	stamp.RegisterModel[*model.WorkflowExecution](temporalMdl)
	stamp.RegisterModel[*model.WorkflowExecutionHistory](temporalMdl)
	stamp.RegisterModel[*model.WorkflowTask](temporalMdl)
	stamp.RegisterModel[*model.WorkflowUpdate](temporalMdl)
}

func NewTestSuite(tb testing.TB) *suite {
	return &suite{Suite: stamp.NewSuite(tb, newLogger)}
}

func (ts *suite) NewCluster(s *stamp.Scenario) *Cluster {
	mdlEnv := stamp.NewModelEnv(s, *temporalMdl, &model.Router{})
	cluster := newCluster(s, mdlEnv)

	// only need one cluster per test environment
	cluster.tbase = ts.setup.Start(func() *testcore.FunctionalTestBase {
		// prevents a race condition when multiple test environments in parallel
		clusterStartLock.Lock()
		defer clusterStartLock.Unlock()

		ts.grpcInterceptor = newGrpcInterceptor(newLogger(s.T()))

		tbase := &testcore.FunctionalTestBase{}
		tbase.Logger = s.Logger()
		tbase.SetT(s.T()) // TODO: drop this; requires cluster tbase to be decoupled from testify
		tbase.SetupSuiteWithCluster(
			"../testdata/es_cluster.yaml",
			// TODO: need to make this the 1st interceptor so it can observe every request
			// TODO: or use proxy instead? can be re-used for distributed test and doesn't require interceptor reshuffeling
			testcore.WithAdditionalGrpcInterceptors(ts.grpcInterceptor.Interceptor()))
		// testcore.WithPersistenceInterceptor(cluster.dbInterceptor())

		// TODO: grpcInterceptor only
		return tbase
	})

	ts.grpcInterceptor.addCluster(cluster)

	//ts.TB.Cleanup(func() {
	//	ts.grpcInterceptor.removeCluster(cluster)
	//	ts.setup.Stop(func(tbase *testcore.FunctionalTestBase) {
	//		tbase.TearDownCluster()
	//	})
	//})

	return cluster
}

func (ts *suite) NewWorkflowClient(c *Cluster, tq *model.TaskQueue) *WorkflowClient {
	return newWorkflowClient(c, tq)
}

func (ts *suite) NewWorkflowWorker(c *Cluster, tq *model.TaskQueue) *WorkflowWorker {
	return newWorkflowWorker(c, tq)
}

func (ts *suite) NewNamespaceSetup(s *stamp.Scenario) (
	*Cluster,
	*model.Namespace,
	*model.TaskQueue,
	*WorkflowClient,
	*WorkflowWorker,
) {
	c := ts.NewCluster(s)
	ns := stamp.Act(c, action.CreateNamespace{})
	tq := stamp.Act(c, action.CreateTaskQueue{Namespace: ns})
	client := ts.NewWorkflowClient(c, tq)
	worker := ts.NewWorkflowWorker(c, tq)
	return c, ns, tq, client, worker
}

func newLogger(t testing.TB) log.Logger {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	testlogger.DontPanicOnError(tl)
	tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)

	// check for unexpected error logs after the test
	t.Cleanup(func() {
		if t.Failed() {
			// no need to check if the test failed
			return
		}
		if tl.ResetFailureStatus() {
			panic(`Failing test as unexpected error logs were found.
							Look for 'Unexpected Error log encountered'.`)
		}
	})

	return tl
}
