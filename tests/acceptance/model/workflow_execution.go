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

package model

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowExecution struct {
		stamp.Model[*WorkflowExecution]
		stamp.Scope[*Workflow]

		RequestID string
		Status    enumspb.WorkflowExecutionStatus
	}
)

func (w *WorkflowExecution) GetNamespace() *Namespace {
	return w.GetScope().GetNamespace()
}

func (w *WorkflowExecution) OnWorkflowStart(
	in IncomingAction[*workflowservice.StartWorkflowExecutionRequest],
) func(OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
	var newStart bool
	if w.RequestID != in.Request.RequestId {
		w.RequestID = in.Request.RequestId
		w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		newStart = true
	}

	return func(out OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
		if resp := out.Response; resp != nil {
			if newStart {
				w.SetID(out.Response.RunId)
			}
		}
	}
}

func (w *WorkflowExecution) OnRespondWorkflowTaskCompleted(
	in IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) {
	for _, commands := range in.Request.Commands {
		switch commands.CommandType {
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		}
	}
}

func (w *WorkflowExecution) IsComplete() stamp.Prop[bool] {
	return stamp.NewProp(w, func(*stamp.PropContext) bool {
		return w.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})
}

func (w *WorkflowExecution) Next(ctx stamp.GenContext) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: string(w.GetScope().GetID()),
		RunId:      "", // TODO: use current ID
	}
}
