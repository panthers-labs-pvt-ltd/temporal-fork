package modifyworkflowproperties

import (
	"context"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	modRequest *historyservice.ModifyWorkflowExecutionPropertiesRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.ModifyWorkflowExecutionPropertiesResponse, retError error) {
	namespaceID := namespace.ID(modRequest.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	request := modRequest.ModifyRequest
	opts := request.Option
	workflowID := request.WorkflowExecution.GetWorkflowId()
	runID := request.WorkflowExecution.GetRunId()

	if !opts.Unsafe {
		return nil, serviceerror.NewUnimplemented("safe mode is not yet implemented; try again with unsafe=true")
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		locks.PriorityHigh, // todo carly: should this be high priority?
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	// todo carly: dedup by requestID
	// todo carly: decide where to use request.Reason
	//mutableState := workflowLease.GetMutableState()

	if opts.BuildId != nil {
		// todo carly: change build id in mutable state
		// todo carly: change build id in visibility
		// todo carly: move queued tasks to correct queue
	}

	if opts.VersioningBehavior != enums.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// todo carly: change change versioning behavior in mutable state
		// todo carly: change versioning behavior in visibility
		// todo carly: move queued tasks to correct queue
	}
	return nil, nil
}
