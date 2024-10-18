package modifyworkflowproperties

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	modifyRequest *historyservice.ModifyWorkflowExecutionPropertiesRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.ModifyWorkflowExecutionPropertiesResponse, retError error) {
	return nil, nil
}
