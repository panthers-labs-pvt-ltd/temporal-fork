package history

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	namespacespb "go.temporal.io/server/api/namespace/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type (

	// Handler - gRPC handler interface for historyservice
	Handler struct {
		historyservice.UnimplementedHistoryServiceServer

		status int32

		tokenSerializer              *tasktoken.Serializer
		startWG                      sync.WaitGroup
		config                       *configs.Config
		eventNotifier                events.Notifier
		logger                       log.Logger
		throttledLogger              log.Logger
		persistenceExecutionManager  persistence.ExecutionManager
		persistenceShardManager      persistence.ShardManager
		persistenceVisibilityManager manager.VisibilityManager
		persistenceHealthSignal      persistence.HealthSignalAggregator
		healthServer                 *health.Server
		historyServiceResolver       membership.ServiceResolver
		metricsHandler               metrics.Handler
		payloadSerializer            serialization.Serializer
		timeSource                   clock.TimeSource
		namespaceRegistry            namespace.Registry
		saProvider                   searchattribute.Provider
		clusterMetadata              cluster.Metadata
		archivalMetadata             archiver.ArchivalMetadata
		hostInfoProvider             membership.HostInfoProvider
		controller                   shard.Controller
		tracer                       trace.Tracer
		taskQueueManager             persistence.HistoryTaskQueueManager
		taskCategoryRegistry         tasks.TaskCategoryRegistry
		dlqMetricsEmitter            *persistence.DLQMetricsEmitter
		healthSignalAggregator       HealthSignalAggregator

		replicationTaskFetcherFactory    replication.TaskFetcherFactory
		replicationTaskConverterProvider replication.SourceTaskConverterProvider
		streamReceiverMonitor            replication.StreamReceiverMonitor
	}

	NewHandlerArgs struct {
		fx.In

		Config                       *configs.Config
		Logger                       log.SnTaggedLogger
		ThrottledLogger              log.ThrottledLogger
		PersistenceExecutionManager  persistence.ExecutionManager
		PersistenceShardManager      persistence.ShardManager
		PersistenceHealthSignal      persistence.HealthSignalAggregator
		HealthServer                 *health.Server
		PersistenceVisibilityManager manager.VisibilityManager
		HistoryServiceResolver       membership.ServiceResolver
		MetricsHandler               metrics.Handler
		PayloadSerializer            serialization.Serializer
		TimeSource                   clock.TimeSource
		NamespaceRegistry            namespace.Registry
		SaProvider                   searchattribute.Provider
		ClusterMetadata              cluster.Metadata
		ArchivalMetadata             archiver.ArchivalMetadata
		HostInfoProvider             membership.HostInfoProvider
		ShardController              shard.Controller
		EventNotifier                events.Notifier
		TracerProvider               trace.TracerProvider
		TaskQueueManager             persistence.HistoryTaskQueueManager
		TaskCategoryRegistry         tasks.TaskCategoryRegistry
		DLQMetricsEmitter            *persistence.DLQMetricsEmitter
		HealthSignalAggregator       HealthSignalAggregator

		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskConverterFactory replication.SourceTaskConverterProvider
		StreamReceiverMonitor           replication.StreamReceiverMonitor
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.HistoryService"
)

var (
	_ historyservice.HistoryServiceServer = (*Handler)(nil)

	errNamespaceNotSet         = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errWorkflowExecutionNotSet = serviceerror.NewInvalidArgument("WorkflowExecution not set on request.")
	errTaskQueueNotSet         = serviceerror.NewInvalidArgument("Task queue not set.")
	errWorkflowIDNotSet        = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errRunIDNotValid           = serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	errSourceClusterNotSet     = serviceerror.NewInvalidArgument("Source Cluster not set on request.")
	errShardIDNotSet           = serviceerror.NewInvalidArgument("ShardId not set on request.")
	errTimestampNotSet         = serviceerror.NewInvalidArgument("Timestamp not set on request.")

	errShuttingDown = serviceerror.NewUnavailable("Shutting down")
)

// Start starts the handler
func (h *Handler) Start() {
	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	h.replicationTaskFetcherFactory.Start()
	h.streamReceiverMonitor.Start()
	// events notifier must starts before controller
	h.eventNotifier.Start()
	h.controller.Start()
	h.dlqMetricsEmitter.Start()
	h.healthSignalAggregator.Start()

	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	h.streamReceiverMonitor.Stop()
	h.replicationTaskFetcherFactory.Stop()
	h.controller.Stop()
	h.eventNotifier.Stop()
	h.dlqMetricsEmitter.Stop()
	h.healthSignalAggregator.Stop()
}

func (h *Handler) isStopped() bool {
	return atomic.LoadInt32(&h.status) == common.DaemonStatusStopped
}

func (h *Handler) DeepHealthCheck(
	ctx context.Context,
	_ *historyservice.DeepHealthCheckRequest,
) (_ *historyservice.DeepHealthCheckResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	// Ensure that the hosts are marked internally as healthy.
	status, err := h.healthServer.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
	if err != nil {
		return nil, err
	}
	if status.Status != healthpb.HealthCheckResponse_SERVING {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_DECLINED_SERVING))
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_DECLINED_SERVING}, nil
	}
	// Check that the RPC latency doesn't exceed the threshold.
	rpcLatency := h.healthSignalAggregator.AverageLatency()
	if rpcLatency > h.config.HealthRPCLatencyFailure() {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_NOT_SERVING))
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_NOT_SERVING}, nil
	}

	// Check if the persistence layer is healthy.
	latency := h.persistenceHealthSignal.AverageLatency()
	errRatio := h.persistenceHealthSignal.ErrorRatio()

	if latency > h.config.HealthPersistenceLatencyFailure() || errRatio > h.config.HealthPersistenceErrorRatio() {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_DECLINED_SERVING))
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_NOT_SERVING}, nil
	}
	metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_SERVING))
	return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_SERVING}, nil
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (h *Handler) IsWorkflowTaskValid(ctx context.Context, request *historyservice.IsWorkflowTaskValidRequest) (_ *historyservice.IsWorkflowTaskValidResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}
	workflowID := request.Execution.WorkflowId

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("IsWorkflowTaskValid", time.Since(startTime), retError)
	}()

	response, err := engine.IsWorkflowTaskValid(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// IsActivityTaskValid - whether activity task is still valid
func (h *Handler) IsActivityTaskValid(ctx context.Context, request *historyservice.IsActivityTaskValidRequest) (_ *historyservice.IsActivityTaskValidResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}
	workflowID := request.Execution.WorkflowId

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("IsActivityTaskValid", time.Since(startTime), retError)
	}()

	response, err := engine.IsActivityTaskValid(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	heartbeatRequest := request.HeartbeatRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordActivityTaskHeartbeat", time.Since(startTime), retError)
	}()

	response, err2 := engine.RecordActivityTaskHeartbeat(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if request.GetNamespaceId() == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordActivityTaskStarted", time.Since(startTime), retError)
	}()

	response, err := engine.RecordActivityTaskStarted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	response.Clock, err = shardContext.NewVectorClock()
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// RecordWorkflowTaskStarted - Record Workflow Task started.
func (h *Handler) RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (_ *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.PollRequest == nil || request.PollRequest.TaskQueue.GetName() == "" {
		return nil, h.convertError(errTaskQueueNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		h.logger.Error("RecordWorkflowTaskStarted failed.",
			tag.Error(err),
			tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
			tag.WorkflowScheduledEventID(request.GetScheduledEventId()),
		)
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordWorkflowTaskStarted", time.Since(startTime), retError)
	}()

	response, err := engine.RecordWorkflowTaskStarted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	response.Clock, err = shardContext.NewVectorClock()
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskCompleted", time.Since(startTime), retError)
	}()

	resp, err2 := engine.RespondActivityTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failRequest := request.FailedRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskFailed", time.Since(startTime), retError)
	}()

	resp, err2 := engine.RespondActivityTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	cancelRequest := request.CancelRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskCanceled", time.Since(startTime), retError)
	}()

	resp, err2 := engine.RespondActivityTaskCanceled(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// RespondWorkflowTaskCompleted - records completion of a workflow task
func (h *Handler) RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	h.logger.Debug("RespondWorkflowTaskCompleted",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := token.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondWorkflowTaskCompleted", time.Since(startTime), retError)
	}()

	response, err2 := engine.RespondWorkflowTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RespondWorkflowTaskFailed - failed response to workflow task
func (h *Handler) RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) (_ *historyservice.RespondWorkflowTaskFailedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failedRequest := request.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	h.logger.Debug("RespondWorkflowTaskFailed",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := token.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondWorkflowTaskFailed", time.Since(startTime), retError)
	}()

	err2 := engine.RespondWorkflowTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return &historyservice.RespondWorkflowTaskFailedResponse{}, nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	startRequest := request.StartRequest
	workflowID := startRequest.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("StartWorkflowExecution", time.Since(startTime), retError)
	}()

	response, err := engine.StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	if response.Clock == nil {
		response.Clock, err = shardContext.NewVectorClock()
		if err != nil {
			return nil, h.convertError(err)
		}
	}
	return response, nil
}

func (h *Handler) ExecuteMultiOperation(ctx context.Context, request *historyservice.ExecuteMultiOperationRequest) (_ *historyservice.ExecuteMultiOperationResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, request.WorkflowId)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.ExecuteMultiOperation(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	for _, opResp := range response.Responses {
		if startResp := opResp.GetStartWorkflow(); startResp != nil {
			if startResp.Clock == nil {
				startResp.Clock, err = shardContext.NewVectorClock()
				if err != nil {
					return nil, h.convertError(err)
				}
			}
		}
	}

	return response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (h *Handler) DescribeHistoryHost(_ context.Context, req *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	// This API supports describe history host by 1. address 2. shard id 3. namespace id + workflow id
	// if option 2/3 is provided, we want to check on the shard ownership to return the correct host address.
	shardID := req.GetShardId()
	if len(req.GetNamespaceId()) != 0 && req.GetWorkflowExecution() != nil {
		shardID = common.WorkflowIDToHistoryShard(req.GetNamespaceId(), req.GetWorkflowExecution().GetWorkflowId(), h.config.NumberOfShards)
	}
	if shardID > 0 {
		_, err := h.controller.GetShardByID(shardID)
		if err != nil {
			return nil, err
		}
	}

	itemsInRegistryByIDCount, itemsInRegistryByNameCount := h.namespaceRegistry.GetRegistrySize()
	ownedShardIDs := h.controller.ShardIDs()
	resp := &historyservice.DescribeHistoryHostResponse{
		ShardsNumber: int32(len(ownedShardIDs)),
		ShardIds:     ownedShardIDs,
		NamespaceCache: &namespacespb.NamespaceCacheInfo{
			ItemsInCacheByIdCount:   itemsInRegistryByIDCount,
			ItemsInCacheByNameCount: itemsInRegistryByNameCount,
		},
		Address: h.hostInfoProvider.HostInfo().GetAddress(),
	}
	return resp, nil
}

// RemoveTask returns information about the internal states of a history host
func (h *Handler) RemoveTask(ctx context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	var err error
	category, ok := h.taskCategoryRegistry.GetCategoryByID(int(request.Category))
	if !ok {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid task category ID: %v", request.Category))
	}

	key := tasks.NewKey(
		timestamp.TimeValue(request.GetVisibilityTime()),
		request.GetTaskId(),
	)
	if err := tasks.ValidateKey(key); err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid task key: %v", err.Error()))
	}

	err = h.persistenceExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      request.GetShardId(),
		TaskCategory: category,
		TaskKey:      key,
	})

	return &historyservice.RemoveTaskResponse{}, err
}

// CloseShard closes a shard hosted by this instance
func (h *Handler) CloseShard(_ context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.controller.CloseShardByID(request.GetShardId())
	return &historyservice.CloseShardResponse{}, nil
}

// GetShard gets a shard hosted by this instance
func (h *Handler) GetShard(ctx context.Context, request *historyservice.GetShardRequest) (_ *historyservice.GetShardResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	resp, err := h.persistenceShardManager.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID: request.ShardId,
	})
	if err != nil {
		return nil, err
	}
	return &historyservice.GetShardResponse{ShardInfo: resp.ShardInfo}, nil
}

// RebuildMutableState attempts to rebuild mutable state according to persisted history events
func (h *Handler) RebuildMutableState(ctx context.Context, request *historyservice.RebuildMutableStateRequest) (_ *historyservice.RebuildMutableStateResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RebuildMutableState", time.Since(startTime), retError)
	}()

	if err := engine.RebuildMutableState(ctx, namespaceID, &commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      workflowExecution.RunId,
	}); err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.RebuildMutableStateResponse{}, nil
}

// ImportWorkflowExecution attempts to workflow execution according to persisted history events
func (h *Handler) ImportWorkflowExecution(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest) (_ *historyservice.ImportWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	if workflowID == "" {
		return nil, h.convertError(errWorkflowIDNotSet)
	}
	runID := workflowExecution.GetRunId()
	if runID == "" {
		return nil, h.convertError(errRunIDNotValid)
	}
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	if !shardContext.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return nil, serviceerror.NewUnimplemented("ImportWorkflowExecution must be used in global namespace mode")
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("ImportWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err := engine.ImportWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// DescribeMutableState - returns the internal analysis of workflow execution state
func (h *Handler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("DescribeMutableState", time.Since(startTime), retError)
	}()

	resp, err2 := engine.DescribeMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("GetMutableState", time.Since(startTime), retError)
	}()

	resp, err2 := engine.GetMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("PollMutableState", time.Since(startTime), retError)
	}()

	resp, err2 := engine.PollMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("DescribeWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err2 := engine.DescribeWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" || request.CancelRequest.GetNamespace() == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	cancelRequest := request.CancelRequest
	h.logger.Debug("RequestCancelWorkflowExecution",
		tag.WorkflowNamespace(cancelRequest.GetNamespace()),
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(cancelRequest.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(cancelRequest.WorkflowExecution.GetRunId()))

	workflowID := cancelRequest.WorkflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RequestCancelWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err2 := engine.RequestCancelWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.SignalRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("SignalWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err2 := engine.SignalWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a workflow task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a workflow task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	signalWithStartRequest := request.SignalWithStartRequest
	workflowID := signalWithStartRequest.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("SignalWithStartWorkflowExecution", time.Since(startTime), retError)
	}()

	for {
		resp, err2 := engine.SignalWithStartWorkflowExecution(ctx, request)
		if err2 == nil {
			return resp, nil
		}

		// Two simultaneous SignalWithStart requests might try to start a workflow at the same time.
		// This can result in one of the requests failing with one of two possible errors:
		//    CurrentWorkflowConditionFailedError || WorkflowConditionFailedError
		// If either error occurs, just go ahead and retry. It should succeed on the subsequent attempt.
		// For simplicity, we keep trying unless the context finishes or we get an error that is not one of the
		// two mentioned above.
		_, isCurrentWorkflowConditionFailedErr := err2.(*persistence.CurrentWorkflowConditionFailedError)
		_, isWorkflowConditionFailedErr := err2.(*persistence.WorkflowConditionFailedError)

		isContextDone := false
		select {
		case <-ctx.Done():
			isContextDone = true
			if ctxErr := ctx.Err(); ctxErr != nil {
				err2 = ctxErr
			}
		default:
		}

		if (!isCurrentWorkflowConditionFailedErr && !isWorkflowConditionFailedErr) || isContextDone {
			return nil, h.convertError(err2)
		}
	}
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal workflow task finished.
func (h *Handler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RemoveSignalMutableState", time.Since(startTime), retError)
	}()

	resp, err2 := engine.RemoveSignalMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.TerminateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("TerminateWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err2 := engine.TerminateWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

func (h *Handler) DeleteWorkflowExecution(ctx context.Context, request *historyservice.DeleteWorkflowExecutionRequest) (_ *historyservice.DeleteWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	h.logger.Info("DeleteWorkflowExecution requested",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(workflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(workflowExecution.GetRunId()))

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("DeleteWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err := engine.DeleteWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.ResetRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("ResetWorkflowExecution", time.Since(startTime), retError)
	}()

	resp, err2 := engine.ResetWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// UpdateWorkflowExecutionOptions updates the options of a workflow execution.
// Can be used to set and unset versioning behavior override.
func (h *Handler) UpdateWorkflowExecutionOptions(ctx context.Context, request *historyservice.UpdateWorkflowExecutionOptionsRequest) (_ *historyservice.UpdateWorkflowExecutionOptionsResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.UpdateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("UpdateWorkflowExecutionOptions", time.Since(startTime), retError)
	}()

	resp, err2 := engine.UpdateWorkflowExecutionOptions(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowID := request.GetRequest().GetExecution().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("QueryWorkflow", time.Since(startTime), retError)
	}()

	resp, err2 := engine.QueryWorkflow(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// convertError is a placeholder for error conversion logic.
func (h *Handler) convertError(err error) error {
	return err
}

// validateTaskToken is a placeholder for task token validation logic.
func validateTaskToken(token interface{}) error {
	return nil
}
