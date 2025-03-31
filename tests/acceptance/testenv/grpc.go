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
	"cmp"
	"context"
	"sync"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type grpcInterceptor struct {
	mu       sync.RWMutex
	clusters map[*Cluster]struct{}
}

func newGrpcInterceptor(logger log.Logger) *grpcInterceptor {
	return &grpcInterceptor{
		clusters: make(map[*Cluster]struct{}),
	}
}

func (i *grpcInterceptor) addCluster(cluster *Cluster) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.clusters[cluster] = struct{}{}
}

func (i *grpcInterceptor) removeCluster(cluster *Cluster) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.clusters, cluster)
}

func (i *grpcInterceptor) Interceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// extract action ID (if any)
		var actID stamp.ActID
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if _, ok := md[actionIdKey]; ok {
				actID = stamp.ActID(md.Get(actionIdKey)[0])
			}
		}
		actID = cmp.Or(actID, stamp.ActID(uuid.New()))

		// get a snapshot of the current clusters to avoid holding the lock during processing
		i.mu.RLock()
		clusters := make([]*Cluster, 0, len(i.clusters))
		for c := range i.clusters {
			clusters = append(clusters, c)
		}
		i.mu.RUnlock()

		// route request
		var onRespFuncs []stamp.OnComplete
		for _, c := range clusters {
			incAction := model.IncomingAction[proto.Message]{
				TriggerID:      actID,
				Cluster:        model.ClusterName(c.GetID()),
				RequestID:      uuid.New(),
				RequestHeaders: md,
				Method:         info.FullMethod,
				Request:        common.CloneProto(req.(proto.Message)),
			}

			// handle request in model
			onResp := c.mdlEnv.Route(incAction)
			if onResp != nil {
				onRespFuncs = append(onRespFuncs, onResp)
			}
		}

		// process the request
		resp, err := handler(ctx, req)

		// route response
		for _, onResp := range onRespFuncs {
			outAction := model.OutgoingAction[proto.Message]{
				ActID: actID,
			}
			if err == nil {
				outAction.Response = common.CloneProto(resp.(proto.Message))
			} else {
				outAction.ResponseErr = err
			}
			onResp(outAction)
		}

		return resp, err
	}
}
