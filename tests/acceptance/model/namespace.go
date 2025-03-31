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
	"go.temporal.io/server/common/testing/stamp"
)

type (
	Namespace struct {
		stamp.Model[*Namespace]
		stamp.Scope[*Cluster]

		InternalID string
	}
	NamespaceCreated struct {
		Name string
	}
	NamespaceProvider interface {
		GetNamespace() *Namespace
	}
)

//func (n *Namespace) Identify(action *Action) stamp.ID {
//	switch t := action.Request.(type) {
//	case NewTaskQueue:
//		return t.NamespaceName
//
//	// GRPC
//	case proto.Message:
//		// First, try the namespace name.
//		if id := stamp.ID(findProtoValueByNameType[string](t, "namespace", protoreflect.StringKind)); id != "" {
//			return id
//		}
//
//		// Then, try to the namespace ID; either from the task token or from the request.
//		var namespaceID string
//		if reqInternalID := findProtoValueByNameType[string](t, "namespace_id", protoreflect.StringKind); reqInternalID != "" {
//			namespaceID = reqInternalID
//		} else if taskToken := findProtoValueByNameType[[]byte](t, "task_token", protoreflect.BytesKind); taskToken != nil {
//			namespaceID = mustDeserializeTaskToken(taskToken).NamespaceId
//		}
//		if namespaceID == n.InternalID {
//			return n.GetID()
//		}
//
//	// persistence
//	case *persistence.CreateNamespaceRequest:
//		return stamp.ID(t.Namespace.Info.Name)
//	case *persistence.InternalCreateNamespaceRequest:
//		// InternalCreateNamespaceRequest is a special case for the test setup
//		// which creates the namespace directly; instead of through the API.
//		return stamp.ID(t.Name)
//	case *persistence.InternalUpdateWorkflowExecutionRequest:
//		if mdlInternalID := n.InternalID; t.UpdateWorkflowMutation.ExecutionInfo.NamespaceId == mdlInternalID {
//			return n.GetID()
//		}
//	}
//
//	return ""
//}
