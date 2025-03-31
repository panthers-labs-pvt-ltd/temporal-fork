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
	"fmt"
	"reflect"
	"strings"

	"go.temporal.io/server/common/testing/stamp"
	"google.golang.org/grpc/metadata"
)

type IncomingAction[T any] struct {
	TriggerID      stamp.ActID `validate:"required"`
	Cluster        ClusterName `validate:"required"`
	RequestID      string
	RequestHeaders metadata.MD
	Method         string
	Request        T
}

func (a IncomingAction[T]) String() string {
	var sb strings.Builder
	sb.WriteString("IncomingAction[\n")
	sb.WriteString(fmt.Sprintf("  Cluster: %s\n", a.Cluster))
	sb.WriteString(fmt.Sprintf("  Request: %T\n", a.Request))
	sb.WriteString(fmt.Sprintf("  RequestID: %s\n", a.RequestID))
	sb.WriteString(fmt.Sprintf("  Method: %s\n", a.Method))
	sb.WriteString("]")
	return sb.String()
}

func (a IncomingAction[T]) ID() stamp.ActID {
	return a.TriggerID
}

func (a IncomingAction[T]) Route() string {
	var res string
	t := reflect.TypeOf(a.Request)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
		res = "*"
	}
	res += t.PkgPath() + "." + t.Name()
	return res
}

type OutgoingAction[T any] struct {
	ActID       stamp.ActID
	Response    T
	ResponseErr error
}

func (a OutgoingAction[T]) ID() stamp.ActID {
	return a.ActID
}

func (a OutgoingAction[T]) String() string {
	var sb strings.Builder
	sb.WriteString("OutgoingAction[\n")
	if a.ResponseErr != nil {
		sb.WriteString(fmt.Sprintf("  ResponseErr: %T\n", a.ResponseErr))
	} else {
		sb.WriteString(fmt.Sprintf("  Response: %T\n", a.Response))
	}
	sb.WriteString("]")
	return sb.String()
}
