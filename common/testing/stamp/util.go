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
	"fmt"
	"reflect"
	"regexp"

	"github.com/davecgh/go-spew/spew"
	"github.com/fatih/color"
	goValidator "github.com/go-playground/validator/v10"
)

var (
	boldStr          = color.New(color.Bold).SprintFunc()
	redStr           = color.New(color.FgRed).SprintFunc()
	underlineStr     = color.New(color.Underline).SprintFunc()
	simpleSpew       = spew.NewDefaultConfig()
	validator        = goValidator.New()
	genericTypeRegex = regexp.MustCompile(`^[^[]+\[([^\[\]]+)\]$`)
)

func init() {
	color.NoColor = false

	simpleSpew.DisablePointerAddresses = true
	simpleSpew.DisableCapacities = true
	simpleSpew.MaxDepth = 2
}

func qualifiedTypeName(t reflect.Type) string {
	return t.PkgPath() + "." + t.Name()
}

func mustGetTypeParam(t reflect.Type) string {
	typeStr := t.String()
	if matches := genericTypeRegex.FindStringSubmatch(typeStr); matches != nil {
		return matches[1]
	}
	panic("not a generic type: " + typeStr)
}

func mustCastVal(src reflect.Value, dstType reflect.Type) reflect.Value {
	dst := reflect.New(dstType)
	if dst.Kind() == reflect.Pointer {
		dst = dst.Elem()
	}
	if src.Kind() == reflect.Pointer {
		src = src.Elem()
	}
	for i := 0; i < src.NumField(); i++ {
		dstField := dst.Field(i)
		if !dstField.CanSet() {
			continue
		}
		srcField := src.Field(i)
		if srcField.Type() == dstField.Type() {
			dstField.Set(srcField)
		} else if srcField.Type().AssignableTo(dstField.Type()) {
			dstField.Set(srcField)
		} else if srcField.Type().ConvertibleTo(dstField.Type()) {
			dstField.Set(srcField.Convert(dstField.Type()))
		} else if srcField.Kind() == reflect.Interface {
			if !srcField.IsNil() {
				dstField.Set(srcField.Elem())
			}
		} else {
			panic(fmt.Sprintf("cannot copy field %q from %q to %q",
				dst.Type().Field(i).Name, srcField.Type(), dstField.Type()))
		}
	}
	return dst
}
