// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build driverlib

package main

import (
	"sync"
	"testing"
)

func TestCancellableContextIsConcurrentSafe(t *testing.T) {
	var contexts cancellableContext
	var group sync.WaitGroup
	for range 100 {
		group.Add(2)
		go func() {
			defer group.Done()
			_ = contexts.newContext()
		}()
		go func() {
			defer group.Done()
			contexts.cancelContext()
		}()
	}
	group.Wait()
	contexts.cancelContext()
}

func TestStatementExecutionContextIsConcurrentSafe(t *testing.T) {
	var statement cStmt
	var group sync.WaitGroup
	for range 100 {
		group.Add(2)
		go func() {
			defer group.Done()
			ctx := statement.beginExecutionContext()
			statement.finishExecutionContext(ctx)
		}()
		go func() {
			defer group.Done()
			statement.cancelExecutionContext()
		}()
	}
	group.Wait()
}
