// Copyright (c) 2025 ADBC Drivers Contributors
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

// Handwritten. Not regenerated.

package main

// #cgo CFLAGS: -DADBC_EXPORTING
// #cgo CXXFLAGS: -std=c++17 -DADBC_EXPORTING
// #include "adbc.h"
import "C"

import "context"

type statementCanceller interface {
	Cancel(context.Context) error
}

func (st *cStmt) beginExecutionContext() context.Context {
	st.executionMu.Lock()
	defer st.executionMu.Unlock()
	if st.executionCancel != nil {
		st.executionCancel()
	}
	st.executionCtx, st.executionCancel = context.WithCancel(context.Background())
	return st.executionCtx
}

func (st *cStmt) finishExecutionContext(ctx context.Context) {
	st.executionMu.Lock()
	defer st.executionMu.Unlock()
	if st.executionCtx == ctx {
		st.executionCtx = nil
		st.executionCancel = nil
	}
}

func (st *cStmt) cancelExecutionContext() bool {
	st.executionMu.Lock()
	defer st.executionMu.Unlock()
	if st.executionCancel == nil {
		return false
	}
	st.executionCancel()
	return true
}

//export AdbcDriverBigQueryInit
func AdbcDriverBigQueryInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	// The driver manager expects the spelling Bigquery and not BigQuery
	// as it derives the init symbol name from the filename
	// (adbc_driver_bigquery -> AdbcDriverBigquery) but user code may
	// expect BigQuery
	return AdbcDriverBigqueryInit(version, rawDriver, err)
}
