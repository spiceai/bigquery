// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bigquery

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	err        error

	cancelFn context.CancelFunc
}

func checkContext(ctx context.Context, maybeErr error) error {
	if maybeErr != nil {
		return maybeErr
	} else if errors.Is(ctx.Err(), context.Canceled) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

// jobCancelTimeout bounds the jobs.cancel request itself. The context that
// would normally bound it is the one that has just been cancelled.
const jobCancelTimeout = 30 * time.Second

// jobCanceller asks BigQuery to stop a job. Narrower than *bigquery.Job so the
// watcher below can be tested without one.
type jobCanceller interface {
	ID() string
	Cancel(ctx context.Context) error
}

// jobCancelWatch stops a BigQuery job once nobody is waiting for it any more.
type jobCancelWatch struct {
	cancelOnce sync.Once
	requested  chan struct{}
	finished   chan struct{}
}

// stop releases the watch without waiting for a cancel already in flight.
//
// It must not block: on the cancellation path the caller is unwinding precisely
// because nobody is waiting any more, and making it wait for jobs.cancel would
// put that request's timeout back on the latency this change exists to remove.
func (w *jobCancelWatch) stop() { w.cancelOnce.Do(func() { close(w.requested) }) }

// cancelNow asks for the job to be stopped even though the context is still
// live, for a caller that is abandoning the job for its own reasons.
func (w *jobCancelWatch) cancelNow() { w.stop() }

// wait blocks until the watch goroutine has finished. For tests.
func (w *jobCancelWatch) wait() { <-w.finished }

// watchJobForCancellation stops the job as soon as nobody is waiting for it.
//
// Cancelling ctx only stops this client waiting. A BigQuery job outlives the
// client that submitted it: it keeps consuming slots, and billing, until
// something calls jobs.cancel. Nothing else in this driver does, so a caller
// that goes away — a cancelled statement, a closed connection, an abandoned
// query — otherwise leaves the query running to completion.
//
// `abandoned` reports whether the job should be stopped when the watch is
// released rather than because the context is done: `runQuery` can return with
// a live context and a job still running, when polling its status fails.
//
// The cancel request cannot use ctx, which is already done in the common case,
// so it gets a fresh context with its own timeout.
func watchJobForCancellation(
	ctx context.Context,
	logger *slog.Logger,
	job jobCanceller,
	abandoned func() bool,
) *jobCancelWatch {
	watch := &jobCancelWatch{
		requested: make(chan struct{}),
		finished:  make(chan struct{}),
	}
	go func() {
		defer close(watch.finished)
		select {
		case <-watch.requested:
			// Both channels can be ready at once — the query context is
			// cancelled, runQuery returns, and its deferred stop fires before
			// this goroutine is first scheduled — and select then picks between
			// them at random. Decide on the state, not on the coin toss.
			if ctx.Err() == nil && !abandoned() {
				return
			}
		case <-ctx.Done():
		}
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobCancelTimeout)
		defer cancel()
		if err := job.Cancel(cancelCtx); err != nil {
			logger.WarnContext(cancelCtx, "failed to cancel job, so it may keep running and billing until it finishes", "id", job.ID(), "error", err)
			return
		}
		logger.DebugContext(cancelCtx, "cancelled job", "id", job.ID())
	}()
	return watch
}

func runQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, executeUpdate bool) (bigquery.ArrowIterator, int64, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "run query")
	}

	// The job is running from here. It stays that way unless this function
	// reaches a terminal status for it, so anything else that ends this call
	// leaves work behind that has to be cancelled.
	reachedTerminalStatus := false
	watch := watchJobForCancellation(ctx, logger, job, func() bool { return !reachedTerminalStatus })
	defer watch.stop()

	// XXX: Google SDK badness.  We can't use Wait here because queries that
	// *fail* with a rateLimitExceeded (e.g. too many metadata operations)
	// will get the *polling* retried infinitely in Google's SDK (I believe
	// the SDK wants to retry "polling for job status" rate limit exceeded but
	// doesn't differentiate between them because googleapi.CheckResponse
	// appears to put the API error from the response object as an error of
	// the API call, from digging around using a debugger.  In other words, it
	// seems to be confusing "I got an error that my API request was rate
	// limited" and "I got an error that my job was rate limited" because
	// their internal APIs mix both errors into a single error path.)
	js, err := safeWaitForJob(ctx, logger, job)
	if err != nil {
		// Polling failed rather than the job finishing, so the job is still
		// running and the deferred stop must cancel it.
		return nil, -1, err
	}
	reachedTerminalStatus = true

	if err := js.Err(); err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "complete job")
	} else if !js.Done() {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] Query job did not complete",
		}
	}

	if executeUpdate {
		stats, ok := js.Statistics.Details.(*bigquery.QueryStatistics)
		if ok {
			return nil, stats.NumDMLAffectedRows, nil
		}
		return nil, -1, nil
	}

	// XXX: the Google SDK badness also applies here; it makes a similar
	// mistake with the retry, so we wait for the job above.
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "read query results")
	}

	var arrowIterator bigquery.ArrowIterator
	// If there is no schema in the row iterator, then arrow
	// iterator should be empty (#2173)
	if iter.TotalRows > 0 {
		// !IsAccelerated() -> failed to get Arrow stream -> we are
		// probably lacking permissions.  readSessionUser may sound
		// unrelated but creating a "read session" is the first step
		// of using the Storage API.  Note that Google swallows the
		// real error, so this is the best we can do.
		// https://cloud.google.com/bigquery/docs/reference/storage#create_a_session
		if !iter.IsAccelerated() {
			return nil, -1, adbc.Error{
				Code: adbc.StatusUnauthorized,
				Msg:  "[bq] Arrow reader requires roles/bigquery.readSessionUser, see https://github.com/apache/arrow-adbc/issues/3282",
			}
		}
		if arrowIterator, err = iter.ArrowIterator(); err != nil {
			return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "read Arrow query results")
		}
	} else {
		arrowIterator = emptyArrowIterator{iter.Schema}
	}
	totalRows := int64(iter.TotalRows)
	return arrowIterator, totalRows, nil
}

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, alloc memory.Allocator) (*ipc.Reader, *arrow.Schema, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))

	fields := make([]arrow.Field, len(arrowIterator.Schema()))
	for i, field := range arrowIterator.Schema() {
		fields[i], err = buildField(field, 0)
		if err != nil {
			return nil, nil, errToAdbcErr(adbc.StatusInternal, err, "build schema")
		}
	}

	if err != nil {
		return nil, nil, err
	}
	return rdr, arrow.NewSchema(fields, nil), nil
}

func getQueryParameter(values arrow.RecordBatch, row int, parameterMode string) ([]bigquery.QueryParameter, error) {
	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := parameterMode == OptionValueQueryParameterModeNamed
	schema := values.Schema()
	for i, v := range values.Columns() {
		pi, err := arrowValueToQueryParameterValue(schema.Field(i), v, row)
		if err != nil {
			return nil, err
		}
		parameters[i] = pi
		if includeName {
			parameters[i].Name = values.ColumnName(i)
		}
	}
	return parameters, nil
}

func runPlainQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int) (bigqueryRdr *reader, totalRows int64, err error) {
	arrowIterator, totalRows, err := runQuery(ctx, logger, query, false)
	if err != nil {
		return nil, -1, err
	}
	rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
	if err != nil {
		return nil, -1, err
	}

	chs := make([]chan arrow.RecordBatch, 1)
	ctx, cancelFn := context.WithCancel(ctx)
	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount:   1,
		chs:        chs,
		curChIndex: 0,
		err:        nil,
		cancelFn:   cancelFn,
		schema:     schema,
	}

	go func() {
		defer rdr.Release()
		for rdr.Next() && ctx.Err() == nil {
			rec := rdr.RecordBatch()
			rec.Retain()
			ch <- rec
		}

		// TODO(lidavidm): doesn't this discard the error?
		err = checkContext(ctx, rdr.Err())
		defer close(ch)
	}()
	return bigqueryRdr, totalRows, nil
}

func queryRecordWithSchemaCallback(ctx context.Context, logger *slog.Logger, group *errgroup.Group, query *bigquery.Query, rec arrow.RecordBatch, ch chan arrow.RecordBatch, parameterMode string, alloc memory.Allocator, rdrSchema func(schema *arrow.Schema)) (int64, error) {
	totalRows := int64(-1)
	for i := range int(rec.NumRows()) {
		parameters, err := getQueryParameter(rec, i, parameterMode)
		if err != nil {
			return -1, err
		}
		if parameters != nil {
			query.Parameters = parameters
		}

		arrowIterator, rows, err := runQuery(ctx, logger, query, false)
		if err != nil {
			return -1, err
		}
		totalRows = rows
		rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
		if err != nil {
			return -1, err
		}
		rdrSchema(schema)
		group.Go(func() error {
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				rec.Retain()
				ch <- rec
			}
			return checkContext(ctx, rdr.Err())
		})
	}
	return totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, logger *slog.Logger, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (bigqueryRdr *reader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, logger, query, alloc, resultRecordBufferSize)
	}
	defer boundParameters.Release()

	totalRows = 0
	// BigQuery can expose result sets as multiple streams when using certain APIs
	// for now lets keep this and set the number of channels to 1
	// when we need to adapt to multiple streams we can change the value here
	chs := make([]chan arrow.RecordBatch, 1)

	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefetchConcurrency)
	ctx, cancelFn := context.WithCancel(ctx)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	for boundParameters.Next() {
		rec := boundParameters.RecordBatch()
		// Each call to Record() on the record reader is allowed to release the previous record
		// and since we're doing this sequentially
		// we don't need to call rec.Retain() here and call call rec.Release() in queryRecordWithSchemaCallback
		batchRows, err := queryRecordWithSchemaCallback(ctx, logger, group, query, rec, ch, parameterMode, alloc, func(schema *arrow.Schema) {
			bigqueryRdr.schema = schema
		})
		if err != nil {
			return nil, -1, err
		}
		totalRows += batchRows
	}
	bigqueryRdr.err = group.Wait()
	defer close(ch)
	return bigqueryRdr, totalRows, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}
	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

type emptyArrowIterator struct {
	schema bigquery.Schema
}

func (e emptyArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	return nil, errors.New("Next should never be invoked on an empty iterator")
}

func (e emptyArrowIterator) Schema() bigquery.Schema {
	return e.schema
}

func (e emptyArrowIterator) SerializedArrowSchema() []byte {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(emptySchema))

	err := writer.Close()
	if err != nil {
		log.Fatalf("Error serializing an empty schema: %v", err)
	}

	return buf.Bytes()
}

var _ array.RecordReader = (*reader)(nil)
