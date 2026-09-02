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

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
)

const jobCancelTimeout = 30 * time.Second

type jobCanceller interface {
	ID() string
	Cancel(context.Context) error
}

type jobResolver func(context.Context) (jobCanceller, error)

// cancellableJob identifies a job before jobs.insert starts, so cancellation
// can still find it if BigQuery accepts the insert but its response is lost.
type cancellableJob struct {
	id      string
	resolve jobResolver

	jobMu sync.RWMutex
	job   jobCanceller

	finished atomic.Bool
	once     sync.Once
	done     chan struct{}
	err      error
}

func newCancellableJob(id string, resolve jobResolver) *cancellableJob {
	return &cancellableJob{id: id, resolve: resolve, done: make(chan struct{})}
}

func (j *cancellableJob) ID() string { return j.id }

func (j *cancellableJob) setJob(job jobCanceller) {
	j.jobMu.Lock()
	j.job = job
	j.jobMu.Unlock()
}

func (j *cancellableJob) currentJob() jobCanceller {
	j.jobMu.RLock()
	defer j.jobMu.RUnlock()
	return j.job
}

func (j *cancellableJob) markFinished() { j.finished.Store(true) }

func (j *cancellableJob) cancel(ctx context.Context) error {
	if j.finished.Load() {
		return nil
	}

	j.once.Do(func() {
		go func() {
			defer close(j.done)
			j.err = j.cancelJob(ctx)
		}()
	})

	select {
	case <-j.done:
		return j.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *cancellableJob) cancelJob(ctx context.Context) error {
	if j.finished.Load() {
		return nil
	}

	job := j.currentJob()
	if job == nil {
		if j.resolve == nil {
			return errors.New("BigQuery job is not available for cancellation")
		}

		backoff := gax.Backoff{Initial: 25 * time.Millisecond, Multiplier: 1.5, Max: 250 * time.Millisecond}
		for job == nil {
			if j.finished.Load() {
				return nil
			}
			resolved, err := j.resolve(ctx)
			if err == nil {
				job = resolved
				j.setJob(resolved)
				break
			}
			if !isJobNotFound(err) {
				return err
			}
			if err := gax.Sleep(ctx, backoff.Pause()); err != nil {
				return err
			}
		}
	}

	if j.finished.Load() {
		return nil
	}
	return job.Cancel(ctx)
}

func isJobNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 404
}

type statementExecution struct {
	cancel context.CancelFunc
}

func (st *statement) beginExecution(ctx context.Context) (context.Context, *statementExecution) {
	ctx, cancel := context.WithCancel(ctx)
	op := &statementExecution{cancel: cancel}

	st.cancelMu.Lock()
	previous := st.execution
	st.execution = op
	st.cancelMu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	return ctx, op
}

func (st *statement) endExecution(op *statementExecution) {
	op.cancel()
	st.cancelMu.Lock()
	if st.execution == op {
		st.execution = nil
	}
	st.cancelMu.Unlock()
}

func (st *statement) beginJob(client *bigquery.Client, config *bigquery.JobIDConfig) *cancellableJob {
	if config.JobID == "" {
		config.JobID = uuid.NewString()
	} else if config.AddJobIDSuffix {
		config.JobID += "-" + uuid.NewString()
	}
	config.AddJobIDSuffix = false
	projectID := config.ProjectID
	if projectID == "" {
		projectID = client.Project()
	}
	location := config.Location
	if location == "" {
		location = client.Location
	}

	flight := newCancellableJob(config.JobID, func(ctx context.Context) (jobCanceller, error) {
		return client.JobFromProject(ctx, projectID, config.JobID, location)
	})
	st.cancelMu.Lock()
	if st.inFlight == nil {
		st.inFlight = make(map[*cancellableJob]struct{})
	}
	st.inFlight[flight] = struct{}{}
	st.cancelMu.Unlock()
	return flight
}

func (st *statement) endJob(flight *cancellableJob) {
	st.cancelMu.Lock()
	delete(st.inFlight, flight)
	if len(st.inFlight) == 0 {
		st.inFlight = nil
	}
	st.cancelMu.Unlock()
}

// Cancel stops the current statement execution and waits for BigQuery to
// accept the jobs.cancel request. It is safe to call concurrently with Execute.
func (st *statement) Cancel(ctx context.Context) error {
	st.cancelMu.Lock()
	op := st.execution
	flights := make([]*cancellableJob, 0, len(st.inFlight))
	for flight := range st.inFlight {
		flights = append(flights, flight)
	}
	st.cancelMu.Unlock()

	if op == nil && len(flights) == 0 {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "[bq] no active query to cancel"}
	}
	if op != nil {
		op.cancel()
	}
	if len(flights) == 0 {
		return nil
	}

	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobCancelTimeout)
	defer cancel()
	var group sync.WaitGroup
	errs := make(chan error, len(flights))
	for _, flight := range flights {
		group.Go(func() {
			if err := flight.cancel(cancelCtx); err != nil {
				errs <- fmt.Errorf("job %s: %w", flight.ID(), err)
			}
		})
	}
	group.Wait()
	close(errs)
	var cancelErrors []error
	for err := range errs {
		cancelErrors = append(cancelErrors, err)
	}
	if err := errors.Join(cancelErrors...); err != nil {
		return adbc.Error{
			Code: adbc.StatusUnknown,
			Msg:  fmt.Sprintf("[bq] failed to cancel BigQuery jobs: %s", err),
		}
	}
	return nil
}

type jobCancelWatch struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	finished chan struct{}
}

func (w *jobCancelWatch) stop() { w.stopOnce.Do(func() { close(w.stopCh) }) }

func (w *jobCancelWatch) wait() { <-w.finished }

// watchJobForCancellation cancels work that outlives its caller. Whether the
// job reached a terminal state, rather than which channel wakes the watcher,
// decides whether jobs.cancel is needed.
func watchJobForCancellation(
	ctx context.Context,
	logger *slog.Logger,
	job *cancellableJob,
	abandoned func() bool,
) *jobCancelWatch {
	watch := &jobCancelWatch{stopCh: make(chan struct{}), finished: make(chan struct{})}
	go func() {
		defer close(watch.finished)
		// The execution path closes stopCh after publishing whether it saw a
		// terminal status. Waiting for that handoff avoids cancelling a job
		// that completed just as the query context was cancelled.
		<-watch.stopCh
		if !abandoned() {
			return
		}

		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobCancelTimeout)
		defer cancel()
		if err := job.cancel(cancelCtx); err != nil {
			if logger != nil {
				logger.WarnContext(cancelCtx, "failed to cancel job, so it may keep running and billing until it finishes", "id", job.ID(), "error", err)
			}
			return
		}
		if logger != nil {
			logger.DebugContext(cancelCtx, "cancelled job", "id", job.ID())
		}
	}()
	return watch
}

// executionBoundReader keeps a statement cancellable while its result stream
// is live, then releases that state at EOF, error, or final Release.
type executionBoundReader struct {
	inner  array.RecordReader
	refs   atomic.Int64
	done   sync.Once
	finish func()
}

func bindExecutionReader(inner array.RecordReader, finish func()) array.RecordReader {
	r := &executionBoundReader{inner: inner, finish: finish}
	r.refs.Store(1)
	return r
}

func (r *executionBoundReader) finishOnce() {
	r.done.Do(func() {
		if r.finish != nil {
			r.finish()
		}
	})
}

func (r *executionBoundReader) Retain() {
	r.refs.Add(1)
	r.inner.Retain()
}

func (r *executionBoundReader) Release() {
	r.inner.Release()
	if r.refs.Add(-1) == 0 {
		r.finishOnce()
	}
}

func (r *executionBoundReader) Schema() *arrow.Schema { return r.inner.Schema() }

func (r *executionBoundReader) Next() bool {
	if r.inner.Next() {
		return true
	}
	r.finishOnce()
	return false
}

func (r *executionBoundReader) RecordBatch() arrow.RecordBatch { return r.inner.RecordBatch() }
func (r *executionBoundReader) Record() arrow.RecordBatch      { return r.RecordBatch() }
func (r *executionBoundReader) Err() error                     { return r.inner.Err() }

var _ array.RecordReader = (*executionBoundReader)(nil)
