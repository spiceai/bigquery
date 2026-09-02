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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cloudbigquery "cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type recordingJobCanceller struct {
	mu        sync.Mutex
	calls     int
	err       error
	block     chan struct{}
	called    chan struct{}
	usableCtx bool
}

func newRecordingJobCanceller(err error) *recordingJobCanceller {
	return &recordingJobCanceller{err: err, called: make(chan struct{}, 1)}
}

func (c *recordingJobCanceller) ID() string { return "test-job" }

func (c *recordingJobCanceller) Cancel(ctx context.Context) error {
	c.mu.Lock()
	c.calls++
	c.usableCtx = ctx.Err() == nil
	block := c.block
	c.mu.Unlock()
	select {
	case c.called <- struct{}{}:
	default:
	}
	if block != nil {
		<-block
	}
	return c.err
}

func (c *recordingJobCanceller) observed() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.usableCtx
}

func testCancellableJob(canceller jobCanceller) *cancellableJob {
	job := newCancellableJob(canceller.ID(), nil)
	job.setJob(canceller)
	return job
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func requireStatus(t *testing.T, err error, status adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, status, adbcErr.Code)
}

func awaitCancel(t *testing.T, canceller *recordingJobCanceller) {
	t.Helper()
	select {
	case <-canceller.called:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never cancelled")
	}
}

func TestWatchCancelsJobWhenContextIsDone(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	job := testCancellableJob(canceller)
	ctx, cancel := context.WithCancel(context.Background())
	watch := watchJobForCancellation(ctx, quietLogger(), job, func() bool { return true })

	cancel()
	watch.stop()
	awaitCancel(t, canceller)
	watch.wait()

	calls, usableCtx := canceller.observed()
	require.Equal(t, 1, calls)
	require.True(t, usableCtx, "jobs.cancel inherited the cancelled query context")
}

func TestWatchDoesNotCancelFinishedJob(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	job := testCancellableJob(canceller)
	job.markFinished()
	watch := watchJobForCancellation(context.Background(), quietLogger(), job, func() bool { return false })

	watch.stop()
	watch.wait()

	calls, _ := canceller.observed()
	require.Zero(t, calls)
}

func TestWatchWaitsForTerminalStatePublication(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	job := testCancellableJob(canceller)
	ctx, cancel := context.WithCancel(context.Background())
	watch := watchJobForCancellation(ctx, quietLogger(), job, func() bool {
		return !job.finished.Load()
	})

	cancel()
	job.markFinished()
	watch.stop()
	watch.wait()

	calls, _ := canceller.observed()
	require.Zero(t, calls)
}

func TestWatchCancelsJobLeftRunningByFailedPoll(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	watch := watchJobForCancellation(context.Background(), quietLogger(), testCancellableJob(canceller), func() bool { return true })

	watch.stop()
	awaitCancel(t, canceller)
	watch.wait()

	calls, _ := canceller.observed()
	require.Equal(t, 1, calls)
}

func TestWatchStopDoesNotWaitForCancelRequest(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	canceller.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	watch := watchJobForCancellation(ctx, quietLogger(), testCancellableJob(canceller), func() bool { return true })
	cancel()

	returned := make(chan struct{})
	go func() {
		watch.stop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("stopping the watch waited for jobs.cancel")
	}
	awaitCancel(t, canceller)
	close(canceller.block)
	watch.wait()
}

func TestWatchCancellationWinsRaceWithStop(t *testing.T) {
	for i := range 50 {
		canceller := newRecordingJobCanceller(nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		watch := watchJobForCancellation(ctx, quietLogger(), testCancellableJob(canceller), func() bool { return true })
		watch.stop()
		watch.wait()

		calls, _ := canceller.observed()
		require.Equalf(t, 1, calls, "iteration %d", i)
	}
}

func TestStatementCancelReturnsInvalidStateWithoutActiveQuery(t *testing.T) {
	err := (&statement{}).Cancel(context.Background())
	requireStatus(t, err, adbc.StatusInvalidState)
}

func TestStatementCancelReturnsUnknownWhenJobCancelFails(t *testing.T) {
	canceller := newRecordingJobCanceller(errors.New("cancel refused"))
	st := &statement{}
	ctx, execution := st.beginExecution(context.Background())
	flight := testCancellableJob(canceller)
	st.inFlight = map[*cancellableJob]struct{}{flight: {}}

	err := st.Cancel(context.Background())
	requireStatus(t, err, adbc.StatusUnknown)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	st.endJob(flight)
	st.endExecution(execution)
}

func TestStatementCancelDoesNotCancelTerminalJob(t *testing.T) {
	canceller := newRecordingJobCanceller(nil)
	flight := testCancellableJob(canceller)
	flight.markFinished()
	st := &statement{}
	_, execution := st.beginExecution(context.Background())
	st.inFlight = map[*cancellableJob]struct{}{flight: {}}

	require.NoError(t, st.Cancel(context.Background()))
	calls, _ := canceller.observed()
	require.Zero(t, calls)

	st.endJob(flight)
	st.endExecution(execution)
}

func TestStatementCancelCancelsEveryInFlightJob(t *testing.T) {
	first := newRecordingJobCanceller(nil)
	second := newRecordingJobCanceller(nil)
	firstJob := testCancellableJob(first)
	secondJob := testCancellableJob(second)
	st := &statement{}
	_, execution := st.beginExecution(context.Background())
	st.inFlight = map[*cancellableJob]struct{}{firstJob: {}, secondJob: {}}

	require.NoError(t, st.Cancel(context.Background()))
	firstCalls, _ := first.observed()
	secondCalls, _ := second.observed()
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 1, secondCalls)

	st.endJob(firstJob)
	st.endJob(secondJob)
	st.endExecution(execution)
}

func TestStreamRecordBatchesReportsCancellation(t *testing.T) {
	schema := arrow.NewSchema(nil, nil)
	source, err := array.NewRecordReader(schema, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := &reader{refCount: 1, cancelFn: func() {}, schema: schema}
	ch := make(chan arrow.RecordBatch)

	streamRecordBatches(ctx, source, result, ch)

	_, open := <-ch
	require.False(t, open)
	requireStatus(t, result.Err(), adbc.StatusCancelled)
}

func TestExecutionBoundReaderFinishesAtEOF(t *testing.T) {
	inner, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
	require.NoError(t, err)
	finished := make(chan struct{})
	rdr := bindExecutionReader(inner, func() { close(finished) })

	require.False(t, rdr.Next())
	select {
	case <-finished:
	default:
		t.Fatal("execution remained active after EOF")
	}
	rdr.Release()
}

type acceptedJobServer struct {
	t         *testing.T
	server    *httptest.Server
	projectID string
	location  string
	accepted  chan string
	cancelled chan string
}

func newAcceptedJobServer(t *testing.T) *acceptedJobServer {
	s := &acceptedJobServer{
		t:         t,
		projectID: "test-project",
		location:  "us-west1",
		accepted:  make(chan string, 1),
		cancelled: make(chan string, 1),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

func (s *acceptedJobServer) close() { s.server.Close() }

func (s *acceptedJobServer) jobResource(jobID, state string) string {
	return fmt.Sprintf(`{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}},"jobReference":{"projectId":%q,"location":%q,"jobId":%q},"status":{"state":%q}}`, s.projectID, s.location, jobID, state)
}

func (s *acceptedJobServer) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/"+s.projectID+"/jobs")
	if r.Method == http.MethodPost && path == "" {
		var body struct {
			JobReference struct {
				JobID string `json:"jobId"`
			} `json:"jobReference"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Errorf("decode jobs.insert body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.accepted <- body.JobReference.JobID
		<-r.Context().Done()
		return
	}

	jobID := strings.TrimPrefix(strings.TrimSuffix(path, "/cancel"), "/")
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, s.jobResource(jobID, "RUNNING"))
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(path, "/cancel") {
		s.cancelled <- jobID
		_, _ = fmt.Fprintf(w, `{"job":%s}`, s.jobResource(jobID, "DONE"))
		return
	}
	http.NotFound(w, r)
}

func TestStatementCancelFindsJobWhenInsertResponseIsLost(t *testing.T) {
	srv := newAcceptedJobServer(t)
	defer srv.close()
	client, err := cloudbigquery.NewClient(
		context.Background(),
		srv.projectID,
		option.WithEndpoint(srv.server.URL+"/bigquery/v2/"),
		option.WithHTTPClient(srv.server.Client()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	client.Location = srv.location

	st := &statement{
		cnxn:        &connectionImpl{client: client, ConnectionImplBase: connectionBaseForCancellationTest()},
		queryConfig: cloudbigquery.QueryConfig{Q: "SELECT 1"},
	}
	execErr := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(context.Background())
		execErr <- err
	}()

	var jobID string
	select {
	case jobID = <-srv.accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("jobs.insert was not accepted")
	}
	require.NotEmpty(t, jobID)
	require.NoError(t, st.Cancel(context.Background()))

	select {
	case cancelledID := <-srv.cancelled:
		require.Equal(t, jobID, cancelledID)
	case <-time.After(10 * time.Second):
		t.Fatal("jobs.cancel was not sent")
	}
	select {
	case err := <-execErr:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled execution did not return")
	}
}

func connectionBaseForCancellationTest() driverbase.ConnectionImplBase {
	base := driverbase.ConnectionImplBase{
		Logger: quietLogger()}
	return base
}
