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

package bigquery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type recordingCanceller struct {
	mu         sync.Mutex
	calls      int
	hadError   error
	cancelled  chan struct{}
	deadlineOK bool
}

func newRecordingCanceller(err error) *recordingCanceller {
	return &recordingCanceller{hadError: err, cancelled: make(chan struct{}, 1)}
}

func (c *recordingCanceller) ID() string { return "test-job" }

func (c *recordingCanceller) Cancel(ctx context.Context) error {
	c.mu.Lock()
	c.calls++
	// The request must not inherit the context that was just cancelled, or it
	// could never be delivered.
	c.deadlineOK = ctx.Err() == nil
	c.mu.Unlock()
	select {
	case c.cancelled <- struct{}{}:
	default:
	}
	return c.hadError
}

func (c *recordingCanceller) observed() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.deadlineOK
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A cancelled context must make the driver ask BigQuery to stop the job, on a
// context that is still usable.
func TestCancelJobOnContextDoneCancelsTheJob(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	ctx, cancel := context.WithCancel(context.Background())
	stop := cancelJobOnContextDone(ctx, quietLogger(), canceller)

	cancel()
	select {
	case <-canceller.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never cancelled after its context was done")
	}
	stop()

	calls, deadlineOK := canceller.observed()
	if calls != 1 {
		t.Fatalf("jobs.cancel called %d times, want 1", calls)
	}
	if !deadlineOK {
		t.Fatal("jobs.cancel was given an already-cancelled context, so it could not be delivered")
	}
}

// A query that finishes normally must not have its job cancelled.
func TestCancelJobOnContextDoneLeavesAFinishedQueryAlone(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := cancelJobOnContextDone(ctx, quietLogger(), canceller)
	stop()
	// Cancelling after the watcher has stopped must not reach the job.
	cancel()
	time.Sleep(200 * time.Millisecond)

	if calls, _ := canceller.observed(); calls != 0 {
		t.Fatalf("jobs.cancel called %d times for a query that was not abandoned, want 0", calls)
	}
}

// A failed cancel is reported, not retried or escalated: the query is already
// beyond this client's reach.
func TestCancelJobOnContextDoneToleratesAFailedCancel(t *testing.T) {
	canceller := newRecordingCanceller(errors.New("cancel refused"))
	ctx, cancel := context.WithCancel(context.Background())
	stop := cancelJobOnContextDone(ctx, quietLogger(), canceller)

	cancel()
	select {
	case <-canceller.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never cancelled after its context was done")
	}
	stop()

	if calls, _ := canceller.observed(); calls != 1 {
		t.Fatalf("jobs.cancel called %d times, want 1", calls)
	}
}
