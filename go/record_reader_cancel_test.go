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
	block      chan struct{}
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
	block := c.block
	c.mu.Unlock()
	select {
	case c.cancelled <- struct{}{}:
	default:
	}
	if block != nil {
		<-block
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

func stillRunning() bool { return true }
func alreadyDone() bool  { return false }

func awaitCancel(t *testing.T, c *recordingCanceller) {
	t.Helper()
	select {
	case <-c.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never cancelled")
	}
}

// A cancelled context must make the driver ask BigQuery to stop the job, on a
// context that is still usable.
func TestWatchCancelsWhenContextIsDone(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	ctx, cancel := context.WithCancel(context.Background())
	watch := watchJobForCancellation(ctx, quietLogger(), canceller, stillRunning)

	cancel()
	awaitCancel(t, canceller)
	watch.stop()
	watch.wait()

	calls, deadlineOK := canceller.observed()
	if calls != 1 {
		t.Fatalf("jobs.cancel called %d times, want 1", calls)
	}
	if !deadlineOK {
		t.Fatal("jobs.cancel was given an already-cancelled context, so it could not be delivered")
	}
}

// A query that reached a terminal status must not have its job cancelled.
func TestWatchLeavesAFinishedQueryAlone(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch := watchJobForCancellation(ctx, quietLogger(), canceller, alreadyDone)
	watch.stop()
	watch.wait()

	if calls, _ := canceller.observed(); calls != 0 {
		t.Fatalf("jobs.cancel called %d times for a finished query, want 0", calls)
	}
}

// Polling a job's status can fail while the job is still running. The context is
// live in that path, so only the abandonment check can stop the job.
func TestWatchCancelsAJobLeftRunningByAFailedPoll(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch := watchJobForCancellation(ctx, quietLogger(), canceller, stillRunning)
	watch.stop()
	awaitCancel(t, canceller)
	watch.wait()

	if calls, _ := canceller.observed(); calls != 1 {
		t.Fatalf("jobs.cancel called %d times for a job left running, want 1", calls)
	}
}

// Releasing the watch must not wait for a cancel already in flight: the caller
// is unwinding because nobody is waiting any more, and jobs.cancel has its own
// timeout.
func TestWatchStopDoesNotBlockOnAnInFlightCancel(t *testing.T) {
	canceller := newRecordingCanceller(nil)
	canceller.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch := watchJobForCancellation(ctx, quietLogger(), canceller, stillRunning)
	cancel()
	awaitCancel(t, canceller)

	returned := make(chan struct{})
	go func() {
		watch.stop()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("stop blocked while jobs.cancel was in flight")
	}
	close(canceller.block)
	watch.wait()
}

// A failed cancel is reported, not retried or escalated: the query is already
// beyond this client's reach.
func TestWatchToleratesAFailedCancel(t *testing.T) {
	canceller := newRecordingCanceller(errors.New("cancel refused"))
	ctx, cancel := context.WithCancel(context.Background())
	watch := watchJobForCancellation(ctx, quietLogger(), canceller, stillRunning)

	cancel()
	awaitCancel(t, canceller)
	watch.stop()
	watch.wait()

	if calls, _ := canceller.observed(); calls != 1 {
		t.Fatalf("jobs.cancel called %d times, want 1", calls)
	}
}

// A context that is already done must cancel the job even when the watch is
// released at the same moment.
//
// Both channels are ready before the watching goroutine is first scheduled, so
// deciding on the select alone loses the cancellation about half the time.
// Repeated because the losing side of that race is chosen at random.
func TestWatchCancelsWhenStopRacesADoneContext(t *testing.T) {
	for i := range 50 {
		canceller := newRecordingCanceller(nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		watch := watchJobForCancellation(ctx, quietLogger(), canceller, alreadyDone)
		watch.stop()
		watch.wait()

		if calls, _ := canceller.observed(); calls != 1 {
			t.Fatalf("iteration %d: jobs.cancel called %d times for an abandoned query, want 1", i, calls)
		}
	}
}
