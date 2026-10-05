package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OlaecheaMK0/workq/internal/queue"
)

type fakeQueue struct {
	heartbeatErr error
	completed    atomic.Int32
	failed       atomic.Int32
}

func (f *fakeQueue) Claim(context.Context, time.Duration) (queue.Job, error) {
	return queue.Job{}, queue.ErrNotFound
}
func (f *fakeQueue) RecoverExpired(context.Context) (int64, error)             { return 0, nil }
func (f *fakeQueue) Heartbeat(context.Context, queue.Job, time.Duration) error { return f.heartbeatErr }
func (f *fakeQueue) Complete(context.Context, queue.Job, json.RawMessage) error {
	f.completed.Add(1)
	return nil
}
func (f *fakeQueue) Fail(context.Context, queue.Job, string, time.Duration) error {
	f.failed.Add(1)
	return nil
}

func configured(f *fakeQueue, h Handler) *Worker {
	return &Worker{Queue: f, Handle: h, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Lease: 300 * time.Millisecond, Poll: time.Millisecond, RetryBase: time.Millisecond, RetryMax: time.Second, Timeout: time.Second}
}

func TestLostLeaseCancelsHandlerWithoutAcknowledgment(t *testing.T) {
	f := &fakeQueue{heartbeatErr: queue.ErrLeaseLost}
	var canceled atomic.Bool
	w := configured(f, func(ctx context.Context, j queue.Job) (json.RawMessage, error) {
		<-ctx.Done()
		canceled.Store(true)
		return nil, ctx.Err()
	})
	w.execute(context.Background(), queue.Job{ID: "job", Attempts: 1})
	if !canceled.Load() || f.completed.Load() != 0 || f.failed.Load() != 0 {
		t.Fatal("lost owner acknowledged work or failed to cancel handler")
	}
}

func TestHandlerPanicBecomesRetryableFailure(t *testing.T) {
	f := &fakeQueue{}
	w := configured(f, func(context.Context, queue.Job) (json.RawMessage, error) { panic("synthetic panic") })
	w.execute(context.Background(), queue.Job{Attempts: 1})
	if f.failed.Load() != 1 || f.completed.Load() != 0 {
		t.Fatal("panic was not failed safely")
	}
}

func TestTimedOutHandlerFailsWithoutStoppingWorker(t *testing.T) {
	f := &fakeQueue{}
	w := configured(f, func(ctx context.Context, j queue.Job) (json.RawMessage, error) { <-ctx.Done(); return nil, ctx.Err() })
	w.Timeout = 20 * time.Millisecond
	w.execute(context.Background(), queue.Job{Attempts: 1})
	if f.failed.Load() != 1 {
		t.Fatal("timed out job was not failed")
	}
}

func TestShutdownLeavesLeaseForRecovery(t *testing.T) {
	f := &fakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	w := configured(f, func(context.Context, queue.Job) (json.RawMessage, error) {
		cancel()
		return nil, errors.New("shutdown")
	})
	w.execute(ctx, queue.Job{Attempts: 1})
	if f.failed.Load() != 0 || f.completed.Load() != 0 {
		t.Fatal("shutdown acknowledged job")
	}
}

func TestReportValidationAndCancellation(t *testing.T) {
	for _, body := range []string{`{}`, `{"name":"x","items":[]}`, `{"name":"x","items":[1],"fail_until_attempt":11}`, `{"name":"x","items":[1],"unexpected":true}`} {
		if _, err := DecodeReport(json.RawMessage(body)); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handler := ReportHandler(nil)
	_, err := handler(ctx, queue.Job{Kind: "report", Payload: json.RawMessage(`{"name":"x","items":[1],"delay_ms":1000}`)})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
