package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/OlaecheaMK0/workq/internal/queue"
)

type Report struct {
	Name             string  `json:"name"`
	Items            []int64 `json:"items"`
	DelayMS          int     `json:"delay_ms,omitempty"`
	FailUntilAttempt int     `json:"fail_until_attempt,omitempty"`
}

func DecodeReport(payload json.RawMessage) (Report, error) {
	var report Report
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(&report); err != nil {
		return report, fmt.Errorf("invalid report payload: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return report, errors.New("payload must contain one JSON object")
	}
	if len(report.Name) < 1 || len(report.Name) > 80 {
		return report, errors.New("name must contain 1 to 80 bytes")
	}
	if len(report.Items) == 0 || len(report.Items) > 1000 {
		return report, errors.New("items must contain 1 to 1000 integers")
	}
	for _, n := range report.Items {
		if n < -1_000_000 || n > 1_000_000 {
			return report, errors.New("each item must be between -1000000 and 1000000")
		}
	}
	if report.DelayMS < 0 || report.DelayMS > 10000 {
		return report, errors.New("delay_ms must be between 0 and 10000")
	}
	if report.FailUntilAttempt < 0 || report.FailUntilAttempt > 10 {
		return report, errors.New("fail_until_attempt must be between 0 and 10")
	}
	return report, nil
}

func Validate(req queue.Request) error {
	if req.Kind != "report" {
		return errors.New("supported kind: report")
	}
	_, err := DecodeReport(req.Payload)
	return err
}

type ReportStore interface {
	SaveReport(context.Context, string, json.RawMessage) (json.RawMessage, error)
}

func ReportHandler(store ReportStore) Handler {
	return func(ctx context.Context, j queue.Job) (json.RawMessage, error) {
		if j.Kind != "report" {
			return nil, errors.New("unsupported job kind")
		}
		report, err := DecodeReport(j.Payload)
		if err != nil {
			return nil, err
		}
		if report.DelayMS > 0 {
			timer := time.NewTimer(time.Duration(report.DelayMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		if j.Attempts <= report.FailUntilAttempt {
			return nil, fmt.Errorf("synthetic failure on attempt %d", j.Attempts)
		}
		var sum int64
		for _, n := range report.Items {
			sum += n
		}
		result, err := json.Marshal(struct {
			Name  string `json:"name"`
			Count int    `json:"count"`
			Sum   int64  `json:"sum"`
		}{report.Name, len(report.Items), sum})
		if err != nil {
			return nil, err
		}
		// Saving the effect is deliberately separate from queue acknowledgment.
		// The unique job_id survives the crash window between those two writes.
		return store.SaveReport(ctx, j.ID, result)
	}
}
