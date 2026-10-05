package queue

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFingerprintIgnoresObjectOrderButPreservesNumbers(t *testing.T) {
	makeHash := func(payload string) string {
		t.Helper()
		hash, err := Fingerprint(Request{Kind: "report", Payload: json.RawMessage(payload), MaxAttempts: 3})
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	if makeHash(`{"a":1,"b":2}`) != makeHash(`{"b":2,"a":1}`) {
		t.Fatal("object-key order changed identity")
	}
	if makeHash(`{"a":9007199254740992}`) == makeHash(`{"a":9007199254740993}`) {
		t.Fatal("distinct integers collapsed")
	}
	if makeHash(`{"a":[1,2]}`) == makeHash(`{"a":[2,1]}`) {
		t.Fatal("array ordering must affect identity")
	}
}

func TestRetryDelayBounds(t *testing.T) {
	cases := []struct {
		attempt   int
		low, high time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{3, 200 * time.Millisecond, 400 * time.Millisecond},
		{100, 250 * time.Millisecond, 500 * time.Millisecond},
	}
	for _, tc := range cases {
		for i := 0; i < 100; i++ {
			got := RetryDelay(tc.attempt, 100*time.Millisecond, 500*time.Millisecond)
			if got < tc.low || got > tc.high {
				t.Fatalf("attempt %d delay %s outside [%s,%s]", tc.attempt, got, tc.low, tc.high)
			}
		}
	}
}

func TestIDRoundTrip(t *testing.T) {
	a, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidID(a) || a == b {
		t.Fatal("invalid or repeated ID")
	}
	for _, bad := range []string{"", "../../jobs", "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", a + "x"} {
		if ValidID(bad) {
			t.Fatalf("accepted %q", bad)
		}
	}
}
