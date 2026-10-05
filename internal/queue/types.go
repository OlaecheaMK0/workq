package queue

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrConflict  = errors.New("idempotency key already belongs to a different request")
	ErrLeaseLost = errors.New("job lease is no longer owned by this worker")
)

type Request struct {
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

type Job struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	State       string          `json:"state"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LeaseUntil  *time.Time      `json:"lease_until,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	LeaseToken  string          `json:"-"`
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}

func ValidID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(id[:8] + id[9:13] + id[14:18] + id[19:23] + id[24:])
	return err == nil
}

// Normalize before hashing so object-key order does not change request identity.
// UseNumber avoids collapsing distinct large integers through float64 rounding.
func Fingerprint(req Request) (string, error) {
	var payload any
	d := json.NewDecoder(bytes.NewReader(req.Payload))
	d.UseNumber()
	if err := d.Decode(&payload); err != nil {
		return "", err
	}
	b, err := json.Marshal(struct {
		Kind        string `json:"kind"`
		Payload     any    `json:"payload"`
		MaxAttempts int    `json:"max_attempts"`
	}{req.Kind, payload, req.MaxAttempts})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// RetryDelay uses equal jitter in [cap/2, cap]. The cap doubles per failed
// attempt, so an outage cannot make every worker retry at the same instant.
func RetryDelay(attempt int, base, max time.Duration) time.Duration {
	cap := base
	for i := 1; i < attempt && cap < max; i++ {
		if cap > max/2 {
			cap = max
			break
		}
		cap *= 2
	}
	if cap > max {
		cap = max
	}
	if cap <= 0 {
		return 0
	}
	floor := cap / 2
	n, err := rand.Int(rand.Reader, big.NewInt(int64(cap-floor)+1))
	if err != nil {
		return cap
	}
	return floor + time.Duration(n.Int64())
}
