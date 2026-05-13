package ws

import (
	"testing"
	"time"
)

func TestBackoffDelayCapsAndJitters(t *testing.T) {
	backoff := Backoff{
		Base: time.Second,
		Max:  10 * time.Second,
		rand: func(max time.Duration) time.Duration {
			return max
		},
	}

	if got := backoff.Delay(0); got != time.Second {
		t.Fatalf("unexpected first delay %s", got)
	}
	if got := backoff.Delay(10); got != 10*time.Second {
		t.Fatalf("expected capped delay, got %s", got)
	}
}

func TestBackoffDelayNormalizesDefaultsAndAvoidsOverflow(t *testing.T) {
	backoff := Backoff{
		Base: 1 << 62 * time.Nanosecond,
		Max:  1<<63 - 1,
		rand: func(max time.Duration) time.Duration {
			return 0
		},
	}

	if got := backoff.Delay(10); got < 0 {
		t.Fatalf("expected non-negative delay, got %s", got)
	}

	defaulted := Backoff{}
	_ = defaulted.Delay(0)
	if defaulted.Base != time.Second || defaulted.Max != time.Minute {
		t.Fatalf("expected persisted defaults, got base=%s max=%s", defaulted.Base, defaulted.Max)
	}
}
