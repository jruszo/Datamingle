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
