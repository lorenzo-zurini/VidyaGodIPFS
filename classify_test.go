package main

// classifyFetchErr is the contract the C++ rolling dispatcher depends on: which single-attempt outcomes are
// Retryable (rotate + back off, retry-forever without holding a slot) vs Terminal (fail for good). Only a USER
// cancel (isCancelled) or an errLocalFatal is Terminal; everything transient — including getRoot's OWN internal
// context.Canceled timeout — is Retryable. The isolation matrix caught the bug where context.Canceled was treated
// as terminal and every gated fetch died after one attempt.

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassifyFetchErr(t *testing.T) {
	const cid = "CID_CLASSIFY"
	clearCancel(cid)
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil is done", nil, fetchDone},
		{"stall is retryable", errIncomplete, fetchRetryable},
		{"missing-files is retryable (ref cleared, refetch)", errMissingFiles, fetchRetryable},
		{"a bare network error is retryable", errors.New("no providers found"), fetchRetryable},
		{"offline is retryable", errors.New("node not started"), fetchRetryable},
		{"getRoot's internal ctx timeout is retryable, NOT terminal", context.Canceled, fetchRetryable},
		{"local-fatal is terminal", localFatal(errors.New("no space left on device")), fetchTerminal},
		{"wrapped local-fatal is terminal", fmt.Errorf("outer: %w", errLocalFatal), fetchTerminal},
	}
	for _, c := range cases {
		if got := classifyFetchErr(cid, c.err); got != c.want {
			t.Errorf("%s: classifyFetchErr(%v) = %d, want %d", c.name, c.err, got, c.want)
		}
	}

	// With the CID user-cancelled, ANY non-nil error is Terminal (the cancel surfaces as "cancelled" or, inside
	// getRoot, as context.Canceled) — but a successful result is still Done (a cancel that lost the race).
	requestCancel(cid)
	defer clearCancel(cid)
	if got := classifyFetchErr(cid, context.Canceled); got != fetchTerminal {
		t.Errorf("a user-cancelled fetch must be terminal, got %d", got)
	}
	if got := classifyFetchErr(cid, errors.New("no providers")); got != fetchTerminal {
		t.Errorf("a user-cancelled fetch must be terminal regardless of the error, got %d", got)
	}
	if got := classifyFetchErr(cid, nil); got != fetchDone {
		t.Errorf("a completed fetch is Done even if a cancel was pending, got %d", got)
	}
}
