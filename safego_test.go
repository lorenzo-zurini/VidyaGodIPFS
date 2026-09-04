package main

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// The whole point of safego.go: a panic on a background path must be contained, not crash the process. If any of
// these did NOT recover, the test binary itself would die — so a green run IS the proof.

func TestGuardRecoversPanic(t *testing.T) {
	ran := false
	guard("test", func() { ran = true; panic("boom") })
	if !ran {
		t.Fatal("fn did not run")
	}
	// Reaching here at all means the panic was recovered (an unrecovered panic would have killed the test binary).
}

func TestGuardRunsCleanFn(t *testing.T) {
	got := 0
	guard("test", func() { got = 42 })
	if got != 42 {
		t.Fatalf("clean fn side effect lost: got %d", got)
	}
}

func TestSafeGoRecoversAndReturns(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	safeGo("test", func() { defer wg.Done(); panic("boom in goroutine") })
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("safeGo goroutine never completed (panic not contained?)")
	}
}

func TestGuardErrConvertsPanicToError(t *testing.T) {
	err := guardErr("test", func() error { panic("boom") })
	if err == nil {
		t.Fatal("panic was not converted to an error")
	}
	// A normal error must pass through unchanged.
	sentinel := errors.New("real error")
	if got := guardErr("test", func() error { return sentinel }); !errors.Is(got, sentinel) {
		t.Fatalf("clean error not returned: got %v", got)
	}
	// A nil return must stay nil.
	if got := guardErr("test", func() error { return nil }); got != nil {
		t.Fatalf("nil return became %v", got)
	}
}
