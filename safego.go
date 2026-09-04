package main

// safego.go — the node's ONLY panic boundary. The embedded node is a c-shared library linked into the Qt app, and
// Go's default is fatal: a panic in ANY goroutine, if it unwinds to the top with no recover, kills the WHOLE process
// — bitswap, the DHT, the friend/overlay LAN, and the GUI, all at once. That is the "the whole node just went off"
// field failure: a latent bug on a background path (the always-on link maintainer's heartbeat tick, a per-conn
// datagram loop, a presence send, a re-dial) takes down a node that was, a moment earlier, happily serving a
// download. These helpers make a background panic a LOGGED, LOCAL event instead of a process death.
//
// Use guard() around the per-iteration body of a long-lived loop (so one bad tick/packet is skipped and the loop —
// and the node — keep running), safeGo() for a one-shot background goroutine, and guardErr() to convert a panic in a
// fallible step (e.g. goOnline) into an error the caller already knows how to handle (retry) rather than a crash.

import (
	"fmt"
	"os"
	"runtime/debug"
)

// guard runs fn synchronously, recovering (and logging, with stack) any panic so it cannot propagate to crash the
// process. Wrap the body of a long-lived loop with it to keep the loop alive across a bad iteration.
func guard(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[node] PANIC recovered in %s: %v\n%s\n", name, r, debug.Stack())
		}
	}()
	fn()
}

// safeGo launches fn in a new goroutine under guard — a panic ends only that goroutine (logged), never the process.
func safeGo(name string, fn func()) { go guard(name, fn) }

// guardErr runs fn and returns its error, converting a panic into an error so a fallible step that panics is handled
// by the caller's existing failure path (e.g. the goOnline retry loop) instead of taking the node down.
func guardErr(name string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[node] PANIC recovered in %s: %v\n%s\n", name, r, debug.Stack())
			err = fmt.Errorf("panic in %s: %v", name, r)
		}
	}()
	return fn()
}
