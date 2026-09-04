package main

// vglog.go — the node's verbose diagnostic log. OFF by default (zero overhead: one atomic load per call site).
// Turned on by the app's `--log <file>` flag (which freopens the process's stderr to that file and calls
// VgSetLogVerbose(1) — so BOTH the C++ side's std::cerr and every vlog line below land in the same file), or by
// setting VG_LOG=1 / VG_VERBOSE=1 in the environment (picked up at load time, before the flag path runs).
//
// vlog lines are for tracing the network stack end to end during a field test: connection open/close (with the
// remote multiaddr, so relay vs direct vs which interface is visible), the friend handshake, the link maintainer's
// per-beat reasoning, and every overlay TX/RX decision. Format: "HH:MM:SS.mmm [component] message".

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

var gLogVerbose atomic.Bool

func init() {
	if v := os.Getenv("VG_LOG"); v == "1" || v == "true" {
		gLogVerbose.Store(true)
	}
	if os.Getenv("VG_VERBOSE") == "1" {
		gLogVerbose.Store(true)
	}
}

// logOn reports whether verbose logging is active (for guarding an expensive-to-build log argument).
func logOn() bool { return gLogVerbose.Load() }

// vlog writes one diagnostic line if verbose logging is on. Cheap no-op otherwise.
func vlog(component, format string, args ...any) {
	if !gLogVerbose.Load() {
		return
	}
	fmt.Fprintf(os.Stderr, "%s [%s] "+format+"\n",
		append([]any{time.Now().Format("15:04:05.000"), component}, args...)...)
}
