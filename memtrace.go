package main

// memtrace.go — where the node's memory is, on request. A receiver ended a 63 GB replication at 6.2 GB resident (it
// idles at ~0.4 GB with the same library), and nothing could say whether that was the Go heap or the C++ side, live
// or merely retained. VG_MEM_TRACE=1 prints a [mem] line with each [net] line: the Go heap (in use, idle, released to
// the OS) and the C allocator (in use, free but kept). VG_PPROF=127.0.0.1:<port> serves Go's heap profiles
// (go tool pprof http://127.0.0.1:<port>/debug/pprof/heap) — loopback only, never a public address.

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux, served only by startPprof
	"os"
	"runtime"
)

var memTrace = os.Getenv("VG_MEM_TRACE") != ""

// memLine: the Go heap and the C allocator, in MiB.
func memLine() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	cInUse, cFree := cAllocator()
	mb := func(b uint64) uint64 { return b >> 20 }
	return fmt.Sprintf("[mem] go: heap in use %d, idle %d, released %d, sys %d, objects %d, gc %d | c: in use %d, free kept %d (MiB)",
		mb(m.HeapInuse), mb(m.HeapIdle-m.HeapReleased), mb(m.HeapReleased), mb(m.Sys), m.HeapObjects, m.NumGC, mb(cInUse), mb(cFree))
}

// startPprof serves Go's profiles at VG_PPROF when it names a loopback address.
func startPprof() {
	addr := os.Getenv("VG_PPROF")
	if addr == "" {
		return
	}
	host, _, err := net.SplitHostPort(addr)
	if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() {
		fmt.Fprintf(os.Stderr, "[mem] VG_PPROF=%q is not a loopback host:port — profiles not served\n", addr)
		return
	}
	safeGo("node.pprof", func() {
		fmt.Fprintf(os.Stderr, "[mem] Go profiles at http://%s/debug/pprof/\n", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Fprintf(os.Stderr, "[mem] pprof: %v\n", err)
		}
	})
}
