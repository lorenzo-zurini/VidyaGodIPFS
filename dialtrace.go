package main

// dialtrace.go — WHO opens our outbound connections (VG_DIAL_TRACE=1). Every outbound dial passes the connection
// gater's InterceptPeerDial in the goroutine that asked for it, so its stack names the subsystem (the DHT, autorelay,
// bitswap, holepunch, our own code). Tallied by the first frame outside libp2p's dial plumbing and printed with the
// [net] line. Diagnosis only: allows every dial, costs a stack walk per dial while enabled.

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
)

func dialTraceOn() bool { return os.Getenv("VG_DIAL_TRACE") == "1" }

type dialTracer struct {
	mu    sync.Mutex
	count map[string]int
}

func newDialTracer() *dialTracer { return &dialTracer{count: map[string]int{}} }

// dialPlumbing: frames that are the dial machinery, not who asked.
// (Module paths carry a version — …/go-libp2p@v0.43.0/p2p/net/swarm/… — so match the path below the module root.)
var dialPlumbing = []string{"/dialtrace.go", "/netgate.go", "/p2p/net/swarm/", "/p2p/host/basic/", "/p2p/host/routed/", "/p2p/host/blank/",
	"/runtime/", "/core/network/", "/core/host/", "/p2p/net/connmgr/", "/p2p/protocol/identify/"}

// dhtQueryPlumbing: the DHT's query engine — a dial from it is attributed to whoever started the query.
var dhtQueryPlumbing = []string{"/query.go", "/lookup.go", "/lookup_optim.go", "/dht.go", "/routing.go", "/handlers.go",
	"/internal/net/", "/qpeerset/", "/netsize/"}

func shortFn(fn string) string {
	if i := strings.LastIndex(fn, "/"); i >= 0 {
		fn = fn[i+1:]
	}
	return fn
}

func dialCaller() string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	first := ""
	for {
		f, more := frames.Next()
		plumbing := false
		for _, p := range dialPlumbing {
			if strings.Contains(f.File, p) {
				plumbing = true
				break
			}
		}
		if !plumbing {
			inKadQuery := false
			if strings.Contains(f.File, "go-libp2p-kad-dht@") {
				for _, p := range dhtQueryPlumbing {
					if strings.Contains(f.File, p) {
						inKadQuery = true
						break
					}
				}
			}
			if !inKadQuery {
				if first != "" {
					return "dht<-" + shortFn(f.Function)
				}
				return shortFn(f.Function)
			}
			if first == "" {
				first = f.Function
			}
		}
		if !more {
			if first != "" {
				return "dht<-(query goroutine)"
			}
			return "?"
		}
	}
}

// note tallies the caller of the dial being gated (call it from the gater, in the dialing goroutine).
func (d *dialTracer) note() {
	c := dialCaller()
	d.mu.Lock()
	d.count[c]++
	d.mu.Unlock()
}

// drain returns the tally since the last call, largest first.
func (d *dialTracer) drain() string {
	d.mu.Lock()
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range d.count {
		all = append(all, kv{k, v})
	}
	d.count = map[string]int{}
	d.mu.Unlock()
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v })
	var b strings.Builder
	for i, e := range all {
		if i == 8 {
			break
		}
		fmt.Fprintf(&b, " %s=%d", e.k, e.v)
	}
	return b.String()
}

var activeDialTracer *dialTracer
