package main

// netq.go — THE network queue. A home router tracks every NEW flow; what took a household router down was never the
// connections we held but how many things were each opening new ones at once — fetches bounded by the download
// setting, while their DHT walks, the bitswap provider searches and ~40k DHT provides ran in side pools of their own.
// So every job that puts new flows on the network takes one slot of this one queue:
//
//   - a fetch attempt (the C++ rolling DownloadQueue takes its slot here, VgNetAcquire) — FOREGROUND. What the fetch
//     does to find providers (its warm walk, its bitswap provider search) runs inside that slot and ends with it;
//   - a provider walk or a provider-record send of the sweeping provider (provide.go) — BACKGROUND.
//
// The size is the user's "max simultaneous downloads". A slot goes to a waiting foreground job before any background
// one, so announcing never slows an install; when nothing is fetching, announcing gets every slot. Rolling: a freed
// slot is handed straight to the next waiter.

import (
	"context"
	"sync"
)

type netQueue struct {
	mu       sync.Mutex
	slots    int
	active   int
	fgActive int             // … of which foreground (fetch) jobs
	fg, bg   []chan struct{} // FIFO waiters per priority; a closed channel = granted
	fgIdle   chan struct{}   // closed while no foreground job holds or waits for a slot
}

var netq = newNetQueue(3)

func newNetQueue(slots int) *netQueue {
	idle := make(chan struct{})
	close(idle)
	return &netQueue{slots: slots, fgIdle: idle}
}

// acquire blocks until a slot is granted (fg first) or ctx ends. release must be called exactly once when ok.
func (q *netQueue) acquire(ctx context.Context, fg bool) (release func(), ok bool) {
	q.mu.Lock()
	if fg {
		q.markFgBusyLocked()
	}
	if q.active < q.slots { // a free slot never coexists with waiters: every release/resize grants them first
		q.active++
		if fg {
			q.fgActive++
		}
		q.mu.Unlock()
		return q.releaser(fg), true
	}
	ch := make(chan struct{})
	if fg {
		q.fg = append(q.fg, ch)
	} else {
		q.bg = append(q.bg, ch)
	}
	q.mu.Unlock()
	select {
	case <-ch:
		return q.releaser(fg), true
	case <-ctx.Done():
		q.mu.Lock()
		if removeWaiter(&q.fg, ch) || removeWaiter(&q.bg, ch) {
			q.markFgIdleIfLocked()
			q.mu.Unlock()
			return nil, false
		}
		q.mu.Unlock()
		// granted while we gave up: hand the slot on
		<-ch
		q.releaser(fg)()
		return nil, false
	}
}

func (q *netQueue) releaser(fg bool) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.active--
			if fg {
				q.fgActive--
			}
			q.grantLocked()
			q.markFgIdleIfLocked()
			q.mu.Unlock()
		})
	}
}

// grantLocked hands free slots to waiters, foreground first.
func (q *netQueue) grantLocked() {
	for q.active < q.slots {
		var ch chan struct{}
		switch {
		case len(q.fg) > 0:
			ch, q.fg = q.fg[0], q.fg[1:]
			q.fgActive++
		case len(q.bg) > 0:
			ch, q.bg = q.bg[0], q.bg[1:]
		default:
			return
		}
		q.active++
		close(ch)
	}
}

func (q *netQueue) markFgBusyLocked() {
	select {
	case <-q.fgIdle:
		q.fgIdle = make(chan struct{})
	default:
	}
}

func (q *netQueue) markFgIdleIfLocked() {
	if q.fgActive > 0 || len(q.fg) > 0 {
		return
	}
	select {
	case <-q.fgIdle:
	default:
		close(q.fgIdle)
	}
}

// waitFgIdle blocks until no foreground job holds or waits for a slot (true), or ctx ends (false). Work that should
// not compete with installs — a whole-datastore compaction — waits here.
func (q *netQueue) waitFgIdle(ctx context.Context) bool {
	q.mu.Lock()
	idle := q.fgIdle
	q.mu.Unlock()
	select {
	case <-idle:
		return true
	case <-ctx.Done():
		return false
	}
}

// setSlots resizes the queue: a larger size lets waiters in at once; a smaller one drains as jobs finish.
func (q *netQueue) setSlots(n int) {
	if n < 1 {
		n = 1
	}
	q.mu.Lock()
	q.slots = n
	q.grantLocked()
	q.mu.Unlock()
}

func (q *netQueue) size() int { q.mu.Lock(); defer q.mu.Unlock(); return q.slots }

// load: slots in use and jobs waiting (both priorities).
func (q *netQueue) load() (active, waiting int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.active, len(q.fg) + len(q.bg)
}

func removeWaiter(list *[]chan struct{}, ch chan struct{}) bool {
	for i, w := range *list {
		if w == ch {
			*list = append((*list)[:i], (*list)[i+1:]...)
			return true
		}
	}
	return false
}
