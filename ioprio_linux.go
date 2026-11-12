package main

import (
	"runtime"

	"golang.org/x/sys/unix"
)

const (
	ioprioWhoThread  = 1 // IOPRIO_WHO_PROCESS: with 0, the calling thread
	ioprioClassIdle  = 3
	ioprioClassShift = 13
)

// backgroundIO puts the calling goroutine's reads at the idle IO class — served after every other read on the disk, so
// a pass reading gigabytes never slows a game loading or the user's own copy — until the returned func is called.
// The goroutine is locked to its thread for the while (LockOSThread): a thread Go starts meanwhile is then started by
// its template thread, never cloned from this one, so the idle class does not leak into Go's pool (a thread cloned
// from a C thread inside a cgo call inherits its IO priority, and would run peers' serves at idle for good).
func backgroundIO() (restore func()) {
	runtime.LockOSThread()
	was, err := ioprioThread()
	if err != nil || ioprioSetThread(ioprioClassIdle<<ioprioClassShift) != nil {
		runtime.UnlockOSThread()
		return func() {}
	}
	return func() {
		_ = ioprioSetThread(was)
		runtime.UnlockOSThread()
	}
}

// ioprioThread: the calling thread's IO priority as the kernel states it (class << 13 | level).
func ioprioThread() (int, error) {
	r, _, errno := unix.Syscall(unix.SYS_IOPRIO_GET, ioprioWhoThread, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

func ioprioSetThread(prio int) error {
	if _, _, errno := unix.Syscall(unix.SYS_IOPRIO_SET, ioprioWhoThread, 0, uintptr(prio)); errno != 0 {
		return errno
	}
	return nil
}
