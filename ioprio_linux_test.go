package main

import (
	"runtime"
	"testing"
)

// A background read is at the idle IO class for its while, and only its while. Teeth: a backgroundIO that sets
// nothing leaves the class as it was; one that does not restore leaves the thread idle.
func TestBackgroundIOIsIdleOnlyWhileItLasts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ioprio is Linux's")
	}
	runtime.LockOSThread() // one thread throughout, to read its class before and after
	defer runtime.UnlockOSThread()
	before, err := ioprioThread()
	if err != nil {
		t.Skip("ioprio_get: ", err)
	}
	restore := backgroundIO()
	during, _ := ioprioThread()
	restore()
	after, _ := ioprioThread()
	if during>>13 != 3 {
		t.Errorf("IO priority during a background read = %#x, want the idle class", during)
	}
	if after != before {
		t.Errorf("IO priority after = %#x, before = %#x", after, before)
	}
}
