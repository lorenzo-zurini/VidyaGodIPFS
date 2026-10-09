package main

import (
	"runtime"
	"testing"
)

// The filestore root: "/" on Unix; on Windows the volume holding the repo, since "/" there is under no drive and
// refused every file added by reference. Teeth: root Windows at "/" again (the Windows case below fails there; on
// Linux the Unix case still pins "/").
func TestFilestoreRootIsTheVolumeOnWindows(t *testing.T) {
	if got := filestoreRoot("linux", "/home/u/.VidyaGod/IPFS"); got != "/" {
		t.Fatalf("unix root %q, want /", got)
	}
	if runtime.GOOS == "windows" {
		if got := filestoreRoot("windows", `D:\a\_temp\data\IPFS`); got != `D:\` {
			t.Fatalf("windows root %q, want D:\\", got)
		}
	}
}
