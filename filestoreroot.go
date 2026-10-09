package main

import (
	"path/filepath"
)

// fileRoot is the directory the filestore's references are recorded relative to (boxo's FileManager root), set once
// when the node opens. Every place that turns a stored reference back into a path joins it to this root.
var fileRoot = "/"

// filestoreRoot: on Unix "/", under which every absolute path lies. Windows has no single root over all drives, so it
// is the volume holding the repo ("C:\"): files on that drive are added by reference, and one on another drive is
// refused by the filestore with its own clear error. Rooting at "/" there refused every file ("cannot add filestore
// references outside ipfs root"): nothing could be seeded or published on Windows.
func filestoreRoot(goos, repoPath string) string {
	if goos != "windows" {
		return "/"
	}
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		abs = repoPath
	}
	if v := filepath.VolumeName(abs); v != "" {
		return v + `\`
	}
	return `\`
}
