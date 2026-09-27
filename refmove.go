package main

// refmove.go — a folder moved on disk takes its filestore references with it.
//
// A fetched file is referenced IN PLACE: each leaf's filestore entry names the file's path and the leaf's offset.
// Installing a received package moves its folder (CATALOG → LIBRARY); without this every reference under it named
// a path that no longer existed — the content read as missing, could not be served to anyone, and the startup heal
// (which re-seeds package-source dirs, not received ones) could not fix it: 678 on one replication receiver.
// moveRefs re-points them: same leaf, same offset, same size, the new path. Nothing is read or hashed — the bytes
// did not change, only where they live.

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
)

// moveRefs re-points every filestore reference whose file lies under oldDir to the same relative path under newDir.
// Returns how many references moved.
func (n *node) moveRefs(ctx context.Context, oldDir, newDir string) (int, error) {
	oldDir, newDir = filepath.Clean(oldDir), filepath.Clean(newDir)
	next, err := filestore.ListAll(ctx, n.fstore, false)
	if err != nil {
		return 0, err
	}
	var moved []*posinfo.FilestoreNode
	for r := next(ctx); r != nil; r = next(ctx) {
		if r.FilePath == "" {
			continue // a plain block, not a reference
		}
		p := filepath.Join("/", r.FilePath)
		rel, err := filepath.Rel(oldDir, p)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue // not under oldDir
		}
		moved = append(moved, &posinfo.FilestoreNode{
			Node:    &refLeaf{c: r.Key, size: int(r.Size)},
			PosInfo: &posinfo.PosInfo{Offset: r.Offset, FullPath: filepath.Join(newDir, rel)},
		})
	}
	if len(moved) == 0 {
		return 0, nil
	}
	if err := n.fstore.FileManager().PutMany(ctx, moved); err != nil { // overwrites each entry in one commit
		return 0, err
	}
	return len(moved), nil
}
