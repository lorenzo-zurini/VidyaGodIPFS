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
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/ipfs/boxo/filestore"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
)

// refMove: the files under from now live at the same relative paths under to — or, when from is a file, that one
// file now lives at to (a content file renamed in place: two versions of one name kept side by side).
type refMove struct{ from, to string }

// moveRefs re-points every filestore reference whose file lies under a move's from to the same relative path under
// its to — the longest from that holds the file wins (a landed folder whose files moved up, inside the package dir
// that moved) — in ONE scan of the references (each scan reads the whole index: ~7 s for 5.8 M references).
// Each reference looks its own path and the folders above it up in the moves, nearest first: the first found is
// the longest from that holds it. Comparing every reference with every move cost a publish ~4 s per renamed node
// file (5.8 M references × the package's renames), hours for a library's first publish.
// Returns how many references moved.
func (n *node) moveRefs(ctx context.Context, moves ...refMove) (int, error) {
	moveRefScans.Add(1)
	to := make(map[string]string, len(moves)) // from (folded where the file system folds case) → to
	for _, m := range moves {
		to[refKey(filepath.Clean(m.from))] = filepath.Clean(m.to)
	}
	next, err := filestore.ListAll(ctx, n.fstore, false)
	if err != nil {
		return 0, err
	}
	var moved []*posinfo.FilestoreNode
	for r := next(ctx); r != nil; r = next(ctx) {
		if r.FilePath == "" {
			continue // a plain block, not a reference
		}
		p := filepath.Join(fileRoot, r.FilePath)
		for dir := p; ; {
			if dest, ok := to[refKey(dir)]; ok {
				rel := strings.TrimPrefix(p[len(dir):], string(filepath.Separator))
				moved = append(moved, &posinfo.FilestoreNode{
					Node:    &refLeaf{c: r.Key, size: int(r.Size)},
					PosInfo: &posinfo.PosInfo{Offset: r.Offset, FullPath: filepath.Join(dest, rel)},
				})
				break
			}
			up := filepath.Dir(dir)
			if up == dir {
				break // the root: no move holds this file
			}
			dir = up
		}
	}
	if len(moved) == 0 {
		return 0, nil
	}
	if err := n.fstore.FileManager().PutMany(ctx, moved); err != nil { // overwrites each entry in one commit
		return 0, err
	}
	// The files they named before are named no more: the listing of files (orphanedRefPaths) learns that from the
	// writes — a file it lists that is gone is looked for in the index once more, even one confirmed before.
	n.refRepointed.Store(true)
	return len(moved), nil
}

// moveRefScans counts moveRefs' scans of the whole reference index (a test reads it: a publish moves its references
// in one).
var moveRefScans atomic.Int64

// refKey is how a path is compared with a move's from: as the file system compares names (Windows folds case).
func refKey(p string) string {
	if runtime.GOOS == "windows" {
		return strings.ToLower(p)
	}
	return p
}
