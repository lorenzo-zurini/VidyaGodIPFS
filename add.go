package main

// add.go — seed a local file into the node BY REFERENCE (filestore --nocopy equivalent).
//
// CID PARITY is mandatory (the node joins the public network with existing CIDs), so this must reproduce Kubo's
// `ipfs add --nocopy` importer settings exactly: fixed-size 256 KiB chunker, raw leaves, balanced layout, dag-pb v0
// root, sha2-256. Verified empirically against a recorded CID (the M1 parity gate).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	blockservice "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	datastore "github.com/ipfs/go-datastore"

	chunker "github.com/ipfs/boxo/chunker"
	files "github.com/ipfs/boxo/files"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Kubo's default add chunk size.
const chunkSize int64 = 262144

// buildFileNode imports a single regular file into dserv BY REFERENCE (filestore --nocopy: leaves reference the file
// in place) and returns its root node — Kubo `ipfs add --nocopy` parity (256 KiB chunker, raw leaves, dag-pb v0, sha2).
func buildFileNode(dserv ipld.DAGService, abs string, st os.FileInfo) (ipld.Node, error) {
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// A path-aware file so the DAG builder can record (AbsPath, Stat, Offset) for each leaf reference.
	rpf, err := files.NewReaderPathFile(abs, f, st)
	if err != nil {
		return nil, err
	}
	spl := chunker.NewSizeSplitter(rpf, chunkSize)

	dbp := uih.DagBuilderParams{
		Maxlinks:   uih.DefaultLinksPerBlock, // 174 — Kubo default
		RawLeaves:  true,                     // forced by --nocopy
		NoCopy:     true,
		CidBuilder: merkledag.V0CidPrefix(), // dag-pb v0 root; raw leaves carry the raw codec
		Dagserv:    dserv,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}
	return balanced.Layout(db)
}

// computeCid returns what a file's CID WOULD be, without touching the node's blockstore, filestore or pinset —
// the DAG is built into a throwaway in-memory store and discarded. Same importer settings as addNoCopy, so the
// answer is identical to what seeding would produce; it just has no side effects. This is the primitive behind
// `--cid`, for answering "do these bytes still match the CID we published?" without seeding anything.
func computeCid(path string) (cid.Cid, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return cid.Undef, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return cid.Undef, err
	}
	if st.IsDir() {
		return cid.Undef, fmt.Errorf("%s is a directory (use --publish-meta for a tree)", abs)
	}
	// The blocks are discarded as they are built: only the root is wanted, and the layout never reads a block back. An
	// in-memory store held the whole file — a multi-GB zip checked before seeding cost its size in RAM.
	mem := blockstore.NewBlockstore(datastore.NewNullDatastore())
	nd, err := buildFileNode(merkledag.NewDAGService(blockservice.New(mem, offline.Exchange(mem))), abs, st)
	if err != nil {
		return cid.Undef, err
	}
	return nd.Cid(), nil
}

// addNoCopy adds a single regular file by reference and pins it recursively. Returns the root CID.
func (n *node) addNoCopy(path string) (cid.Cid, error) {
	return n.addNoCopyIf(path, cid.Undef, false)
}

// addNoCopyIf adds a single regular file by reference — only when it is want (cid.Undef: whatever it is) — reading it
// ONCE, and returns the CID it is. The DAG is built into a scratch (scratchDag): each leaf kept as its reference (where
// in the file, how long — never its bytes), each interior node whole. Only a root that is want reaches the node: a file
// that is not what its node records is never referenced, pinned or announced. The pin is recorded, not walked
// (PinWithMode). Before, a seed read the file three times — computeCid to check it, the build, and the recursive Pin's
// FetchGraph reading every leaf back — so an 8.5 GB disc image cost ~25 GB of reads, and the app was closed before a
// start's seed pass ever got past it. background: its reads yield to everyone else's (backgroundIO).
//
// Every leaf is referenced HERE, over any reference it already had: these bytes were just hashed at these offsets, so
// the reference is proven, while one already held may name a file since deleted or changed in place (a leaf shared with
// another file: a run of zeros, a common header). The walking pin read such a leaf back and failed; a recorded pin over
// a kept stale reference would announce content nobody can be served. A leaf held as stored bytes keeps them.
func (n *node) addNoCopyIf(path string, want cid.Cid, background bool) (cid.Cid, error) {
	if background {
		defer backgroundIO()()
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return cid.Undef, &fileReadError{err}
	}
	st, err := os.Stat(abs)
	if err != nil {
		return cid.Undef, &fileReadError{err} // gone, or not ours to read: the file, not the store
	}
	if st.IsDir() {
		if want.Defined() {
			return cid.Undef, &fileReadError{fmt.Errorf("%s is a directory, not one file", abs)}
		}
		return n.addDirNoCopy(abs)
	}
	scratch := &scratchDag{}
	root, err := buildFileNode(scratch, abs, st)
	if err != nil {
		return cid.Undef, &fileReadError{err} // the file, not the store: nothing of it was kept
	}
	if want.Defined() && !root.Cid().Equals(want) {
		return root.Cid(), nil // not the content asked for: nothing of it was kept
	}
	var refs []*posinfo.FilestoreNode
	var stored []blocks.Block
	for _, b := range scratch.blocks {
		fn, isRef := b.(*posinfo.FilestoreNode)
		if !isRef {
			stored = append(stored, b) // interior nodes: small, stored whole
			continue
		}
		if held, err := n.plain.Has(n.ctx, fn.Cid()); err != nil {
			return cid.Undef, err
		} else if !held {
			refs = append(refs, fn)
		}
	}
	//A leaf another file is referenced for (a heal re-pointing a gone file's blocks, a shared run) is taken over: the
	//orphan probe looks again for a gone file it had confirmed. A leaf new to the store (a download) is not (the bloom
	//filter answers most of these without a read).
	for _, fn := range refs {
		if had, err := n.fstore.FileManager().Has(n.ctx, fn.Cid()); err == nil && had {
			n.refRepointed.Store(true)
			break
		}
	}
	if err := n.plain.PutMany(n.ctx, stored); err != nil {
		return cid.Undef, err
	}
	if err := n.fstore.FileManager().PutMany(n.ctx, refs); err != nil { // overwrites a held leaf's reference
		return cid.Undef, err
	}
	if err := n.pinner.PinWithMode(n.ctx, root.Cid(), ipfspinner.Recursive, abs); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announce(root.Cid()) // publish to the DHT now so it's discoverable without waiting for the 22h reprovide
	return root.Cid(), nil
}

// scratchDag keeps what a build adds, for the node to take whole or not at all: a leaf as its reference
// (refLeaf: CID and size; where it lies in the file), never its bytes — an 8.5 GB file is ~33 000 of them, a few MB —
// and an interior node whole (a few KiB each). Nothing is read back while a file is built. A block met twice (a file
// repeating a 256 KiB run: zeros, padding) is kept once, at its first place, as one Put at a time kept it.
type scratchDag struct {
	mu     sync.Mutex
	blocks []blocks.Block
	seen   map[cid.Cid]struct{}
}

func (s *scratchDag) Add(_ context.Context, nd ipld.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.seen[nd.Cid()]; dup {
		return nil
	}
	if s.seen == nil {
		s.seen = map[cid.Cid]struct{}{}
	}
	s.seen[nd.Cid()] = struct{}{}
	if fn, ok := nd.(*posinfo.FilestoreNode); ok {
		nd = &posinfo.FilestoreNode{Node: &refLeaf{c: fn.Cid(), size: len(fn.RawData())}, PosInfo: fn.PosInfo}
	}
	s.blocks = append(s.blocks, nd)
	return nil
}

func (s *scratchDag) AddMany(ctx context.Context, nds []ipld.Node) error {
	for _, nd := range nds {
		if err := s.Add(ctx, nd); err != nil {
			return err
		}
	}
	return nil
}

func (s *scratchDag) Get(context.Context, cid.Cid) (ipld.Node, error) { return nil, ipld.ErrNotFound{} }
func (s *scratchDag) GetMany(context.Context, []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption)
	close(ch)
	return ch
}
func (s *scratchDag) Remove(context.Context, cid.Cid) error       { return nil }
func (s *scratchDag) RemoveMany(context.Context, []cid.Cid) error { return nil }

// buildDirNode recursively assembles a UnixFS directory node for dir: every regular file is referenced in place
// (buildFileNode), subdirectories recurse, and the directory node is added to n.dserv. Symlinks/irregular entries are
// skipped. This is what makes a whole folder of dehydrated packages addressable as one folder CID.
func (n *node) buildDirNode(dir string) (ipld.Node, error) {
	d, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		info, ierr := e.Info()
		if ierr != nil {
			return nil, ierr
		}
		var child ipld.Node
		switch {
		case info.IsDir():
			child, err = n.buildDirNode(p)
		case info.Mode().IsRegular():
			child, err = buildFileNode(n.dserv, p, info)
		default:
			continue // skip symlinks / devices / etc.
		}
		if err != nil {
			return nil, err
		}
		if err := d.AddChild(n.ctx, e.Name(), child); err != nil {
			return nil, err
		}
	}
	dn, err := d.GetNode()
	if err != nil {
		return nil, err
	}
	if err := n.dserv.Add(n.ctx, dn); err != nil {
		return nil, err
	}
	return dn, nil
}

// addDirNoCopy recursively adds a directory tree by reference and pins the root. Returns the folder CID — the counterpart
// to fetchDirToPath, used to publish a folder of dehydrated packages as one add-by-CID source.
func (n *node) addDirNoCopy(path string) (cid.Cid, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return cid.Undef, err
	}
	root, err := n.buildDirNode(abs)
	if err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Pin(n.ctx, root, true, abs); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announce(root.Cid()) // publish to the DHT now so it's discoverable without waiting for the 22h reprovide
	return root.Cid(), nil
}

// buildMetaDirNode is buildDirNode restricted to a TEXT-ONLY "Meta" view of a package tree: only *.json manifests are
// referenced in place, the DEFPREFIX/USERDATA runtime subtrees are skipped whole, and any subdirectory left with no
// qualifying files is omitted entirely (returns a nil node). This lets a Meta-CID seed directly from the hydrated
// package dir — no JSON-only staging copy — while producing the SAME folder DAG (hence the same CID) a mirror would:
// os.ReadDir yields entries in sorted order, so the surviving *.json children are added in the identical order.
func (n *node) buildMetaDirNode(dir string) (ipld.Node, error) {
	d, err := ufsio.NewDirectory(n.dserv)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	children := 0
	for _, e := range entries {
		name := e.Name()
		p := filepath.Join(dir, name)
		info, ierr := e.Info()
		if ierr != nil {
			return nil, ierr
		}
		var child ipld.Node
		switch {
		case info.IsDir():
			if name == "DEFPREFIX" || name == "USERDATA" {
				continue // per-machine runtime state — never part of a reproducible Meta-CID
			}
			if child, err = n.buildMetaDirNode(p); err != nil {
				return nil, err
			}
			if child == nil {
				continue // subtree carried no JSON — omit the empty directory so the DAG matches a mirror
			}
		case info.Mode().IsRegular():
			if filepath.Ext(name) != ".json" {
				continue // manifests only: covers/zips/deltas travel as content CIDs referenced from the JSON
			}
			if child, err = buildFileNode(n.dserv, p, info); err != nil {
				return nil, err
			}
		default:
			continue // skip symlinks / devices / etc.
		}
		if err := d.AddChild(n.ctx, name, child); err != nil {
			return nil, err
		}
		children++
	}
	if children == 0 {
		return nil, nil // nothing text-only here
	}
	dn, err := d.GetNode()
	if err != nil {
		return nil, err
	}
	if err := n.dserv.Add(n.ctx, dn); err != nil {
		return nil, err
	}
	return dn, nil
}

// addMetaNoCopy publishes a TEXT-ONLY Meta-CID for a package dir (a single bundle OR a collection of bundle subdirs):
// the folder DAG references the *.json manifests IN PLACE, so the CID seeds straight from the package tree with no
// staging directory. Same CID as adding a JSON-only mirror of the same tree.
func (n *node) addMetaNoCopy(path string) (cid.Cid, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return cid.Undef, err
	}
	root, err := n.buildMetaDirNode(abs)
	if err != nil {
		return cid.Undef, err
	}
	if root == nil {
		return cid.Undef, fmt.Errorf("no JSON manifests under %s", abs)
	}
	// Re-point across a moved/removed staging dir: identical *.json content seeded earlier at a different path (e.g. an
	// old <Name>.meta mirror that has since been deleted) leaves stale filestore references, and Filestore.Put skips a
	// block it already has — so the just-built DAG still points at the gone path and the recursive Pin (or any later
	// serve) would read it. If the reference is stale, drop the closure and rebuild once so it re-points in place.
	// Mirrors addNoCopy's drop-and-retry (fetch.go dropRef).
	if n.cidMissing(root.Cid()) {
		n.dropRef(root.Cid())
		if root, err = n.buildMetaDirNode(abs); err != nil {
			return cid.Undef, err
		}
	}
	if err := n.pinner.Pin(n.ctx, root, true, abs); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announce(root.Cid()) // publish to the DHT now so it's discoverable without waiting for the 22h reprovide
	return root.Cid(), nil
}

// fileReadError: addNoCopyIf could not read the file it was to add (nothing was kept) — as against the node failing to
// keep what it read.
type fileReadError struct{ err error }

func (e *fileReadError) Error() string { return e.err.Error() }
func (e *fileReadError) Unwrap() error { return e.err }
