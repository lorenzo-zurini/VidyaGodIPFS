package main

// add.go — seed a local file into the node BY REFERENCE (filestore --nocopy equivalent).
//
// CID PARITY is mandatory (the node joins the public network with existing CIDs), so this must reproduce Kubo's
// `ipfs add --nocopy` importer settings exactly: fixed-size 256 KiB chunker, raw leaves, balanced layout, dag-pb v0
// root, sha2-256. Verified empirically against a recorded CID (the M1 parity gate).

import (
	"fmt"
	"os"
	"path/filepath"

	chunker "github.com/ipfs/boxo/chunker"
	files "github.com/ipfs/boxo/files"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Kubo's default add chunk size.
const chunkSize int64 = 262144

// buildFileNode imports a single regular file into n.dserv BY REFERENCE (filestore --nocopy: leaves reference the file
// in place) and returns its root node — Kubo `ipfs add --nocopy` parity (256 KiB chunker, raw leaves, dag-pb v0, sha2).
func (n *node) buildFileNode(abs string, st os.FileInfo) (ipld.Node, error) {
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
		Dagserv:    n.dserv,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}
	return balanced.Layout(db)
}

// addNoCopy adds a single regular file by reference and pins it recursively. Returns the root CID.
func (n *node) addNoCopy(path string) (cid.Cid, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return cid.Undef, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return cid.Undef, err
	}
	if st.IsDir() {
		return n.addDirNoCopy(abs)
	}
	root, err := n.buildFileNode(abs, st)
	if err != nil {
		return cid.Undef, err
	}
	// Re-point across a MOVE: identical content already seeded at a different (now moved/deleted) backing path leaves a
	// stale filestore reference, and Filestore.Put skips a block it already has — so the leaves just "built" still point
	// at the OLD path and the recursive Pin (and any later serve) would read a gone file. If the reference is stale, drop
	// the closure and rebuild once so the leaves re-point at `abs`. Mirrors fetchToPath's drop-and-retry (fetch.go).
	if n.cidMissing(root.Cid()) {
		n.dropRef(root.Cid())
		if root, err = n.buildFileNode(abs, st); err != nil {
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
			child, err = n.buildFileNode(p, info)
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
			if child, err = n.buildFileNode(p, info); err != nil {
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
	if err := n.pinner.Pin(n.ctx, root, true, abs); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	n.announce(root.Cid()) // publish to the DHT now so it's discoverable without waiting for the 22h reprovide
	return root.Cid(), nil
}
