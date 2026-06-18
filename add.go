package main

// add.go — seed a local file into the node BY REFERENCE (filestore --nocopy equivalent).
//
// CID PARITY is mandatory (the node joins the public network with existing CIDs), so this must reproduce Kubo's
// `ipfs add --nocopy` importer settings exactly: fixed-size 256 KiB chunker, raw leaves, balanced layout, dag-pb v0
// root, sha2-256. Verified empirically against a recorded CID (the M1 parity gate).

import (
	"errors"
	"os"
	"path/filepath"

	chunker "github.com/ipfs/boxo/chunker"
	files "github.com/ipfs/boxo/files"
	merkledag "github.com/ipfs/boxo/ipld/merkledag"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	uih "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	cid "github.com/ipfs/go-cid"
)

// Kubo's default add chunk size.
const chunkSize int64 = 262144

// addNoCopy adds a single regular file by reference and pins it recursively. Returns the root CID.
// (Directory support — recursive add with Kubo-parity link ordering — lands in M2.)
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
		return cid.Undef, errors.New("directory add not implemented yet (M1: files only)")
	}

	f, err := os.Open(abs)
	if err != nil {
		return cid.Undef, err
	}
	defer f.Close()

	// A path-aware file so the DAG builder can record (AbsPath, Stat, Offset) for each leaf reference.
	rpf, err := files.NewReaderPathFile(abs, f, st)
	if err != nil {
		return cid.Undef, err
	}
	spl := chunker.NewSizeSplitter(rpf, chunkSize)

	dbp := uih.DagBuilderParams{
		Maxlinks:   uih.DefaultLinksPerBlock, // 174 — Kubo default
		RawLeaves:  true,                     // forced by --nocopy
		NoCopy:     true,
		CidBuilder: merkledag.V0CidPrefix(),  // dag-pb v0 root; raw leaves carry the raw codec
		Dagserv:    n.dserv,
	}
	db, err := dbp.New(spl)
	if err != nil {
		return cid.Undef, err
	}

	root, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	if err := n.pinner.Pin(n.ctx, root, true, abs); err != nil {
		return cid.Undef, err
	}
	if err := n.pinner.Flush(n.ctx); err != nil {
		return cid.Undef, err
	}
	return root.Cid(), nil
}
