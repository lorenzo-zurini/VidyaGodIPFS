package main

// resume.go — torrent-like resumable fetch state. A large file is fetched leaf-by-leaf into a preallocated `dest.tmp`;
// a `dest.part` sidecar records which leaves are durably on disk (1 bit per leaf) so a stalled/dropped/killed fetch can
// resume by re-requesting ONLY the missing leaves instead of restarting from zero.

import (
	"encoding/binary"
	"errors"
	"os"
)

// errIncomplete: the fetch stalled or the session dropped before every leaf arrived, but the partial (dest.tmp +
// dest.part) was KEPT. The wrapper backs off and re-enters, resuming only the missing leaves. Distinct from
// errMissingFiles (a stale LOCAL filestore reference whose backing file is gone).
var errIncomplete = errors.New("incomplete")

const partMagic = "VGPT" // VidyaGod ParT — resume sidecar identifier

// partBits is per-leaf resume state plus the identity it belongs to, so a stale/foreign sidecar is rejected rather than
// corrupting a fresh fetch of different content.
type partBits struct {
	root  string // root CID string this partial belongs to
	total int64  // full file size (== preallocated tmp size)
	count int    // number of leaves
	nset  int    // leaves marked done (kept in sync with bits)
	bits  []byte // ceil(count/8) bytes, bit i = leaf i durable on disk
}

func newPartBits(root string, total int64, count int) *partBits {
	return &partBits{root: root, total: total, count: count, bits: make([]byte, (count+7)/8)}
}
func (p *partBits) get(i int) bool { return p.bits[i>>3]&(1<<uint(i&7)) != 0 }
func (p *partBits) set(i int) {
	if p.bits[i>>3]&(1<<uint(i&7)) == 0 {
		p.bits[i>>3] |= 1 << uint(i&7)
		p.nset++
	}
}
func (p *partBits) allSet() bool { return p.nset == p.count }

func partPath(dest string) string { return dest + ".part" }
func tmpPath(dest string) string  { return dest + ".tmp" }

// removePartial discards a partial fetch (its tmp + part) — an explicit cancel or a completed finalize.
func removePartial(dest string) {
	_ = os.Remove(tmpPath(dest))
	_ = os.Remove(partPath(dest))
}

// loadPart reads dest.part and returns it ONLY if it matches (root,total,count); a missing/short/corrupt/foreign
// sidecar returns (nil,false) so the caller starts fresh. Layout: magic(4) rootLen(u16) root total(i64) count(i64) bitmap.
func loadPart(dest, root string, total int64, count int) (*partBits, bool) {
	b, err := os.ReadFile(partPath(dest))
	if err != nil || len(b) < 6 || string(b[:4]) != partMagic {
		return nil, false
	}
	o := 4
	rl := int(binary.LittleEndian.Uint16(b[o:]))
	o += 2
	if len(b) < o+rl+16 {
		return nil, false
	}
	r := string(b[o : o+rl])
	o += rl
	t := int64(binary.LittleEndian.Uint64(b[o:]))
	o += 8
	c := int(binary.LittleEndian.Uint64(b[o:]))
	o += 8
	nb := (count + 7) / 8
	if r != root || t != total || c != count || len(b) < o+nb {
		return nil, false
	}
	p := newPartBits(root, total, count)
	copy(p.bits, b[o:o+nb])
	for i := 0; i < count; i++ {
		if p.get(i) {
			p.nset++
		}
	}
	return p, true
}

// savePart writes dest.part atomically (sibling + rename). The caller MUST fsync dest.tmp first so the bitmap never
// claims a leaf whose bytes aren't durable.
func savePart(dest string, p *partBits) error {
	buf := make([]byte, 0, 4+2+len(p.root)+16+len(p.bits))
	buf = append(buf, partMagic...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(p.root)))
	buf = append(buf, p.root...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(p.total))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(p.count))
	buf = append(buf, p.bits...)
	sib := partPath(dest) + ".w"
	if err := os.WriteFile(sib, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(sib, partPath(dest))
}
