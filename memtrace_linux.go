//go:build linux

package main

// #include <malloc.h>
import "C"

// cAllocator: glibc's bytes in use and free-but-kept (mallinfo2).
func cAllocator() (inUse, free uint64) {
	mi := C.mallinfo2()
	return uint64(mi.uordblks) + uint64(mi.hblkhd), uint64(mi.fordblks)
}
