//go:build !linux

package main

// cAllocator: not measured off glibc.
func cAllocator() (inUse, free uint64) { return 0, 0 }
