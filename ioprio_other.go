//go:build !linux

package main

// backgroundIO: reads at background priority on Linux only (ioprio_linux.go); elsewhere the reads keep their class.
func backgroundIO() (restore func()) { return func() {} }
