//go:build !linux && !windows

package main

// overlaytun_other.go — stub so the package compiles on platforms with no TUN backend. Linux has a real /dev/net/tun
// impl (overlaytun_linux.go) and Windows has a real Wintun impl (overlaytun_windows.go); this covers everything else.

import "errors"

type tunLink struct{ name string }

func newTUN(string, int) (*tunLink, error) { return nil, errors.New("TUN overlay is Linux-only") }

func (t *tunLink) ReadPacket() ([]byte, error) { return nil, errors.New("unsupported") }
func (t *tunLink) WritePacket([]byte) error     { return errors.New("unsupported") }
func (t *tunLink) Close() error                 { return nil }
func (t *tunLink) configureIP(string) error     { return errors.New("unsupported") }

func (o *overlayService) serve(string) error { return errors.New("TUN overlay is Linux-only") }
