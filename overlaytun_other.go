//go:build !linux

package main

// overlaytun_other.go — stub so the package compiles off Linux. The embedded node ships only on Linux (the TUN
// overlay is a Linux feature), so these never run in production.

import "errors"

type tunLink struct{ name string }

func newTUN(string, int) (*tunLink, error) { return nil, errors.New("TUN overlay is Linux-only") }

func (t *tunLink) ReadPacket() ([]byte, error) { return nil, errors.New("unsupported") }
func (t *tunLink) WritePacket([]byte) error     { return errors.New("unsupported") }
func (t *tunLink) Close() error                 { return nil }
func (t *tunLink) configureIP(string) error     { return errors.New("unsupported") }

func (o *overlayService) serve(string) error { return errors.New("TUN overlay is Linux-only") }
