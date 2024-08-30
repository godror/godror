// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/shirou/gopsutil/v4/net"
	"golang.org/x/sys/unix"
)

// SetFd sets the FC_CLOEXEC flag on the given file descriptor.
func SetFd(fd uintptr) error {
	_, err := unix.FcntlInt(uintptr(fd), syscall.F_SETFD, syscall.FD_CLOEXEC)
	return err
}

// ClearFd clears the FC_CLOEXEC flag on the given file descriptor.
func ClearFd(fd uintptr) error {
	_, err := unix.FcntlInt(uintptr(fd), syscall.F_SETFD, ^syscall.FD_CLOEXEC)
	return err
}

// SetNetConnectiosn sets the FC_CLOEXEC flag on all open (default "tcp")
// connections. Esp. useful for connections opened in C libraries.
func SetNetConnections(typ string) error {
	if typ == "" {
		typ = "tcp"
	}
	connections, err := net.Connections(typ)
	if err != nil {
		return err
	}
	var errs []error
	for _, c := range connections {
		if err = SetFd(uintptr(c.Fd)); err != nil {
			errs = append(errs, fmt.Errorf("%d: set FD_CLOEXEC: %w", c.Fd, err))
		}
	}
	return errors.Join(errs...)
}
