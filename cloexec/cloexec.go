// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/shirou/gopsutil/v4/net"
	"golang.org/x/sys/unix"
)

var mu sync.Mutex

// SetFd sets the FC_CLOEXEC flag on the given file descriptor.
func SetFd(fd uintptr) error {
	mu.Lock()
	defer mu.Unlock()
	return setFd(fd, true)
}

// ClearFd clears the FC_CLOEXEC flag on the given file descriptor.
func ClearFd(fd uintptr) error {
	mu.Lock()
	defer mu.Unlock()
	return setFd(fd, false)
}

func setFd(fd uintptr, set bool) error {
	mask := syscall.FD_CLOEXEC
	if !set {
		mask = ^mask
	}
	_, err := unix.FcntlInt(uintptr(fd), syscall.F_SETFD, mask)
	return err
}

// SetNetConnectiosn sets the FD_CLOEXEC flag on all open
// (default "tcp") connections.
// Esp. useful for connections opened in C libraries.
func SetNetConnections(typ string) error {
	if typ == "" {
		typ = "tcp"
	}
	mu.Lock()
	defer mu.Unlock()
	connections, err := net.Connections(typ)
	if err != nil {
		return err
	}
	var errs []error
	for _, c := range connections {
		if err = setFd(uintptr(c.Fd), true); err != nil {
			errs = append(errs, fmt.Errorf("%d: set FD_CLOEXEC: %w", c.Fd, err))
		}
	}
	return errors.Join(errs...)
}
