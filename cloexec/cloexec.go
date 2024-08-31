// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v4/net"
	"golang.org/x/sys/unix"
)

var mu sync.Mutex

var testLogf func(string, ...any)

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
func getFd(fd uintptr) (bool, error) {
	rc, err := unix.FcntlInt(uintptr(fd), syscall.F_GETFD, 0) // arg is ignored
	if testLogf != nil {
		testLogf("getFd(%d): (%d,%+v)", fd, rc, err)
	}
	return rc&syscall.FD_CLOEXEC != 0, err
}

// SetNetConnections sets the FD_CLOEXEC flag on all open
// (default "tcp") connections.
// Esp. useful for connections opened in C libraries.
func SetNetConnections(kind string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	mu.Lock()
	defer mu.Unlock()
	connections, err := getConnections(ctx, kind)
	cancel()
	if err != nil {
		return err
	}
	var errs []error
	for _, c := range connections {
		if isSet, err := getFd(uintptr(c.Fd)); err != nil || isSet {
			continue
		}
		if err = setFd(uintptr(c.Fd), true); err != nil {
			errs = append(errs, fmt.Errorf("%d: set FD_CLOEXEC: %w", c.Fd, err))
		}
	}
	return errors.Join(errs...)
}

func getConnections(ctx context.Context, kind string) ([]net.ConnectionStat, error) {
	if kind == "" {
		kind = "tcp"
	}
	return net.ConnectionsPidWithoutUidsWithContext(ctx, kind, int32(os.Getpid()))
}
