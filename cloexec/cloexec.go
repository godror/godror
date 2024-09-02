// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"errors"
	"fmt"
	"sync"
)

var (
	mu sync.Mutex

	testLogf func(string, ...any)

	ErrNotImplemented = errors.New("not implemented")
)

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

// SetNetConnections sets the FD_CLOEXEC flag on all open
// (default "tcp") connections.
// Esp. useful for connections opened in C libraries.
func SetNetConnections(kind string) error {
	mu.Lock()
	defer mu.Unlock()
	connections, err := getConnections(kind)
	if err != nil {
		return err
	}
	var errs []error
	for _, fd := range connections {
		if isSet, err := getFd(uintptr(fd)); err != nil || isSet {
			continue
		}
		if err = setFd(uintptr(fd), true); err != nil {
			errs = append(errs, fmt.Errorf("%d: set FD_CLOEXEC: %w", fd, err))
		}
	}
	return errors.Join(errs...)
}
