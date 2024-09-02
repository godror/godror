//go:build windows

// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func setFd(fd uintptr, set bool) error {
	if !set {
		return ErrNotImplemented
	}
	syscall.CloseOnExec(syscall.Handle(fd))
	return nil
}

var (
	modkernel32              = windows.NewLazySystemDLL("kernel32.dll")
	procGetHandleInformation = modkernel32.NewProc("GetHandleInformation")
)

func getFd(fd uintptr) (bool, error) {
	var flags uint32
	r1, _, e1 := syscall.SyscallN(procGetHandleInformation.Addr(), uintptr(fd), uintptr(unsafe.Pointer(&flags)))
	var err error
	if r1 == 0 {
		err = errnoErr(e1)
	}
	return flags&windows.HANDLE_FLAG_INHERIT != 0, err
}

func getConnections(kind string) ([]uint32, error) {
	return nil, ErrNotImplemented
}

//
// Copied from https://cs.opensource.google/go/x/sys/+/refs/tags/v0.24.0:windows/zsyscall_windows.go;l=23
//

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return errERROR_EINVAL
	case errnoERROR_IO_PENDING:
		return errERROR_IO_PENDING
	}
	// TODO: add more here, after collecting data on the common
	// error values see on Windows. (perhaps when running
	// all.bat?)
	return e
}

// Do the interface allocations only once for common
// Errno values.
const (
	errnoERROR_IO_PENDING = 997
)

var (
	errERROR_IO_PENDING error = syscall.Errno(errnoERROR_IO_PENDING)
	errERROR_EINVAL     error = syscall.EINVAL
)
