// +build !windows

// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#cgo LDFLAGS: -ldl -lpthread

// https://www.win.tue.nl/~aeb/linux/misc/gcc-semibug.html
__asm__(".symver memcpy,memcpy@GLIBC_2.2.5");
*/
import "C"
