// +build !go1.15

// Copyright 2020 The Godror Authors
// Copyright 2016 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "math/big"

func fillBytes(x *big.Int, buf []byte) []byte {
	// Clear whole buffer. (This gets optimized into a memclr.)
	for i := range buf {
		buf[i] = 0
	}
	copy(buf, x.Bytes())
	return buf
}
