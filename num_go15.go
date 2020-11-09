// +build go1.15

// Copyright 2020 The Godror Authors
// Copyright 2016 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "math/big"

func fillBytes(x *big.Int, buf []byte) []byte {
	return x.FillBytes(buf)
}
