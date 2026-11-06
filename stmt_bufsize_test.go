// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "testing"

func TestBindBufSize(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, 32}, {1, 32}, {32, 32},
		{33, 128}, {128, 128},
		{129, 2000}, {2000, 2000},
		{2001, 4000}, {4000, 4000},
		{4001, 4096}, {4097, 8192}, {8192, 8192}, {8193, 16384},
		{16385, 32767}, {32767, 32767},
		{32768, 32768}, {100000, 100000},
	} {
		if got := bindBufSize(tc.in); got != tc.want {
			t.Errorf("bindBufSize(%d)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestVarInfoCanHold(t *testing.T) {
	prev := varInfo{SliceLen: 10, BufSize: 128}
	for _, tc := range []struct {
		name string
		next varInfo
		want bool
	}{
		{"same", varInfo{SliceLen: 10, BufSize: 128}, true},
		{"smaller buffer", varInfo{SliceLen: 10, BufSize: 32}, true},
		{"larger buffer", varInfo{SliceLen: 10, BufSize: 2000}, false},
		{"fewer rows in", varInfo{SliceLen: 3, BufSize: 128}, true},
		{"fewer rows out", varInfo{SliceLen: 3, BufSize: 128, IsOut: true}, false},
		{"same rows out", varInfo{SliceLen: 10, BufSize: 128, IsOut: true}, true},
		{"more rows", varInfo{SliceLen: 11, BufSize: 128}, false},
		{"plsql array", varInfo{SliceLen: 10, BufSize: 128, IsPLSArray: true}, false},
	} {
		if got := prev.canHold(tc.next); got != tc.want {
			t.Errorf("%s: canHold=%t, want %t", tc.name, got, tc.want)
		}
	}
}
