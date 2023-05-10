//go:build go1.20

// Copyright 2023 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "strings"

func stringsCutPrefix(s, prefix string) (after string, found bool) {
	return strings.CutPrefix(s, prefix)
}
func stringsCut(s, prefix string) (before, after string, found bool) {
	return strings.Cut(s, prefix)
}
