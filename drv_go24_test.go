//go:build cgo && go1.24

// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"os"
	"strings"
	"testing"
)

func TestTokenCallbackCreateEntrypointsAllowCallbacks(t *testing.T) {
	source, err := os.ReadFile("drv_go24.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, entrypoint := range []string{"dpiConn_create", "dpiPool_create"} {
		if strings.Contains(string(source), "#cgo nocallback "+entrypoint) {
			t.Errorf("%s must allow callbacks for TokenCB", entrypoint)
		}
	}
}
