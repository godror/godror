// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"testing"
)

// TestStmtAddCleanupTwice checks that guarding the same statement twice
// (NextResultSet does that) does not crash: runtime.SetFinalizer
// throws if a finalizer is already set.
func TestStmtAddCleanupTwice(t *testing.T) {
	prev := guardWithFinalizers.Load()
	GuardWithFinalizers(true)
	defer GuardWithFinalizers(prev)
	st := &statement{}
	stmtAddCleanup(context.Background(), st, "first")
	stmtAddCleanup(context.Background(), st, "second")
}
