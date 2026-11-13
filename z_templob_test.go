// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestTempLobClose checks that closing a temporary LOB that was never
// written to releases it (DirectLob.Close used to do nothing unless
// WriteAt had opened it, leaking the temporary LOB).
func TestTempLobClose(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const qry = "SELECT cache_lobs + nocache_lobs FROM v$temporary_lobs WHERE sid = SYS_CONTEXT('USERENV', 'SID')"
	count := func() int {
		var n int
		if err := conn.QueryRowContext(ctx, qry).Scan(&n); err != nil {
			t.Skipf("%s: %v", qry, err)
		}
		return n
	}
	before := count()
	type tempLobber interface {
		NewTempLob(bool) (*godror.DirectLob, error)
	}
	const N = 20
	if err = conn.Raw(func(dc any) error {
		tl, ok := dc.(tempLobber)
		if !ok {
			t.Skipf("%T has no NewTempLob", dc)
		}
		for i := 0; i < N; i++ {
			lob, err := tl.NewTempLob(i%2 == 0)
			if err != nil {
				return err
			}
			if err = lob.Close(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if after := count(); after > before {
		t.Errorf("%d temporary LOBs left open after closing %d (before: %d)", after-before, N, before)
	}
}
