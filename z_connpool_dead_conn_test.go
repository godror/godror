// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A connection whose session is lost must not be put back into the ConnPool.
func TestConnPoolDeadConn(t *testing.T) {
	ensureSystemDB(t)
	sysCtx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	// The session to kill is found by its module, which no other session has.
	module := fmt.Sprintf("connpool-dead-%d", time.Now().UnixNano())
	ctx := godror.ContextWithTraceTag(sysCtx, godror.TraceTag{Module: module})

	pool := godror.NewConnPool(testDb, 1)
	defer pool.Close()
	use := func() error {
		c, err := pool.Conn(ctx)
		if err != nil {
			return err
		}
		defer c.Close()
		var n int
		return c.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&n)
	}
	if err := use(); err != nil {
		t.Fatal(err)
	}

	var sid, serial int64
	if err := testSystemDb.QueryRowContext(sysCtx,
		"SELECT sid, serial# FROM v$session WHERE module = :1", module,
	).Scan(&sid, &serial); err != nil {
		t.Skip("find the session:", err)
	}
	if _, err := testSystemDb.ExecContext(sysCtx,
		fmt.Sprintf("ALTER SYSTEM KILL SESSION '%d,%d' IMMEDIATE", sid, serial),
	); err != nil {
		t.Skip("kill the session:", err)
	}

	// The first use may find out that the session is gone.
	err := use()
	t.Log("first use after the kill:", err)
	for i := range 3 {
		if err := use(); err != nil {
			t.Errorf("%d. the ConnPool gave a dead connection again: %v", i+2, err)
		}
	}
}
