// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// database/sql executes the statement again when the driver returns
// driver.ErrBadConn. That must happen only if the server has not executed it.
//
// The errors are simulated: the PL/SQL block does its work, commits,
// then raises the error of a lost connection itself.
func TestQueryRetryOnBadConn(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_qretry" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")
	count := func(id int) int {
		var n int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = :1", id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// ORA-03113: the statement may have been executed, so no retry.
	t.Run("executed", func(t *testing.T) {
		rows, err := testDb.QueryContext(ctx,
			"DECLARE lost EXCEPTION; PRAGMA EXCEPTION_INIT(lost, -3113); "+
				"BEGIN INSERT INTO "+tbl+" (id) VALUES (1); COMMIT; RAISE lost; END;")
		if err == nil {
			rows.Close()
			t.Fatal("wanted error")
		}
		if errors.Is(err, driver.ErrBadConn) {
			t.Errorf("got driver.ErrBadConn for a statement that was executed: %v", err)
		}
		var oe *godror.OraErr
		if !errors.As(err, &oe) || oe.Code() != 3113 {
			t.Errorf("got %+v, wanted ORA-03113", err)
		}
		if n := count(1); n != 1 {
			t.Errorf("one Query call executed the block %d times", n)
		}
	})

	// ORA-03114 (not connected): the statement has not been executed,
	// so the Query is retried, and succeeds on another connection.
	t.Run("notConnected", func(t *testing.T) {
		conn, err := testDb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var sessionID string
		err = conn.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'SESSIONID') FROM DUAL").Scan(&sessionID)
		// Back to the pool of database/sql: the next query gets this connection.
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := testDb.QueryContext(ctx,
			"DECLARE nc EXCEPTION; PRAGMA EXCEPTION_INIT(nc, -3114); "+
				"BEGIN INSERT INTO "+tbl+" (id) VALUES (2); COMMIT; "+
				"IF SYS_CONTEXT('USERENV', 'SESSIONID') = :1 THEN RAISE nc; END IF; END;",
			sessionID)
		if err != nil {
			t.Fatalf("the Query has not been retried: %+v", err)
		}
		rows.Close()
		// once on the "not connected" session, once on another
		if n := count(2); n != 2 {
			t.Errorf("the block has been executed %d times, wanted 2", n)
		}
	})
}
