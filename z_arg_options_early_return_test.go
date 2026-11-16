// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOptionsOfNotExecutedCall checks that the Options given to an execution
// of a prepared statement that did not start (the context is already done,
// wrong number of arguments) do not apply to its next execution.
func TestOptionsOfNotExecutedCall(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_opt_notexec" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6) PRIMARY KEY, b VARCHAR2(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	// database/sql does not check the context for a statement of a Conn.
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done, doneCancel := context.WithCancel(ctx)
	doneCancel()
	prepare := func(t *testing.T, qry string) *sql.Stmt {
		t.Helper()
		st, err := conn.PrepareContext(ctx, qry)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	ins := "INSERT INTO " + tbl + " (id, b) VALUES (:1, :2)"

	t.Run("ParseOnly, context done", func(t *testing.T) {
		st := prepare(t, ins)
		if _, err := st.ExecContext(done, godror.ParseOnly(), 1, "x"); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, wanted context.Canceled", err)
		}
		if _, err := st.ExecContext(ctx, 2, "x"); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = 2").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("the Exec without Options inserted %d rows: it was only parsed", n)
		}
	})

	t.Run("BoolToString, wrong number of arguments", func(t *testing.T) {
		st := prepare(t, ins)
		if _, err := st.ExecContext(ctx, godror.BoolToString("Y", "N"), 3); err == nil {
			t.Fatal("wanted an error for 1 argument of 2")
		}
		if _, err := st.ExecContext(ctx, 4, true); err != nil {
			t.Fatal(err)
		}
		var b string
		if err := conn.QueryRowContext(ctx, "SELECT b FROM "+tbl+" WHERE id = 4").Scan(&b); err != nil {
			t.Fatal(err)
		}
		if b == "Y" {
			t.Errorf("the Exec without Options stored true as %q", b)
		}
	})

	t.Run("ParseOnly, wrong number of arguments, then no arguments", func(t *testing.T) {
		st := prepare(t, "INSERT INTO "+tbl+" (id) VALUES (5)")
		if _, err := st.ExecContext(ctx, godror.PartialBatch(), godror.ParseOnly(), 5); err == nil {
			t.Fatal("wanted an error for 1 argument of 0")
		}
		if _, err := st.ExecContext(ctx); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = 5").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("the Exec without arguments inserted %d rows: it was only parsed", n)
		}
	})

	t.Run("Query NumberAsString, context done", func(t *testing.T) {
		st := prepare(t, "SELECT 1.5 FROM DUAL")
		if rows, err := st.QueryContext(done, godror.NumberAsString()); !errors.Is(err, context.Canceled) {
			if err == nil {
				rows.Close()
			}
			t.Fatalf("got %v, wanted context.Canceled", err)
		}
		var v any
		if err := st.QueryRowContext(ctx).Scan(&v); err != nil {
			t.Fatal(err)
		}
		if _, ok := v.(godror.Number); !ok {
			t.Errorf("the Query without Options returned %T, wanted godror.Number", v)
		}
	})
}
