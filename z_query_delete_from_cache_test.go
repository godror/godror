// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestQueryDeleteFromCache checks that DeleteFromCache is honored for queries:
// the same text gets a new statement, so it works after the type
// of a column has changed in between.
func TestQueryDeleteFromCache(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl, view := "test_qdfc"+tblSuffix, "test_qdfc_v"+tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (a NUMBER(5), b VARCHAR2(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (1, 'one')"); err != nil {
		t.Fatal(err)
	}
	if _, err := testDb.ExecContext(ctx, "CREATE OR REPLACE VIEW "+view+" AS SELECT a, b FROM "+tbl); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP VIEW "+view)
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	qry := "SELECT * FROM " + view
	read := func() {
		t.Helper()
		rows, err := conn.QueryContext(ctx, qry, godror.DeleteFromCache())
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer rows.Close()
		var n int
		for rows.Next() {
			var a int
			var b string
			if err := rows.Scan(&a, &b); err != nil {
				t.Fatalf("scan: %+v", err)
			}
			if a != 1 || b != "one" {
				t.Errorf("got (%d, %q), wanted (1, \"one\")", a, b)
			}
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		if n != 1 {
			t.Errorf("got %d rows, wanted 1", n)
		}
	}
	read()
	// The same column becomes a CLOB: a cached statement is not usable for it.
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE VIEW "+view+" AS SELECT a, TO_CLOB(b) AS b FROM "+tbl); err != nil {
		t.Fatal(err)
	}
	read()
}
