// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestVectorNullOut reads a NULL VECTOR through an OUT bind and through
// RETURNING INTO: it must be the same empty Vector that a query returns.
func TestVectorNullOut(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_vector_null_out" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), c VECTOR(3, FLOAT32))"); err != nil {
		t.Skipf("VECTOR needs Oracle 23ai: %+v", err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+tbl+" (id, c) VALUES (1, NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO "+tbl+" (id, c) VALUES (2, :1)", godror.Vector{Values: []float32{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ name, qry string }{
		{"query", ""},
		{"out", "BEGIN SELECT c INTO :v FROM " + tbl + " WHERE id = :id; END;"},
		{"returning", "UPDATE " + tbl + " SET id = id WHERE id = :id RETURNING c INTO :v"},
	} {
		// Read the NULL (1), a value (2) and the NULL again into the same
		// destination with the same prepared statement.
		got := godror.Vector{Values: []float32{9, 9, 9}}
		var stmt *sql.Stmt
		if tc.qry != "" {
			if stmt, err = conn.PrepareContext(ctx, tc.qry); err != nil {
				t.Fatalf("%s: %+v", tc.name, err)
			}
			defer stmt.Close()
		}
		for _, id := range []int{1, 2, 1} {
			if stmt == nil {
				err = conn.QueryRowContext(ctx, "SELECT c FROM "+tbl+" WHERE id = :1", id).Scan(&got)
			} else {
				_, err = stmt.ExecContext(ctx, sql.Named("v", sql.Out{Dest: &got}), sql.Named("id", id))
			}
			if err != nil {
				t.Errorf("%s id=%d: %+v", tc.name, id, err)
				continue
			}
			vals, _ := got.Values.([]float32)
			if id == 1 && (got.Values != nil || got.Dimensions != 0) {
				t.Errorf("%s: NULL vector: got %#v, wanted the zero Vector", tc.name, got)
			} else if id == 2 && len(vals) != 3 {
				t.Errorf("%s: got %#v, wanted 3 values", tc.name, got)
			}
		}
	}
}
