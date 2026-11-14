// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"
)

// TestExecEmptySlice checks that an Exec whose slice binds are all empty
// executes nothing, instead of running once with an unset element.
func TestExecEmptySlice(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_es_exec" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	count := func(t *testing.T) int {
		t.Helper()
		var n int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	qry := "INSERT INTO " + tbl + " (id) VALUES (:1)"

	t.Run("direct", func(t *testing.T) {
		res, err := testDb.ExecContext(ctx, qry, []int64{})
		if err != nil {
			t.Fatal(err)
		}
		if ra, err := res.RowsAffected(); err != nil {
			t.Fatal(err)
		} else if ra != 0 {
			t.Errorf("RowsAffected=%d, wanted 0", ra)
		}
		if n := count(t); n != 0 {
			t.Errorf("got %d rows, wanted 0", n)
		}
	})

	t.Run("prepared", func(t *testing.T) {
		if _, err := testDb.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatal(err)
		}
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		if _, err := stmt.ExecContext(ctx, []int64{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		res, err := stmt.ExecContext(ctx, []int64{})
		if err != nil {
			t.Fatal(err)
		}
		if ra, err := res.RowsAffected(); err != nil {
			t.Fatal(err)
		} else if ra != 0 {
			t.Errorf("RowsAffected=%d, wanted 0", ra)
		}
		if n := count(t); n != 3 {
			t.Errorf("got %d rows, wanted 3", n)
		}
	})

	// An empty out slice is not an empty batch: RETURNING INTO fills it.
	t.Run("returning", func(t *testing.T) {
		var got []int64
		if _, err := testDb.ExecContext(ctx,
			"UPDATE "+tbl+" SET id=id WHERE id > :1 RETURNING id INTO :2",
			1, sql.Out{Dest: &got},
		); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Errorf("got %v, wanted 2 elements", got)
		}
	})
}
