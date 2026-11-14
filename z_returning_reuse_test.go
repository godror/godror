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

// TestReturningReuseAfterNoRows reuses a prepared RETURNING INTO statement
// after an execution that returned no rows.
func TestReturningReuseAfterNoRows(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_rr_ret" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER, a VARCHAR2(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, a) VALUES (1, 'one')"); err != nil {
		t.Fatal(err)
	}

	stmt, err := testDb.PrepareContext(ctx, "UPDATE "+tbl+" SET a=a WHERE id=:1 RETURNING a INTO :2")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i, tC := range []struct {
		id   int
		want string
	}{
		{-1, ""}, {1, "one"}, {-1, ""}, {1, "one"},
	} {
		var s string
		if _, err := stmt.ExecContext(ctx, tC.id, sql.Out{Dest: &s}); err != nil {
			t.Fatalf("%d. id=%d: %+v", i, tC.id, err)
		}
		if s != tC.want {
			t.Errorf("%d. id=%d: got %q, wanted %q", i, tC.id, s, tC.want)
		}
	}
}
