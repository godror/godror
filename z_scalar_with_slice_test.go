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

// TestExecManyScalarArg checks that a scalar bound next to slices
// (executeMany) is used for every row, not just the first.
func TestExecManyScalarArg(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_sws_many" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (b NUMBER, v NUMBER, s VARCHAR2(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := testDb.PrepareContext(ctx, "INSERT INTO "+tbl+" (b, v, s) VALUES (:1, :2, :3)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	// The second call reuses the vars of the first one.
	for _, tC := range []struct {
		b int64
		v []int64
		s string
	}{
		{b: 7, v: []int64{1, 2, 3}, s: "abc"},
		{b: 8, v: []int64{4, 5}, s: "de"},
	} {
		if _, err := stmt.ExecContext(ctx, tC.b, tC.v, tC.s); err != nil {
			t.Fatal(err)
		}
		for _, v := range tC.v {
			var b sql.NullInt64
			var s sql.NullString
			if err := testDb.QueryRowContext(ctx,
				"SELECT b, s FROM "+tbl+" WHERE v = :1", v,
			).Scan(&b, &s); err != nil {
				t.Fatal(err)
			}
			if !b.Valid || b.Int64 != tC.b || !s.Valid || s.String != tC.s {
				t.Errorf("v=%d: got b=%v s=%v, wanted b=%d s=%q", v, b, s, tC.b, tC.s)
			}
		}
	}
}
