// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestExecNilBytes checks that a nil []byte argument is bound as NULL
// and the statement is executed, not skipped as an empty batch.
func TestExecNilBytes(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_nil_bytes" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), r RAW(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	var nilBytes []byte
	for i, b := range [][]byte{{1, 2}, {}, nil} {
		res, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, r) VALUES (:1, :2)", i, b)
		if err != nil {
			t.Fatalf("%d. %#v: %v", i, b, err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("%d. %#v: RowsAffected=%d, wanted 1", i, b, n)
		}
	}
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, r) VALUES (3, :1)", &nilBytes); err != nil {
		t.Errorf("*[]byte: %v", err)
	}
	if _, err := testDb.ExecContext(ctx, "BEGIN INSERT INTO "+tbl+" (id, r) VALUES (4, :1); END;", nilBytes); err != nil {
		t.Errorf("PL/SQL: %v", err)
	}
	if _, err := testDb.ExecContext(ctx,
		"BEGIN INSERT INTO "+tbl+" (id, r) VALUES (5, :1); :1 := HEXTORAW('0102'); END;",
		sql.Out{Dest: &nilBytes, In: true},
	); err != nil {
		t.Errorf("IN OUT: %v", err)
	} else if len(nilBytes) != 2 {
		t.Errorf("IN OUT: got %x, wanted 0102", nilBytes)
	}

	rows, err := testDb.QueryContext(ctx, "SELECT id, r FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		var r []byte
		if err := rows.Scan(&id, &r); err != nil {
			t.Fatal(err)
		}
		if id > 0 && len(r) != 0 {
			t.Errorf("id %d: got %x, wanted NULL", id, r)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 6 {
		t.Errorf("got rows %v, wanted ids 0-5", ids)
	}
}
