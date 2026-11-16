// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestPartialBatchCommit checks that outside of a transaction the rows that a
// PartialBatch reports as Affected are committed, as the rows of any other
// successful Exec are.
func TestPartialBatchCommit(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_pbc_batch" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// committed returns the number of rows another session sees.
	committed := func() int {
		t.Helper()
		var n int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wantBatchErrors := func(err error, affected int) {
		t.Helper()
		var be *godror.BatchErrors
		if !errors.As(err, &be) {
			t.Fatalf("got %v, wanted BatchErrors", err)
		}
		if len(be.Affected) != affected {
			t.Fatalf("got %v as affected, wanted %d rows", be.Affected, affected)
		}
	}

	qry := "INSERT INTO " + tbl + " (id) VALUES (:1)"
	if _, err := conn.ExecContext(ctx, qry, 3); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, qry, godror.PartialBatch(), []int{30, 31, 3, 32})
	wantBatchErrors(err, 3)
	if n := committed(); n != 4 {
		t.Errorf("after the PartialBatch another session sees %d rows, wanted 4 (3, 30, 31, 32)", n)
	}

	// A failed batch is rolled back: that must not take the rows of the
	// PartialBatch with it.
	var oraErr *godror.OraErr
	if _, err = conn.ExecContext(ctx, qry, []int{40, 3, 41}); !errors.As(err, &oraErr) || oraErr.Code() != 1 {
		t.Fatalf("got %v, wanted ORA-00001", err)
	}
	if _, err := conn.ExecContext(ctx, qry, 50); err != nil {
		t.Fatal(err)
	}
	if n := committed(); n != 5 {
		t.Errorf("after a failed batch and a row %d rows are left, wanted 5 (3, 30, 31, 32, 50)", n)
	}

	// In a transaction the caller decides.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, qry, godror.PartialBatch(), []int{60, 3, 61})
	wantBatchErrors(err, 2)
	if n := committed(); n != 5 {
		t.Errorf("another session sees %d rows before the end of the transaction, wanted 5", n)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := committed(); n != 5 {
		t.Errorf("%d rows after the rollback, wanted 5", n)
	}
}
