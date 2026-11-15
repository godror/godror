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

// A statement bound to a transaction (or to an sql.Conn) is one statement
// in the driver. If it is executed again while its previous rows are open,
// those rows must fail, not return the data of the new execution.
func TestStmtReexecutedWithOpenRows(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "SELECT :1 * 1000 + LEVEL FROM DUAL CONNECT BY LEVEL <= :2")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	const outerRows = 50
	outer, err := stmt.QueryContext(ctx, 1, outerRows, godror.FetchArraySize(10), godror.PrefetchCount(10))
	if err != nil {
		t.Fatal(err)
	}
	defer outer.Close()
	var n int
	for outer.Next() {
		var v int
		if err := outer.Scan(&v); err != nil {
			t.Fatal(err)
		}
		n++
		if v != 1000+n {
			t.Fatalf("outer row %d: got %d, wanted %d", n, v, 1000+n)
		}
		if n != 5 {
			continue
		}
		inner, err := stmt.QueryContext(ctx, 2, 30, godror.FetchArraySize(10), godror.PrefetchCount(10))
		if err != nil {
			t.Fatal(err)
		}
		var m int
		for inner.Next() {
			if err := inner.Scan(&v); err != nil {
				t.Fatal(err)
			}
			m++
			if v != 2000+m {
				t.Errorf("inner row %d: got %d, wanted %d", m, v, 2000+m)
			}
		}
		if err := inner.Close(); err != nil {
			t.Error(err)
		}
		if m != 30 {
			t.Errorf("inner: got %d rows, wanted 30", m)
		}
	}
	if err := outer.Err(); !errors.Is(err, godror.ErrStmtReexecuted) {
		t.Errorf("outer: got %d rows of %d and error %v, wanted ErrStmtReexecuted", n, outerRows, err)
	}
}

// TestStmtFailedCallKeepsOpenRows checks that a call that fails before
// the statement is executed leaves the rows of the previous execution usable.
func TestStmtFailedCallKeepsOpenRows(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "SELECT LEVEL FROM DUAL WHERE :1 IS NOT NULL CONNECT BY LEVEL <= 100")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, "x", godror.FetchArraySize(10), godror.PrefetchCount(10))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for n < 5 && rows.Next() {
		n++
	}
	// This fails in the bind, the statement is not executed.
	type unsupported struct{ A, B int }
	if rows2, err := stmt.QueryContext(ctx, unsupported{1, 2}); err == nil {
		rows2.Close()
		t.Fatal("the bind of an unsupported type succeeded")
	}
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Errorf("after %d rows: %+v", n, err)
	} else if n != 100 {
		t.Errorf("got %d rows, wanted 100", n)
	}
}
