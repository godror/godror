// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOutRefCursorPreparedReexec executes a prepared statement with an OUT
// REF CURSOR several times: each execution must return its own cursor.
func TestOutRefCursorPreparedReexec(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// read reads at most max rows (all if max < 0), and returns the columns and the rows.
	read := func(rc driver.Rows, max int) (string, error) {
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s:", strings.Join(rc.Columns(), ","))
		dest := make([]driver.Value, len(rc.Columns()))
		for i := 0; max < 0 || i < max; i++ {
			if err := rc.Next(dest); err == io.EOF {
				break
			} else if err != nil {
				return sb.String(), err
			}
			fmt.Fprintf(&sb, "%v", dest)
		}
		return sb.String(), nil
	}

	stmt, err := conn.PrepareContext(ctx, `BEGIN
  IF :k < 100 THEN
    OPEN :rc FOR SELECT level + :k AS a FROM DUAL CONNECT BY level <= 3;
  ELSE
    OPEN :rc FOR SELECT 'x'||level AS b, level + :k AS c FROM DUAL CONNECT BY level <= 2;
  END IF;
END;`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	exec := func(k int) driver.Rows {
		t.Helper()
		var rc driver.Rows
		if _, err := stmt.ExecContext(ctx, sql.Named("k", k), sql.Named("rc", sql.Out{Dest: &rc})); err != nil {
			t.Fatalf("k=%d: %v", k, err)
		}
		return rc
	}
	want := func(k int) string {
		if k < 100 {
			return fmt.Sprintf("A:[%d][%d][%d]", k+1, k+2, k+3)
		}
		return fmt.Sprintf("B,C:[x1 %d][x2 %d]", k+1, k+2)
	}
	check := func(what string, k int, rc driver.Rows) {
		t.Helper()
		got, err := read(rc, -1)
		if err != nil {
			t.Errorf("%s k=%d: %v", what, k, err)
		} else if got != want(k) {
			t.Errorf("%s k=%d: got %q, wanted %q", what, k, got, want(k))
		}
		if err := rc.Close(); err != nil {
			t.Errorf("%s k=%d: close: %v", what, k, err)
		}
	}

	// Each cursor is read and closed before the next execution;
	// the cursors of k >= 100 have other columns.
	for _, k := range []int{0, 10, 20, 100, 30, 200} {
		check("loop", k, exec(k))
	}

	// Several cursors are open at the same time.
	ks := []int{40, 50, 300, 60}
	held := make([]driver.Rows, len(ks))
	for i, k := range ks {
		held[i] = exec(k)
	}
	for i, k := range ks {
		check("held", k, held[i])
	}

	// The cursors must not accumulate, whether they are unread, read fully
	// or read in part (open_cursors is 300 by default).
	for i := 0; i < 500; i++ {
		rc := exec(i % 90)
		if i%3 != 0 {
			if _, err := read(rc, 2*(i%3)-3); err != nil {
				t.Fatalf("%d: %v", i, err)
			}
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
	}
	check("last", 70, exec(70))
}

// TestOutRefCursorOpenError checks that when the rows of an OUT REF CURSOR
// cannot be opened (here the fetch array is too big), the statement of the
// bind variable is not released: without the fix this is a use after free,
// which -asan reports.
func TestOutRefCursorOpenError(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const qry = "BEGIN OPEN :1 FOR SELECT level AS a FROM DUAL CONNECT BY level <= 3; END;"
	for i := 0; i < 3; i++ {
		var rc driver.Rows
		if _, err := conn.ExecContext(ctx, qry, sql.Out{Dest: &rc}, godror.FetchArraySize(1<<30)); err == nil {
			rc.Close()
			t.Error("wanted an error for a too big fetch array size")
		}
	}
	var rc driver.Rows
	if _, err := conn.ExecContext(ctx, qry, sql.Out{Dest: &rc}); err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	dest := make([]driver.Value, 1)
	if err := rc.Next(dest); err != nil {
		t.Error(err)
	}
}
