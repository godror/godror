// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestChildFetchArraySize checks that statements opened from a parent
// (nested cursors, OUT ref cursors and implicit results) use the parent's
// FetchArraySize. Their define variables are sized to it, so a fetch with
// ODPI-C's default array size of 100 used to fail with DPI-1018.
func TestChildFetchArraySize(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	const fetchArraySize = 10
	const want = 3
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	countRows := func(t *testing.T, r driver.Rows) {
		t.Helper()
		defer r.Close()
		dest := make([]driver.Value, len(r.Columns()))
		var n int
		for {
			if err := r.Next(dest); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("Next: %+v", err)
			}
			n++
		}
		if n != want {
			t.Errorf("got %d rows, wanted %d", n, want)
		}
	}

	t.Run("nested", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx,
			"SELECT CURSOR(SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3) FROM DUAL",
			godror.FetchArraySize(fetchArraySize))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var r driver.Rows
			if err := rows.Scan(&r); err != nil {
				t.Fatal(err)
			}
			countRows(t, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("outRefCursor", func(t *testing.T) {
		var r driver.Rows
		if _, err := conn.ExecContext(ctx,
			"BEGIN OPEN :1 FOR SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3; END;",
			sql.Out{Dest: &r}, godror.FetchArraySize(fetchArraySize),
		); err != nil {
			t.Fatal(err)
		}
		countRows(t, r)
	})

	t.Run("implicitResult", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, `DECLARE
  c SYS_REFCURSOR;
BEGIN
  OPEN c FOR SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3;
  DBMS_SQL.RETURN_RESULT(c);
END;`, godror.FetchArraySize(fetchArraySize))
		if err != nil {
			if strings.Contains(err.Error(), "PLS-00302:") {
				t.Skip(err)
			}
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.NextResultSet() {
			t.Fatalf("no implicit result: %+v", rows.Err())
		}
		var n int
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("got %d rows, wanted %d", n, want)
		}
	})
}
