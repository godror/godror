// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Nested cursors that are scanned but not closed must not stay open
// till the parent rows are closed: they are closed at the next row.
func TestNestedCursorLeak(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	// A DB of its own, with fresh sessions: the cursors that other tests
	// left open on the sessions of the shared pool must not count here.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	db := sql.OpenDB(godror.NewDriver().NewConnector(P))
	defer db.Close()
	// More rows than OPEN_CURSORS (300 by default).
	const openCursors = 1000
	const qry = "SELECT CURSOR(SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3) FROM DUAL CONNECT BY LEVEL <= :1"
	readAll := func(dr driver.Rows) (n int, err error) {
		dest := make([]driver.Value, 1)
		for ; err == nil; n++ {
			err = dr.Next(dest)
		}
		return n - 1, err
	}
	for _, fetchArraySize := range []int{godror.DefaultFetchArraySize, 1} {
		func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			// Scanned, neither read nor closed.
			rows, err := conn.QueryContext(ctx, qry, openCursors+100, godror.FetchArraySize(fetchArraySize))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var n int
			var first sql.Rows
			for rows.Next() {
				sub := new(sql.Rows)
				if n == 0 {
					sub = &first
				}
				if err = rows.Scan(sub); err != nil {
					t.Fatal(err)
				}
				if n++; n == 2 && (first.Next() || !errors.Is(first.Err(), godror.ErrCursorClosed)) {
					t.Errorf("fetchArraySize=%d: the cursor of the first row: got %v, wanted ErrCursorClosed", fetchArraySize, first.Err())
				}
			}
			if err = rows.Err(); err != nil || n != openCursors+100 {
				t.Errorf("fetchArraySize=%d: got %d rows of %d: %v", fetchArraySize, n, openCursors+100, err)
			}
			rows.Close()

			// The cursor of the current row is usable, that of the previous is closed.
			if rows, err = conn.QueryContext(ctx, qry, 5, godror.FetchArraySize(fetchArraySize)); err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var prev driver.Rows
			for i := 0; rows.Next(); i++ {
				var dr driver.Rows
				if err = rows.Scan(&dr); err != nil {
					t.Fatal(err)
				}
				// Read every second cursor in its row, the others late.
				if i%2 == 0 {
					if n, err := readAll(dr); n != 3 || err != io.EOF {
						t.Errorf("fetchArraySize=%d: current cursor: got %d rows and %v, wanted 3 and EOF", fetchArraySize, n, err)
					}
				} else if _, err := readAll(prev); err != io.EOF {
					t.Errorf("fetchArraySize=%d: previous cursor, read before: got %v, wanted EOF", fetchArraySize, err)
				}
				if i%2 == 0 && prev != nil {
					if _, err := readAll(prev); !errors.Is(err, godror.ErrCursorClosed) {
						t.Errorf("fetchArraySize=%d: previous cursor, not read: got %v, wanted ErrCursorClosed", fetchArraySize, err)
					}
				}
				prev = dr
			}
			if err = rows.Err(); err != nil {
				t.Error(err)
			}
			rows.Close()

			// The cursor of a single row is usable after QueryRow closed the rows.
			var dr driver.Rows
			if err = conn.QueryRowContext(ctx, qry, 1).Scan(&dr); err != nil {
				t.Fatal(err)
			}
			if n, err := readAll(dr); n != 3 || err != io.EOF {
				t.Errorf("fetchArraySize=%d: after QueryRow: got %d rows and %v, wanted 3 and EOF", fetchArraySize, n, err)
			}
			dr.Close()
		}()
	}
}
