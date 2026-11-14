// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

const implicitResultsQry = `BEGIN
  DECLARE
    c1 SYS_REFCURSOR;
    c2 SYS_REFCURSOR;
  BEGIN
    OPEN c1 FOR SELECT 1 FROM DUAL;
    DBMS_SQL.RETURN_RESULT(c1);
    OPEN c2 FOR SELECT 'A' FROM DUAL;
    DBMS_SQL.RETURN_RESULT(c2);
  END;
END;`

// readImplicitResults reads all result sets with the standard database/sql loop.
func readImplicitResults(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, implicitResultsQry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var got []string
	for {
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				return got, err
			}
			got = append(got, s)
		}
		if err := rows.Err(); err != nil {
			return got, err
		}
		if !rows.NextResultSet() {
			break
		}
	}
	return got, rows.Err()
}

func skipNoImplicitResults(t *testing.T, err error) {
	if err != nil && strings.Contains(err.Error(), "PLS-00302:") {
		t.Skip(err)
	}
}

// TestImplicitResultsStandardLoop reads two implicit results with the
// usual Next/NextResultSet loop.
func TestImplicitResultsStandardLoop(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got, err := readImplicitResults(ctx, conn)
	skipNoImplicitResults(t, err)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[1 A]" {
		t.Errorf("got %q, wanted [1 A]", got)
	}
}

// TestImplicitResultsNoCursorLeak runs a block returning implicit results
// many times on one session, which must not run out of open cursors.
func TestImplicitResultsNoCursorLeak(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := 0; i < 1000; i++ {
		got, err := readImplicitResults(ctx, conn)
		skipNoImplicitResults(t, err)
		if err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
		if fmt.Sprint(got) != "[1 A]" {
			t.Fatalf("%d: got %q, wanted [1 A]", i, got)
		}
	}
}
