// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestLongStringPlSQL checks that a string of up to 32767 bytes
// is bound as VARCHAR2, which PL/SQL and CALL accept, not as LONG.
func TestLongStringPlSQL(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl, prc := "test_longstr"+tblSuffix, "test_longstr_p"+tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	for _, qry := range []string{
		"CREATE TABLE " + tbl + " (id NUMBER(3), c CLOB)",
		"CREATE OR REPLACE PROCEDURE " + prc + "(p IN VARCHAR2, q IN OUT VARCHAR2, o OUT NUMBER) IS BEGIN o := LENGTHB(p) + LENGTHB(q); q := SUBSTR(q, 1, 1); END;",
	} {
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	defer func() {
		testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")
		testDb.ExecContext(context.Background(), "DROP PROCEDURE "+prc)
	}()

	// A LONG bind executed the cursor that the connection had used for
	// a shorter value, and the selected string was cut to that size.
	// The text is new in every run: cursors that other sessions left in the
	// shared pool for the same text decide the size, too.
	qry := fmt.Sprintf("SELECT /* %s %d */ :1 FROM DUAL", tbl, time.Now().UnixNano())
	for _, n := range []int{12000, 10, 12000, 100, 32767} {
		s := strings.Repeat("x", n)
		var got string
		if err = conn.QueryRowContext(ctx, qry, s).Scan(&got); err != nil {
			t.Errorf("SELECT :1 with %d bytes: %+v", n, err)
		} else if got != s {
			t.Errorf("SELECT :1 with %d bytes returned %d bytes", n, len(got))
		}
	}

	for _, unit := range []string{"x", "á", "€"} {
		for _, n := range []int{8191, 8192, 20000, 32767, 32768} {
			s := strings.Repeat(unit, n/len(unit))
			s += strings.Repeat("x", n-len(s))
			name := strconv.Itoa(len(unit)) + "-byte characters, " + strconv.Itoa(n) + " bytes"

			// A string over 32767 bytes is a LONG as before, good for a CLOB.
			if _, err = conn.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				t.Fatal(err)
			}
			if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c) VALUES (1, :1)", s); err != nil {
				t.Errorf("%s: INSERT: %+v", name, err)
			}
			if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c) VALUES (:1, :2)",
				[]int{2, 3, 4}, []string{"short", s, ""},
			); err != nil {
				t.Errorf("%s: batch INSERT: %+v", name, err)
			}
			for _, id := range []int{1, 3} {
				var got string
				if err = conn.QueryRowContext(ctx, "SELECT c FROM "+tbl+" WHERE id = :1", id, godror.ClobAsString()).Scan(&got); err != nil {
					t.Errorf("%s: SELECT %d: %+v", name, id, err)
				} else if got != s {
					t.Errorf("%s: row %d has %d bytes", name, id, len(got))
				}
			}
			if n > 32767 {
				continue
			}

			var length int
			if _, err = conn.ExecContext(ctx, "BEGIN :1 := LENGTHB(:2); END;", sql.Out{Dest: &length}, s); err != nil {
				t.Errorf("%s: PL/SQL expression: %+v", name, err)
			} else if length != n {
				t.Errorf("%s: PL/SQL expression: got %d", name, length)
			}
			for _, qry := range []string{"CALL " + prc + "(:1, :2, :3)", "BEGIN " + prc + "(:1, :2, :3); END;"} {
				length = 0
				inOut := s
				if _, err = conn.ExecContext(ctx, qry, s, sql.Out{Dest: &inOut, In: true}, sql.Out{Dest: &length}); err != nil {
					t.Errorf("%s: %s: %+v", name, qry, err)
				} else if length != 2*n || inOut != unit {
					t.Errorf("%s: %s: got %d, %q", name, qry, length, inOut)
				}
			}
		}
	}
}
