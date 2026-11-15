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

// TestQueryOutParams checks that the out parameters of a PL/SQL block are set
// when it is run with Query, which is needed to read its implicit results.
func TestQueryOutParams(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	stmt, err := testDb.PrepareContext(ctx, `DECLARE c1 SYS_REFCURSOR; BEGIN
  OPEN c1 FOR SELECT level FROM DUAL CONNECT BY level <= :1;
  DBMS_SQL.RETURN_RESULT(c1);
  :2 := :1 * 2; :3 := 'set' || :1; END;`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, in := range []int64{3, 5} {
		num, str := int64(-1), "unset"
		rows, err := stmt.QueryContext(ctx, in, sql.Out{Dest: &num}, sql.Out{Dest: &str})
		if err != nil {
			t.Fatal(err)
		}
		var n int64
		for rows.NextResultSet() {
			for rows.Next() {
				n++
			}
		}
		if err = rows.Err(); err != nil {
			t.Error(err)
		}
		rows.Close()
		if n != in {
			t.Errorf("in=%d: got %d rows, wanted %d", in, n, in)
		}
		if want := "set" + string(rune('0'+in)); num != 2*in || str != want {
			t.Errorf("in=%d: got out parameters %d, %q; wanted %d, %q", in, num, str, 2*in, want)
		}
	}
}
