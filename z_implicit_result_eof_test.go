// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"fmt"
	"testing"
	"time"
)

// TestImplicitResultsBreak leaves each implicit result set after its first
// row: after the last NextResultSet, rows.Err must be nil.
func TestImplicitResultsBreak(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	const qry = `DECLARE c1 SYS_REFCURSOR; c2 SYS_REFCURSOR; BEGIN
  OPEN c1 FOR SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 2; DBMS_SQL.RETURN_RESULT(c1);
  OPEN c2 FOR SELECT 10+LEVEL FROM DUAL CONNECT BY LEVEL <= 2; DBMS_SQL.RETURN_RESULT(c2); END;`
	for _, brk := range []bool{false, true} {
		rows, err := testDb.QueryContext(ctx, qry)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int
		for {
			for rows.Next() {
				var id int
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				ids = append(ids, id)
				if brk {
					break
				}
			}
			if !rows.NextResultSet() {
				break
			}
		}
		if err = rows.Err(); err != nil {
			t.Errorf("break=%t: rows.Err: %v", brk, err)
		}
		rows.Close()
		want := "[1 2 11 12]"
		if brk {
			want = "[1 11]"
		}
		if got := fmt.Sprint(ids); got != want {
			t.Errorf("break=%t: got %s, wanted %s", brk, got, want)
		}
	}
}
