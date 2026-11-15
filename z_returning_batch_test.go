// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

// TestReturningBatch checks that the out slice of a RETURNING INTO does not
// set the number of executions, and that it gets the rows of every batch row.
func TestReturningBatch(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_ret_batch" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (dept NUMBER(3), id NUMBER(6), name VARCHAR2(20), n NUMBER(6))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	// dept 1 gets ids 10, 11, 12; dept 2 gets 20; dept 3 gets 30, 31.
	depts := []int{1, 1, 1, 2, 3, 3}
	ids := []int64{10, 11, 12, 20, 30, 31}
	names := []string{"a", "b", "c", "d", "e", "f"}
	gotIDs := []int64{7, 8} // must not matter
	var gotNames []string
	res, err := testDb.ExecContext(ctx,
		"INSERT INTO "+tbl+" (dept, id, name, n) VALUES (:1, :2, :3, 0) RETURNING id+1, name||'x' INTO :4, :5",
		depts, ids, names, sql.Out{Dest: &gotIDs}, sql.Out{Dest: &gotNames})
	if err != nil {
		t.Fatal(err)
	}
	if ra, _ := res.RowsAffected(); ra != 6 {
		t.Errorf("insert: RowsAffected=%d, wanted 6", ra)
	}
	if want := []int64{11, 12, 13, 21, 31, 32}; !reflect.DeepEqual(gotIDs, want) {
		t.Errorf("insert: got ids %v, wanted %v", gotIDs, want)
	}
	if want := []string{"ax", "bx", "cx", "dx", "ex", "fx"}; !reflect.DeepEqual(gotNames, want) {
		t.Errorf("insert: got names %v, wanted %v", gotNames, want)
	}

	stmt, err := testDb.PrepareContext(ctx, "UPDATE "+tbl+" SET n = n + 1 WHERE dept = :1 RETURNING id INTO :2")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	// The out slice keeps the previous result between the calls.
	gotIDs = nil
	for _, tC := range []struct {
		dept any
		want []int64
	}{
		{1, []int64{10, 11, 12}},
		{3, []int64{30, 31}},
		{9, []int64{}},
		// each batch row returns a different number of rows
		{[]int{1, 9, 2, 3}, []int64{10, 11, 12, 20, 30, 31}},
		{2, []int64{20}},
	} {
		res, err := stmt.ExecContext(ctx, tC.dept, sql.Out{Dest: &gotIDs})
		if err != nil {
			t.Fatalf("dept=%v: %+v", tC.dept, err)
		}
		if ra, _ := res.RowsAffected(); int(ra) != len(tC.want) {
			t.Errorf("dept=%v: RowsAffected=%d, wanted %d", tC.dept, ra, len(tC.want))
		}
		if len(gotIDs) != len(tC.want) || len(gotIDs) != 0 && !reflect.DeepEqual(gotIDs, tC.want) {
			t.Errorf("dept=%v: got %v, wanted %v", tC.dept, gotIDs, tC.want)
		}
	}

	// Every UPDATE above must have run once.
	rows, err := testDb.QueryContext(ctx, "SELECT dept, SUM(n) FROM "+tbl+" GROUP BY dept ORDER BY dept")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[int]int{1: 6, 2: 2, 3: 4}
	for rows.Next() {
		var dept, n int
		if err := rows.Scan(&dept, &n); err != nil {
			t.Fatal(err)
		}
		if n != want[dept] {
			t.Errorf("dept %d: SUM(n)=%d, wanted %d", dept, n, want[dept])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
