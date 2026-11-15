// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A NULL vector bound after a non-NULL one, into the same variable, must
// be inserted as NULL.
func TestVectorNullAfterValue(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_vector_null" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), v VECTOR)")
	skipIfNoVectorSupport(t, err)
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx, "INSERT INTO "+tbl+" (id, v) VALUES (:1, :2)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	val := godror.Vector{Values: []float32{1, 2, 3}}
	for _, args := range [][2]any{
		{1, val},
		{2, godror.Vector{}},
		{3, val},
		{4, (*godror.Vector)(nil)},
		{[]int{5, 6}, []godror.Vector{val, val}},
		{[]int{7, 8}, []godror.Vector{{}, val}},
		{[]int{9, 10}, []*godror.Vector{&val, nil}},
	} {
		if _, err = stmt.ExecContext(ctx, args[0], args[1]); err != nil {
			t.Fatalf("%v: %+v", args[0], err)
		}
	}

	wantNull := map[int]bool{2: true, 4: true, 7: true, 10: true}
	rows, err := conn.QueryContext(ctx, "SELECT id, NVL2(v, 0, 1) FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var id, isNull int
		if err = rows.Scan(&id, &isNull); err != nil {
			t.Fatal(err)
		}
		n++
		if (isNull == 1) != wantNull[id] {
			t.Errorf("id=%d: got NULL=%t, wanted %t", id, isNull == 1, wantNull[id])
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("got %d rows, wanted 10", n)
	}
}
