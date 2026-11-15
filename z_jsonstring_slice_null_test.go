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

// An empty element in a []JSONString must make its own row NULL, and no
// other row.
func TestJSONStringSliceNull(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_jsonstring_null" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)"); err != nil {
		if errIs(err, 902, "") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	// The second batch reuses the variable of the first.
	if _, err = stmt.ExecContext(ctx, []int{1, 2, 3},
		[]godror.JSONString{{Value: `{"a":1}`}, {Value: `{"a":2}`}, {Value: `{"a":3}`}},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = stmt.ExecContext(ctx, []int{4, 5, 6},
		[]godror.JSONString{{Value: `{"a":4}`}, {}, {Value: `{"a":6}`}},
	); err != nil {
		t.Fatal(err)
	}

	want := map[int]string{1: `{"a":1}`, 2: `{"a":2}`, 3: `{"a":3}`, 4: `{"a":4}`, 5: "NULL", 6: `{"a":6}`}
	rows, err := conn.QueryContext(ctx, "SELECT id, NVL(JSON_SERIALIZE(jdoc), 'NULL') FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var id int
		var s string
		if err = rows.Scan(&id, &s); err != nil {
			t.Fatal(err)
		}
		n++
		if s != want[id] {
			t.Errorf("id=%d: got %s, wanted %s", id, s, want[id])
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Errorf("got %d rows, wanted %d", n, len(want))
	}
}
