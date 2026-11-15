// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJSONValueTopLevelTime checks that a time.Time can be bound as
// the whole JSON document, not only inside an object or an array.
func TestJSONValueTopLevelTime(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_json_toptime" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)"); err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && oraErr.Code() == 902 {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	// A TIMESTAMP in JSON is read back in time.Local.
	want := []time.Time{
		time.Date(2024, 2, 29, 23, 59, 58, 123456789, time.Local),
		time.Date(1990, 2, 25, 11, 6, 39, 0, time.Local),
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.Local),
	}
	// scalar bind
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (0, :1)",
		godror.JSONValue{Value: want[0]},
	); err != nil {
		t.Fatal(err)
	}
	// array bind
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)",
		[]int{1, 2}, []godror.JSONValue{{Value: want[1]}, {Value: want[2]}},
	); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, "SELECT id, jdoc, JSON_VALUE(jdoc, '$.type()') FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var id int
		var doc godror.JSON
		var typ string
		if err = rows.Scan(&id, &doc, &typ); err != nil {
			t.Fatal(err)
		}
		n++
		if typ != "timestamp" {
			t.Errorf("%d. stored as %q, wanted timestamp", id, typ)
		}
		v, err := doc.GetValue(godror.JSONOptDefault)
		if err != nil {
			t.Errorf("%d. %+v", id, err)
			continue
		}
		if got, ok := v.(time.Time); !ok || !got.Equal(want[id]) {
			t.Errorf("%d. got %v, wanted %v", id, v, want[id])
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Errorf("got %d rows, wanted %d", n, len(want))
	}

	// Other structs are still rejected.
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (9, :1)",
		godror.JSONValue{Value: struct{ A int }{1}},
	); err == nil {
		t.Error("wanted error for a struct value")
	}
}
