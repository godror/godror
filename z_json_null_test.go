// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	godror "github.com/godror/godror"
)

// TestJSONValueNull checks that a nil in a JSONValue is written as JSON null,
// at the top level and inside an object or an array, and is read back as nil.
func TestJSONValueNull(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_json_null" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)"); err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && oraErr.Code() == 902 {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	want := []struct {
		Value any
		Text  string
	}{
		{nil, `null`},
		{map[string]any{"a": nil}, `{"a":null}`},
		{[]any{nil, "x"}, `[null,"x"]`},
		{nil, `null`},
	}
	// The first row goes through the scalar bind, the rest through the array bind.
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (0, :1)",
		godror.JSONValue{Value: want[0].Value},
	); err != nil {
		t.Fatal(err)
	}
	ids := make([]int, 0, len(want)-1)
	docs := make([]godror.JSONValue, 0, len(want)-1)
	for i, w := range want[1:] {
		ids = append(ids, i+1)
		docs = append(docs, godror.JSONValue{Value: w.Value})
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)", ids, docs); err != nil {
		t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx, "SELECT id, jdoc, JSON_SERIALIZE(jdoc) FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		var id int
		var doc godror.JSON
		var text sql.NullString
		if err = rows.Scan(&id, &doc, &text); err != nil {
			t.Fatal(err)
		}
		n++
		// JSON null, not SQL NULL
		if !text.Valid || text.String != want[id].Text {
			t.Errorf("%d. stored %+v, wanted %s", id, text, want[id].Text)
		}
		got, err := doc.GetValue(godror.JSONOptDefault)
		if err != nil {
			t.Errorf("%d. %+v", id, err)
			continue
		}
		if d := cmp.Diff(want[id].Value, got); d != "" {
			t.Errorf("%d. read back mismatch: %s", id, d)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Errorf("got %d rows, wanted %d", n, len(want))
	}
}
