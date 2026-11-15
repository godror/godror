// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	godror "github.com/godror/godror"
)

// TestJSONObjectGetInto checks that JSONObject.GetInto fills a struct from
// a document with strings, numbers, booleans, null, nested objects and arrays,
// and a key that the struct does not have.
func TestJSONObjectGetInto(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	type person struct {
		Name     string
		Raw      []byte
		Age      int
		Score    float64
		Tiny     int8
		Active   bool
		Nick     *string
		Address  map[string]any
		Tags     []any
		Anything any
		hidden   string
	}
	const doc = `{"Name":"Mary","Raw":"bytes","Age":25,"Score":4.5,"Tiny":1000,` +
		`"Active":true,"Nick":null,` +
		`"Address":{"City":"Budapest","Geo":{"Lat":47.5,"Tags":[1,null]}},` +
		`"Tags":["a",2,false,null,{"k":"v"},[]],` +
		`"Anything":true,"hidden":"x","Extra":"no such field"}`
	nick := "to be cleared"
	got := person{Nick: &nick, Tiny: 7}
	want := person{
		Name: "Mary", Raw: []byte("bytes"), Age: 25, Score: 4.5, Active: true,
		Tiny: 7, // 1000 does not fit, so it is left alone
		Address: map[string]any{
			"City": "Budapest",
			"Geo":  map[string]any{"Lat": 47.5, "Tags": []any{1.0, nil}},
		},
		Tags:     []any{"a", 2.0, false, nil, map[string]any{"k": "v"}, []any{}},
		Anything: true,
	}

	rows, err := testDb.QueryContext(ctx, "SELECT JSON(:1) FROM DUAL", doc)
	if err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && oraErr.Code() == 902 {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var j godror.JSON
	if err = rows.Scan(&j); err != nil {
		t.Fatal(err)
	}
	obj, err := j.GetJSONObject(godror.JSONOptDefault)
	if err != nil {
		t.Fatal(err)
	}
	obj.GetInto(&got)
	if d := cmp.Diff(want, got, cmp.AllowUnexported(person{})); d != "" {
		t.Error(d)
	}
}
