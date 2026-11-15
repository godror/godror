// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"reflect"
	"testing"

	godror "github.com/godror/godror"
)

// MapToSlice must skip comments, literals and quoted identifiers,
// and nothing else.
func TestMapToSliceScanner(t *testing.T) {
	for _, tC := range []struct {
		in, want string
		params   []any
	}{
		// a star right after a closed comment or hint is not a new comment
		{"SELECT /*+ FIRST_ROWS(10) */* FROM t WHERE id = :id AND x = :x",
			"SELECT /*+ FIRST_ROWS(10) */* FROM t WHERE id = :1 AND x = :2", []any{"id", "x"}},
		{"SELECT COUNT(/*all*/*) FROM t WHERE id = :id",
			"SELECT COUNT(/*all*/*) FROM t WHERE id = :1", []any{"id"}},
		{"SELECT :a /* c */* :b FROM DUAL",
			"SELECT :1 /* c */* :2 FROM DUAL", []any{"a", "b"}},
		// the star that opens a comment does not close it
		{"SELECT /*/ :x */ :a FROM DUAL", "SELECT /*/ :x */ :1 FROM DUAL", []any{"a"}},
		{"SELECT /**/ :a FROM DUAL", "SELECT /**/ :1 FROM DUAL", []any{"a"}},
		// comments do not nest
		{"SELECT /* a /* :b */ :c FROM DUAL", "SELECT /* a /* :b */ :1 FROM DUAL", []any{"c"}},
		// an apostrophe or a quote in a comment
		{"SELECT /* it's :x */ :a -- don't :y \"\n, :b FROM DUAL",
			"SELECT /* it's :x */ :1 -- don't :y \"\n, :2 FROM DUAL", []any{"a", "b"}},
		{"SELECT :a -- :x", "SELECT :1 -- :x", []any{"a"}},
		{"SELECT :a-:b - - :c FROM DUAL", "SELECT :1-:2 - - :3 FROM DUAL", []any{"a", "b", "c"}},
		{"SELECT :a/:b*:c FROM DUAL", "SELECT :1/:2*:3 FROM DUAL", []any{"a", "b", "c"}},

		// alternative quoting
		{"SELECT q'[it's :not a param]' FROM t WHERE id = :id",
			"SELECT q'[it's :not a param]' FROM t WHERE id = :1", []any{"id"}},
		{"BEGIN EXECUTE IMMEDIATE q'{UPDATE t SET a = 'x' WHERE b = :b}' USING :val; END;",
			"BEGIN EXECUTE IMMEDIATE q'{UPDATE t SET a = 'x' WHERE b = :b}' USING :1; END;", []any{"val"}},
		{"SELECT Q'(a':x)', q'<b':y>', q'!c':z!', q'|d'|:w|', :a FROM DUAL",
			"SELECT Q'(a':x)', q'<b':y>', q'!c':z!', q'|d'|:w|', :1 FROM DUAL", []any{"a"}},
		{"SELECT q'[]', q'[]]', q'['']', :a FROM DUAL",
			"SELECT q'[]', q'[]]', q'['']', :1 FROM DUAL", []any{"a"}},
		{"SELECT nq'<it's :x>', Nq'[:y']', :a FROM DUAL",
			"SELECT nq'<it's :x>', Nq'[:y']', :1 FROM DUAL", []any{"a"}},
		// not alternative quoting
		{"SELECT n'it''s :x', N':y', :a FROM DUAL", "SELECT n'it''s :x', N':y', :1 FROM DUAL", []any{"a"}},
		{"SELECT 'it''s :x', '', '''', :a FROM DUAL", "SELECT 'it''s :x', '', '''', :1 FROM DUAL", []any{"a"}},

		// quoted identifiers
		{`SELECT a AS "ratio a:b" FROM t WHERE id = :id`,
			`SELECT a AS "ratio a:b" FROM t WHERE id = :1`, []any{"id"}},
		{`SELECT a AS "driver's" FROM t WHERE id = :id`,
			`SELECT a AS "driver's" FROM t WHERE id = :1`, []any{"id"}},
		{`SELECT '"' AS "'", :a AS "--", :b AS "/*" FROM DUAL`,
			`SELECT '"' AS "'", :1 AS "--", :2 AS "/*" FROM DUAL`, []any{"a", "b"}},

		// what ends a parameter name may start something
		{`SELECT :a"it's :x", :b FROM DUAL`, `SELECT :1"it's :x", :2 FROM DUAL`, []any{"a", "b"}},
		{"SELECT :a/* :x */, :b--:y", "SELECT :1/* :x */, :2--:y", []any{"a", "b"}},
		{"SELECT :a||':x'||:b FROM DUAL", "SELECT :1||':x'||:2 FROM DUAL", []any{"a", "b"}},

		// as before
		{"BEGIN :a := :b; x := 1; y:=2; END;", "BEGIN :1 := :2; x := 1; y:=2; END;", []any{"a", "b"}},
		{"SELECT a::b, :c FROM DUAL", "SELECT a::b, :1 FROM DUAL", []any{"c"}},
		{"SELECT :1, :a1, :a_$#, :2b FROM DUAL", "SELECT :1, :1, :2, :2b FROM DUAL", []any{"a1", "a_$#"}},
		{"SELECT 'árvíztűrő :x', :a FROM DUAL", "SELECT 'árvíztűrő :x', :1 FROM DUAL", []any{"a"}},
		{":a", ":1", []any{"a"}},
		{"", "", []any{}},
	} {
		got, params := godror.MapToSlice(tC.in, func(s string) any { return s })
		if got != tC.want || !reflect.DeepEqual(params, tC.params) {
			t.Errorf("MapToSlice(%q)\n   got %q %v\nwanted %q %v", tC.in, got, params, tC.want, tC.params)
		}
	}
}
