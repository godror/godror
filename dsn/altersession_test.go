// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import "testing"

// TestAlterSessionStmt checks the one place that writes the ALTER SESSION SET
// statement: a Keyword is written as-is, a String as a quoted string literal,
// except the documented identifier-taking parameters.
func TestAlterSessionStmt(t *testing.T) {
	for _, tC := range []struct {
		ps   []SessionParam
		want string
	}{
		{
			ps:   []SessionParam{{Key: "NLS_DATE_FORMAT", Value: String(`YYYY "it's" MM`)}},
			want: `ALTER SESSION SET NLS_DATE_FORMAT='YYYY "it''s" MM'`,
		},
		{
			ps:   []SessionParam{{Key: "NLS_NUMERIC_CHARACTERS", Value: String(", ")}},
			want: `ALTER SESSION SET NLS_NUMERIC_CHARACTERS=', '`,
		},
		{
			ps:   []SessionParam{{Key: "ISOLATION_LEVEL", Value: Keyword("READ COMMITTED")}},
			want: `ALTER SESSION SET ISOLATION_LEVEL=READ COMMITTED`,
		},
		{
			ps: []SessionParam{
				{Key: "edition", Value: String("ORA$BASE")},
				{Key: "TIMED_STATISTICS", Value: Keyword("TRUE")},
			},
			want: `ALTER SESSION SET edition=ORA$BASE TIMED_STATISTICS=TRUE`,
		},
		{
			ps:   []SessionParam{{Key: "DB_FILE_MULTIBLOCK_READ_COUNT", Value: String("128")}},
			want: `ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT='128'`,
		},
	} {
		if got := AlterSessionStmt(tC.ps); got != tC.want {
			t.Errorf("%#v:\ngot  %q\nwant %q", tC.ps, got, tC.want)
		}
	}
}

// TestParseSessionParamValue checks how the textual form tells a String from
// a Keyword: a value containing a single quote is a String, everything else
// is a Keyword.
func TestParseSessionParamValue(t *testing.T) {
	for _, tC := range []struct {
		in   string
		want SessionParamValue
	}{
		{"b", Keyword("b")},
		{"READ COMMITTED", Keyword("READ COMMITTED")},
		{"'b c'", String("b c")},
		{"'it''s'", String("it's")},
		{"''", String("")},
		// Contains a quote, but is not written as a literal: use it as is.
		{"it's", String("it's")},
	} {
		if got := parseSessionParamValue(tC.in); got != tC.want {
			t.Errorf("%q: got %#v, wanted %#v", tC.in, got, tC.want)
		}
	}
}

// TestSessionParamRoundtrip checks that String and Parse keep the type of
// the session parameters, and the statement they make.
func TestSessionParamRoundtrip(t *testing.T) {
	ps := []SessionParam{
		{Key: "NLS_DATE_FORMAT", Value: String(`YYYY "it's" MM`)},
		{Key: "NLS_NUMERIC_CHARACTERS", Value: String(", ")},
		{Key: "ISOLATION_LEVEL", Value: Keyword("READ COMMITTED")},
		{Key: "CURRENT_SCHEMA", Value: Keyword("SYSTEM")},
	}
	P0 := ConnectionParams{CommonParams: CommonParams{AlterSession: ps}}
	P1, err := Parse(P0.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	if len(P1.AlterSession) != len(ps) {
		t.Fatalf("got %d session params, wanted %d", len(P1.AlterSession), len(ps))
	}
	for i := range ps {
		got := P1.AlterSession[i]
		if got.Key != ps[i].Key {
			t.Errorf("param %d: got key %q, wanted %q", i, got.Key, ps[i].Key)
		}
		switch want := ps[i].Value.(type) {
		case Keyword:
			if g, ok := got.Value.(Keyword); !ok || g != want {
				t.Errorf("param %d: got %#v, wanted the Keyword %q", i, got.Value, want)
			}
		case String:
			if g, ok := got.Value.(String); !ok || g != want {
				t.Errorf("param %d: got %#v, wanted the String %q", i, got.Value, want)
			}
		}
	}
	if got, want := AlterSessionStmt(P1.AlterSession), AlterSessionStmt(ps); got != want {
		t.Errorf("the round-trip changed the statement:\ngot  %q\nwant %q", got, want)
	}
}
