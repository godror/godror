// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestSessionParamTyped checks that the ALTER SESSION values written on
// session init are what the caller said: a godror.Keyword is written as-is
// (for the parameters that take a keyword or an identifier), everything
// else as a quoted string literal that keeps its quotes and content.
func TestSessionParamTyped(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const readback = `SELECT TO_CHAR(DATE '2026-01-02'), TO_CHAR(1.5), SESSIONTIMEZONE, DBTIMEZONE,
	SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL`

	for _, tC := range []struct {
		name    string
		reparse bool
		set     func(P *godror.ConnectionParams)
		want    [5]string
	}{
		{
			name: "Keyword and string",
			set: func(P *godror.ConnectionParams) {
				P.SetSessionParamOnInit("NLS_DATE_FORMAT", `YYYY "it's" MM`)
				P.SetSessionParamOnInit("NLS_NUMERIC_CHARACTERS", ", ")
				P.SetSessionParamOnInitKeyword("ISOLATION_LEVEL", godror.SessionParamKeyword("SERIALIZABLE"))
				P.SetSessionParamOnInitKeyword("TIMED_STATISTICS", godror.SessionParamKeyword("TRUE"))
				P.SetSessionParamOnInitKeyword("CURRENT_SCHEMA", godror.SessionParamKeyword("SYSTEM"))
			},
			want: [5]string{`2026 it's 01`, "1,5", "", "", "SYSTEM"},
		},
		{
			name: "TIME_ZONE keyword",
			set: func(P *godror.ConnectionParams) {
				P.SetSessionParamOnInitKeyword("TIME_ZONE", godror.SessionParamKeyword("DBTIMEZONE"))
			},
			// SESSIONTIMEZONE (2) must equal DBTIMEZONE (3): checked below.
			want: [5]string{"", "", "", "", ""},
		},
		{
			name: "identifier params as plain strings (compat)",
			set: func(P *godror.ConnectionParams) {
				P.SetSessionParamOnInit("CURRENT_SCHEMA", "SYSTEM")
				P.SetSessionParamOnInit("NLS_DATE_FORMAT", `YYYY"|"MM`)
			},
			want: [5]string{"2026|01", "", "", "", "SYSTEM"},
		},
		{
			name:    "round-tripped through the DSN text",
			reparse: true,
			set: func(P *godror.ConnectionParams) {
				P.SetSessionParamOnInit("NLS_DATE_FORMAT", `YYYY "it's" MM`)
				P.SetSessionParamOnInit("NLS_NUMERIC_CHARACTERS", ", ")
				P.SetSessionParamOnInitKeyword("ISOLATION_LEVEL", godror.SessionParamKeyword("SERIALIZABLE"))
				P.SetSessionParamOnInitKeyword("CURRENT_SCHEMA", godror.SessionParamKeyword("SYSTEM"))
			},
			want: [5]string{`2026 it's 01`, "1,5", "", "", "SYSTEM"},
		},
		{
			name: "NewSessionIniter (deprecated)",
			set: func(P *godror.ConnectionParams) {
				P.OnInit = godror.NewSessionIniter(map[string]string{
					"NLS_DATE_FORMAT":        `YYYY "it's" MM`,
					"NLS_NUMERIC_CHARACTERS": ", ",
				})
			},
			want: [5]string{`2026 it's 01`, "1,5", "", "", ""},
		},
	} {
		t.Run(tC.name, func(t *testing.T) {
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.StandaloneConnection = godror.Bool(true)
			tC.set(&P)
			if tC.reparse {
				// The value types must survive the textual form.
				if P, err = godror.ParseDSN(P.StringWithPassword()); err != nil {
					t.Fatal(err)
				}
			}
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			var got [5]string
			if err = db.QueryRowContext(ctx, readback).Scan(
				&got[0], &got[1], &got[2], &got[3], &got[4]); err != nil {
				t.Fatal(err)
			}
			t.Logf("got %q", got)
			for i, want := range tC.want {
				if want != "" && got[i] != want {
					t.Errorf("column %d: got %q, wanted %q", i, got[i], want)
				}
			}
			if tC.name == "TIME_ZONE keyword" && got[2] != got[3] {
				t.Errorf("SESSIONTIMEZONE %q != DBTIMEZONE %q", got[2], got[3])
			}
			if t.Failed() {
				t.FailNow()
			}
		})
	}
}
