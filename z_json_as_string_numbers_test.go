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

// TestJSONAsStringNumberPrecision checks that the JSON text of a document
// has every digit of its numbers.
func TestJSONAsStringNumberPrecision(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const qry = "SELECT JSON(:1) FROM DUAL"
	for _, tc := range []struct{ doc, want string }{
		{`9007199254740993`, `9007199254740993`},
		{`12345678901234567890123456789012345678`, `12345678901234567890123456789012345678`},
		{`-0.1234567890123456789012345678901234567`, `-0.1234567890123456789012345678901234567`},
		{`{"a":12345678901234567890123456789012345678}`, `{"a":12345678901234567890123456789012345678}`},
		{`[9007199254740993,{"b":[0.1234567890123456789012345678901234567]}]`, `[9007199254740993,{"b":[0.1234567890123456789012345678901234567]}]`},
		// No exponent: ODPI-C gives the NUMBER as plain decimal text.
		{`[1e21,1e-7]`, `[1000000000000000000000,0.0000001]`},
		// These are the same as before.
		{`[0,1,-1,100,1.10,-0.5,123456.789,1e20,0.000001]`, `[0,1,-1,100,1.1,-0.5,123456.789,100000000000000000000,0.000001]`},
		{`{"s":"1.10","t":true,"n":null}`, `{"n":null,"s":"1.10","t":true}`},
	} {
		var got string
		if err := conn.QueryRowContext(ctx, qry, tc.doc, godror.JSONAsString()).Scan(&got); err != nil {
			if tc.doc == `9007199254740993` {
				t.Skipf("JSON needs Oracle 21c: %+v", err)
			}
			t.Errorf("%s: %+v", tc.doc, err)
			continue
		}
		if got != tc.want {
			t.Errorf("JSONAsString: got %s, wanted %s", got, tc.want)
		}

		var j godror.JSON
		rows, err := conn.QueryContext(ctx, qry, tc.doc)
		if err != nil {
			t.Fatalf("%s: %+v", tc.doc, err)
		}
		for rows.Next() {
			// The JSON is valid only till the next rows.Next.
			if err = rows.Scan(&j); err != nil {
				t.Errorf("%s: %+v", tc.doc, err)
			} else if got = j.String(); got != tc.want {
				t.Errorf("JSON.String: got %s, wanted %s", got, tc.want)
			}
		}
		rows.Close()
	}

	// An explicit JSONStringOption(JSONOptNumberAsString) keeps
	// returning the numbers as strings.
	var got string
	if err := conn.QueryRowContext(ctx, qry, `[9007199254740993]`,
		godror.JSONAsString(), godror.JSONStringOption(godror.JSONOptNumberAsString),
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if want := `["9007199254740993"]`; got != want {
		t.Errorf("JSONStringOption(JSONOptNumberAsString): got %s, wanted %s", got, want)
	}
}
