// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"math"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJSONIntervalDSOverflow checks INTERVAL DAY TO SECOND values in JSON
// that don't fit in time.Duration: the GetValue methods return an error,
// and Data.GetIntervalDS saturates instead of wrapping around.
func TestJSONIntervalDSOverflow(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	for _, tc := range []struct {
		lit  string
		want time.Duration
		ok   bool
	}{
		{lit: "100 1:2:3.5", want: 100*24*time.Hour + time.Hour + 2*time.Minute + 3500*time.Millisecond, ok: true},
		{lit: "200000 0:0:0", want: math.MaxInt64},  // about 547 years
		{lit: "-200000 0:0:0", want: math.MinInt64}, // about -547 years
	} {
		iv := "INTERVAL '" + tc.lit + "' DAY(9) TO SECOND(9)"
		rows, err := testDb.QueryContext(ctx,
			"SELECT JSON_SCALAR("+iv+"), JSON_ARRAY("+iv+" RETURNING JSON), JSON_OBJECT('iv' VALUE "+iv+" RETURNING JSON) FROM DUAL")
		if err != nil {
			if errIs(err, 902, "invalid datatype") || errIs(err, 904, "invalid identifier") {
				t.Skip(err)
			}
			t.Fatal(err)
		}
		for rows.Next() {
			var scalar, arr, obj godror.JSON
			if err := rows.Scan(&scalar, &arr, &obj); err != nil {
				t.Fatal(err)
			}
			js, err := scalar.GetJSONScalar(godror.JSONOptDefault)
			if err != nil {
				t.Fatal(err)
			}
			var d godror.Data
			if err := scalar.Get(&d, godror.JSONOptDefault); err != nil {
				t.Fatal(err)
			}
			if d.NativeTypeNum != godror.NativeTypeIntervalDS {
				t.Fatalf("%s: got native type %d, wanted INTERVAL DAY TO SECOND", tc.lit, d.NativeTypeNum)
			}
			if got := d.GetIntervalDS(); got != tc.want {
				t.Errorf("%s: GetIntervalDS got %v, wanted %v", tc.lit, got, tc.want)
			}

			for _, g := range []struct {
				name string
				get  func() (any, error)
				pick func(any) any
			}{
				{"JSONScalar", js.GetValue, func(v any) any { return v }},
				{"JSONArray", func() (any, error) { return arr.GetValue(godror.JSONOptDefault) },
					func(v any) any { return v.([]any)[0] }},
				{"JSONObject", func() (any, error) { return obj.GetValue(godror.JSONOptDefault) },
					func(v any) any { return v.(map[string]any)["iv"] }},
			} {
				v, err := g.get()
				if !tc.ok {
					if err == nil {
						t.Errorf("%s: %s.GetValue got %v, wanted an error", tc.lit, g.name, v)
					}
				} else if err != nil {
					t.Errorf("%s: %s.GetValue: %v", tc.lit, g.name, err)
				} else if got := g.pick(v); got != tc.want {
					t.Errorf("%s: %s.GetValue got %v, wanted %v", tc.lit, g.name, got, tc.want)
				}
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
