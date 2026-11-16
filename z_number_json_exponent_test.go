// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A Number must be readable from a JSON number with an exponent (that is
// how encoding/json writes a big or a small float64), and the result must
// be something the database accepts.
func TestNumberJSONExponent(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	big, err := json.Marshal(1e21) // 1e+21
	if err != nil {
		t.Fatal(err)
	}
	small, err := json.Marshal(1e-7) // 1e-07
	if err != nil {
		t.Fatal(err)
	}
	for _, tC := range []struct {
		in   string
		want godror.Number
	}{
		{in: string(big), want: "1000000000000000000000"},
		{in: string(small), want: "0.0000001"},
		{in: "1e3", want: "1000"},
		{in: "1E3", want: "1000"},
		{in: "1e0", want: "1"},
		{in: "0e5", want: "0"},
		{in: "-0.00e-3", want: "0"},
		{in: "-1.5e3", want: "-1500"},
		{in: "1.5e-3", want: "0.0015"},
		{in: "-12.345e-4", want: "-0.0012345"},
		{in: "12.345e1", want: "123.45"},
		{in: "12.345e3", want: "12345"},
		{in: "0.05e1", want: "0.5"},
		{in: "0.001e3", want: "1"},
		{in: `"2.5e+2"`, want: "250"},
		{in: "1e125", want: godror.Number("1" + strings.Repeat("0", 125))},
		{in: "1e-130", want: godror.Number("0." + strings.Repeat("0", 129) + "1")},
		{in: "123", want: "123"},
		{in: "-0.25", want: "-0.25"},
	} {
		got := godror.Number("7") // must be overwritten
		if err := json.Unmarshal([]byte(tC.in), &got); err != nil {
			t.Errorf("unmarshal %s: %v", tC.in, err)
			continue
		}
		if got != tC.want {
			t.Errorf("unmarshal %s: got %q, wanted %q", tC.in, got, tC.want)
			continue
		}
		// bind it and get it back
		var fromDB godror.Number
		if err := testDb.QueryRowContext(ctx, "SELECT :1 + 0 FROM DUAL", got).Scan(&fromDB); err != nil {
			t.Errorf("bind %q (from %s): %v", got, tC.in, err)
		} else if fromDB != tC.want {
			t.Errorf("bound %q (from %s), the database returned %q", got, tC.in, fromDB)
		}
	}

	for _, in := range []string{"1e", "e5", ".e1", "1e+", "1e1e1", "1e1.5"} {
		var n godror.Number
		if err := json.Unmarshal([]byte(in), &n); err == nil {
			t.Errorf("unmarshal %s: no error, got %q", in, n)
		}
	}
	// The expanded text has the same limit as the plain one.
	for _, in := range []string{"1e173", "1e-172", "1e5000", "1e-5000", "1e99999999999999999999"} {
		var n godror.Number
		if err := json.Unmarshal([]byte(in), &n); !errors.Is(err, godror.ErrNumberTooLong) {
			t.Errorf("unmarshal %s: got %q and %v, wanted ErrNumberTooLong", in, n, err)
		}
	}
}
