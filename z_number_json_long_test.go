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

// A NUMBER's text can be longer than 40 characters.
func TestNumberJSONLong(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	type rec struct {
		N godror.Number
	}
	for _, qry := range []string{
		"SELECT 1/4 FROM DUAL",
		"SELECT 1/3 FROM DUAL",
		"SELECT -2/3 FROM DUAL",
		"SELECT 1e40 FROM DUAL",
		"SELECT 1e-39 FROM DUAL",
		"SELECT -1.23e-100 FROM DUAL",
	} {
		var want rec
		if err := testDb.QueryRowContext(ctx, qry).Scan(&want.N); err != nil {
			t.Errorf("%s: %v", qry, err)
			continue
		}
		b, err := json.Marshal(want)
		if err != nil {
			t.Errorf("%s: marshal %q: %v", qry, want.N, err)
			continue
		}
		var got rec
		if err = json.Unmarshal(b, &got); err != nil {
			t.Errorf("%s: %q marshals to %s, unmarshal: %v", qry, want.N, b, err)
		} else if got != want {
			t.Errorf("%s: %q (%d chars) marshals to %s, unmarshals as %q", qry, want.N, len(want.N), b, got.N)
		}

		if b, err = want.N.MarshalText(); err != nil {
			t.Errorf("%s: MarshalText: %v", qry, err)
		} else if err = got.N.UnmarshalText(b); err != nil {
			t.Errorf("%s: UnmarshalText(%q): %v", qry, b, err)
		} else if got != want {
			t.Errorf("%s: %q as text is %q, parsed back as %q", qry, want.N, b, got.N)
		}
	}

	// A long text that is not a number is an error, not an empty Number.
	var n godror.Number
	if err := json.Unmarshal([]byte(`"`+strings.Repeat("1", 50)+`x"`), &n); err == nil {
		t.Errorf("unmarshal of a long non-number: no error, got %q", n)
	}
}

// The longest texts of a NUMBER must survive the marshaling and the database,
// and a longer text must be refused.
func TestNumberTextLimit(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	type rec struct {
		N godror.Number
	}
	zeros := func(n int) string { return strings.Repeat("0", n) }
	// 171 characters: the smallest exponent with all the 20 base-100 digits
	longest := godror.Number("-0." + zeros(128) + strings.Repeat("3", 40))
	for _, want := range []godror.Number{
		godror.Number("1" + zeros(125)),                            // 1e125
		godror.Number("-" + strings.Repeat("9", 40) + zeros(86)),   // the largest
		godror.Number("0." + zeros(129) + "1"),                     // 1e-130
		godror.Number("-0." + zeros(129) + "1"),                    // -1e-130
		godror.Number("-0." + zeros(89) + strings.Repeat("9", 38)), // NUMBER(38,127)
		godror.Number("-" + strings.Repeat("9", 38) + zeros(84)),   // NUMBER(38,-84)
		godror.Number("0." + zeros(128) + strings.Repeat("9", 40)), // 170 characters
		longest,
	} {
		var fromDB godror.Number
		if err := testDb.QueryRowContext(ctx, "SELECT :1 FROM DUAL", want).Scan(&fromDB); err != nil {
			t.Errorf("%q: %v", want, err)
		} else if fromDB != want {
			t.Errorf("bound %q (%d chars), got %q back", want, len(want), fromDB)
		}

		b, err := json.Marshal(rec{N: want})
		if err != nil {
			t.Errorf("marshal %q (%d chars): %v", want, len(want), err)
			continue
		}
		var got rec
		if err = json.Unmarshal(b, &got); err != nil {
			t.Errorf("%q marshals to %s, unmarshal: %v", want, b, err)
		} else if got.N != want {
			t.Errorf("%q (%d chars) marshals to %s, unmarshals as %q", want, len(want), b, got.N)
		}

		got.N = ""
		if b, err = want.MarshalText(); err != nil {
			t.Errorf("MarshalText %q (%d chars): %v", want, len(want), err)
		} else if err = got.N.UnmarshalText(b); err != nil {
			t.Errorf("UnmarshalText(%q): %v", b, err)
		} else if got.N != want {
			t.Errorf("%q as text is %q, parsed back as %q", want, b, got.N)
		}
	}

	// One character more is refused, in both directions.
	tooLong := longest + "345"
	if _, err := tooLong.MarshalText(); !errors.Is(err, godror.ErrNumberTooLong) {
		t.Errorf("MarshalText of %d chars: got %v, wanted ErrNumberTooLong", len(tooLong), err)
	}
	if _, err := json.Marshal(tooLong); !errors.Is(err, godror.ErrNumberTooLong) {
		t.Errorf("json.Marshal of %d chars: got %v, wanted ErrNumberTooLong", len(tooLong), err)
	}
	n := godror.Number("7")
	if err := n.UnmarshalText([]byte(tooLong)); !errors.Is(err, godror.ErrNumberTooLong) {
		t.Errorf("UnmarshalText of %d chars: got %v (%q), wanted ErrNumberTooLong", len(tooLong), err, n)
	}
	if err := json.Unmarshal([]byte(`"`+tooLong+`"`), &n); !errors.Is(err, godror.ErrNumberTooLong) {
		t.Errorf("json.Unmarshal of %d chars: got %v (%q), wanted ErrNumberTooLong", len(tooLong), err, n)
	}
}
