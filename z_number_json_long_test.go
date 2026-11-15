// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"encoding/json"
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
