// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestNumberNoDigit checks that a Number without a digit is refused,
// not stored as 0.
func TestNumberNoDigit(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const qry = "SELECT TO_CHAR(:1) FROM DUAL"
	for _, text := range []string{"-", ".", "-.", "e5", "-e5", ".e5"} {
		var n godror.Number
		if err := n.UnmarshalText([]byte(text)); !errors.Is(err, godror.ErrInvalidNumber) {
			t.Errorf("UnmarshalText(%q): got %q with %v, wanted ErrInvalidNumber", text, n, err)
		}
		if err := json.Unmarshal([]byte(`"`+text+`"`), &n); !errors.Is(err, godror.ErrInvalidNumber) {
			t.Errorf("UnmarshalJSON(%q): got %q with %v, wanted ErrInvalidNumber", text, n, err)
		}

		var got sql.NullString
		if err := testDb.QueryRowContext(ctx, qry, godror.Number(text)).Scan(&got); !errors.Is(err, godror.ErrInvalidNumber) {
			t.Errorf("Number(%q): the database got %+v, error %v, wanted ErrInvalidNumber", text, got, err)
		}
		// in a slice, with executeMany
		if _, err := testDb.ExecContext(ctx, "BEGIN IF :1 IS NULL THEN NULL; END IF; END;",
			[]godror.Number{"1", godror.Number(text)},
		); !errors.Is(err, godror.ErrInvalidNumber) {
			t.Errorf("[]Number{1, %q}: got %v, wanted ErrInvalidNumber", text, err)
		}
	}

	// The neighbours are kept: the empty Number is NULL, a number is itself.
	for _, tC := range []struct{ text, want string }{
		{"", ""}, {"0", "0"}, {"-0", "0"}, {".5", ".5"}, {"5.", "5"}, {"-.5", "-.5"}, {"0e5", "0"}, {"1e2", "100"},
	} {
		var n godror.Number
		if err := n.UnmarshalText([]byte(tC.text)); err != nil {
			t.Errorf("UnmarshalText(%q): %+v", tC.text, err)
		}
		for _, n := range []godror.Number{n, godror.Number(tC.text)} {
			var got sql.NullString
			if err := testDb.QueryRowContext(ctx, qry, n).Scan(&got); err != nil {
				t.Errorf("Number(%q): %+v", n, err)
			} else if got.String != tC.want {
				t.Errorf("Number(%q): the database got %q, wanted %q", n, got.String, tC.want)
			}
		}
	}
}
