// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"
)

// TestIntervalDSOverflow checks that an INTERVAL DAY TO SECOND that does not
// fit in time.Duration (about 292 years) gives an error instead of a
// silently wrapped value.
func TestIntervalDSOverflow(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	const tooLong = "TIMESTAMP '2026-01-01 00:00:00' - TIMESTAMP '1700-01-01 00:00:00'"

	for _, tc := range []struct {
		expr string
		want time.Duration
	}{
		{"TIMESTAMP '2026-01-02 03:04:05.5' - TIMESTAMP '2026-01-01 00:00:00'", 27*time.Hour + 4*time.Minute + 5500*time.Millisecond},
		{"TIMESTAMP '2026-01-01 00:00:00' - TIMESTAMP '2026-01-02 03:04:05.5'", -(27*time.Hour + 4*time.Minute + 5500*time.Millisecond)},
		{"TIMESTAMP '2026-01-01 00:00:00' - TIMESTAMP '1800-01-01 00:00:00'", 82545 * 24 * time.Hour},
	} {
		var d time.Duration
		if err := testDb.QueryRowContext(ctx, "SELECT "+tc.expr+" FROM DUAL").Scan(&d); err != nil {
			t.Errorf("%s: %+v", tc.expr, err)
		} else if d != tc.want {
			t.Errorf("%s: got %s, wanted %s", tc.expr, d, tc.want)
		}
	}

	t.Run("query", func(t *testing.T) {
		var d time.Duration
		err := testDb.QueryRowContext(ctx, "SELECT "+tooLong+" FROM DUAL").Scan(&d)
		t.Log(err)
		if err == nil {
			t.Errorf("got %s, wanted an error", d)
		}
	})

	t.Run("out", func(t *testing.T) {
		var d time.Duration
		_, err := testDb.ExecContext(ctx, "BEGIN :1 := "+tooLong+"; END;", sql.Out{Dest: &d})
		t.Log(err)
		if err == nil {
			t.Errorf("got %s, wanted an error", d)
		}
	})
}

// TestIntervalYMString checks the string form of INTERVAL YEAR TO MONTH.
func TestIntervalYMString(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	for _, tc := range []struct{ expr, want string }{
		{"INTERVAL '1-2' YEAR TO MONTH", "1-2"},
		{"INTERVAL '-1-2' YEAR TO MONTH", "-1-2"},
		{"INTERVAL '-0-2' YEAR TO MONTH", "-0-2"},
		{"INTERVAL '-3-0' YEAR TO MONTH", "-3-0"},
		{"INTERVAL '0-0' YEAR TO MONTH", "0-0"},
	} {
		var s string
		if err := testDb.QueryRowContext(ctx, "SELECT "+tc.expr+" FROM DUAL").Scan(&s); err != nil {
			t.Errorf("%s: %+v", tc.expr, err)
		} else if s != tc.want {
			t.Errorf("%s: got %q, wanted %q", tc.expr, s, tc.want)
		}
	}
}
