// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Oracle has no year 0: year -1 (1 BC) is followed by year 1.
// Go uses astronomical year numbering, where year 0 is 1 BC.
func TestBCYear(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	// A date written by SQL: the Ides of March, 44 BC.
	const qry = "SELECT DATE '-0044-03-15' FROM DUAL"
	var tm time.Time
	var a any
	if err := testDb.QueryRowContext(ctx, qry).Scan(&tm); err != nil {
		t.Fatal(err)
	}
	if err := testDb.QueryRowContext(ctx, qry).Scan(&a); err != nil {
		t.Fatal(err)
	}
	if got := tm.Year(); got != -43 {
		t.Errorf("%s: got year %d, wanted -43", qry, got)
	}
	if got := a.(time.Time).Year(); got != -43 {
		t.Errorf("%s into any: got year %d, wanted -43", qry, got)
	}

	// Write, then read.
	for _, tC := range []struct {
		year int
		ora  string
	}{
		{2020, " 2020"}, {1, " 0001"}, {0, "-0001"}, {-1, "-0002"}, {-43, "-0044"}, {-4711, "-4712"},
	} {
		want := time.Date(tC.year, 3, 15, 12, 0, 0, 0, time.UTC)
		var s string
		var a any
		if err := testDb.QueryRowContext(ctx,
			"SELECT TO_CHAR(CAST(:1 AS DATE), 'SYYYY'), CAST(:2 AS DATE), CAST(:3 AS DATE) FROM DUAL",
			want, want, want,
		).Scan(&s, &tm, &a); err != nil {
			t.Errorf("%d: %v", tC.year, err)
			continue
		}
		if s != tC.ora {
			t.Errorf("%d: Oracle has year %q, wanted %q", tC.year, s, tC.ora)
		}
		if got := tm.Year(); got != tC.year {
			t.Errorf("%d: SELECT got back year %d", tC.year, got)
		}
		if got := a.(time.Time).Year(); got != tC.year {
			t.Errorf("%d: SELECT into any got back year %d", tC.year, got)
		}
		var out time.Time
		if _, err := testDb.ExecContext(ctx, "BEGIN :1 := :2; END;", sql.Out{Dest: &out}, want); err != nil {
			t.Errorf("%d: %v", tC.year, err)
		} else if got := out.Year(); got != tC.year {
			t.Errorf("%d: OUT bind got back year %d", tC.year, got)
		}
		var d godror.Data
		d.SetTime(want)
		if got := d.GetTimeIn(time.UTC).Year(); got != tC.year {
			t.Errorf("%d: Data.GetTime got back year %d", tC.year, got)
		}
	}
}
