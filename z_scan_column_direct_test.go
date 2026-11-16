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

// TestScanNullAfterValue checks that a NULL scanned into an sql.Null* type
// leaves the zero value in it, not the value of an earlier row.
// The result set must be longer than the fetch array for the buffer to be reused.
func TestScanNullAfterValue(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const qry = `SELECT CAST(4200 + n AS NUMBER(9)), 4200.5 + n, NVL2(n, 'x' || n, NULL), DATE '2026-01-01' + n
		FROM (SELECT CASE WHEN MOD(level, 3) = 1 THEN level END n FROM DUAL CONNECT BY level <= 250)`
	for _, second := range []bool{false, true} {
		rows, err := testDb.QueryContext(ctx, qry, godror.FetchArraySize(100))
		if err != nil {
			t.Fatal(err)
		}
		var bad, n int
		for rows.Next() {
			n++
			var (
				i   sql.NullInt64
				is  sql.NullString
				num godror.Number
				ns  sql.NullString
				s   sql.NullString
				tm  sql.NullTime
			)
			if second {
				err = rows.Scan(&is, &ns, &s, &tm)
			} else {
				err = rows.Scan(&i, &num, &s, &tm)
			}
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if n%3 == 1 {
				continue
			}
			if i.Valid || is.Valid || ns.Valid || tm.Valid ||
				i.Int64 != 0 || is.String != "" || num != "" || ns.String != "" || s.String != "" || !tm.Time.IsZero() {
				if bad++; bad == 1 {
					t.Errorf("row %d: NULLs scanned as %+v %+v %q %+v %+v %+v", n, i, is, num, ns, s, tm)
				}
			}
		}
		if err = rows.Close(); err != nil {
			t.Fatal(err)
		}
		if bad != 0 {
			t.Errorf("%d of %d rows have a stale value in a NULL", bad, n)
		}
	}
}
