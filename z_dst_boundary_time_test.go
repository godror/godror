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

// TestBindTimeAtZoneBoundary checks that a time exactly at a DST end at
// 01:00 local keeps its instant, while a local midnight that falls into a
// DST gap (as read from a DATE) is still bound as 00:00.
func TestBindTimeAtZoneBoundary(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const utcQry = `SELECT TO_CHAR(SYS_EXTRACT_UTC(CAST(:1 AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS') FROM DUAL`
	const dateQry = `SELECT TO_CHAR(CAST(:1 AS DATE), 'YYYY-MM-DD HH24:MI:SS') FROM DUAL`
	for _, tC := range []struct {
		zone, qry string
		time      func(*time.Location) time.Time
		want      string
	}{
		// DST ends: 02:00 BST -> 01:00 GMT
		{zone: "Europe/London", qry: utcQry,
			time: func(*time.Location) time.Time { return time.Date(2025, 10, 26, 1, 0, 0, 0, time.UTC) },
			want: "2025-10-26 01:00:00"},
		// DST ends: 02:00 EDT -> 01:00 EST
		{zone: "America/New_York", qry: utcQry,
			time: func(*time.Location) time.Time { return time.Date(2025, 11, 2, 6, 0, 0, 0, time.UTC) },
			want: "2025-11-02 06:00:00"},
		// DST started at midnight: time.Date gives 01:00 CEST.
		{zone: "Europe/Budapest", qry: dateQry,
			time: func(loc *time.Location) time.Time { return time.Date(1980, 4, 6, 0, 0, 0, 0, loc) },
			want: "1980-04-06 00:00:00"},
	} {
		t.Run(tC.zone, func(t *testing.T) {
			loc, err := time.LoadLocation(tC.zone)
			if err != nil {
				t.Skip(err)
			}
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.Timezone = loc
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()

			in := tC.time(loc)
			var got string
			if err := db.QueryRowContext(ctx, tC.qry, in).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != tC.want {
				t.Errorf("%s: got %s, wanted %s", in.Format(time.RFC3339), got, tC.want)
			}
		})
	}
}
