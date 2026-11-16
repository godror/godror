// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestTSLTZRead checks that a TIMESTAMP WITH LOCAL TIME ZONE column is read
// as the stored instant, whatever the session time zone is (issue #495).
// "" is the session time zone the client starts with.
func TestTSLTZRead(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	// A session of its own: the pool must not get a session with an altered time zone.
	P.StandaloneConnection = godror.Bool(true)
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_tsltz_read" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(3), ltz TIMESTAMP(9) WITH LOCAL TIME ZONE)",
	); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	want := []time.Time{
		time.Date(2021, 7, 1, 12, 34, 56, 123456789, time.UTC),
		time.Date(2021, 1, 15, 12, 34, 56, 123456789, time.UTC),
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+` (id, ltz)
		SELECT 0, TIMESTAMP '2021-07-01 12:34:56.123456789 +00:00' FROM DUAL UNION ALL
		SELECT 1, TIMESTAMP '2021-01-15 12:34:56.123456789 +00:00' FROM DUAL`,
	); err != nil {
		t.Fatal(err)
	}

	for _, sessTZ := range []string{"", "+00:00", "-07:00", "+05:30", "Europe/Budapest", "America/New_York"} {
		if sessTZ != "" {
			if _, err = conn.ExecContext(ctx, "ALTER SESSION SET TIME_ZONE = '"+sessTZ+"'"); err != nil {
				t.Fatalf("%s: %v", sessTZ, err)
			}
		}
		rows, err := conn.QueryContext(ctx,
			"SELECT ltz, TO_CHAR(ltz, 'YYYY-MM-DD HH24:MI:SS') FROM "+tbl+" ORDER BY id")
		if err != nil {
			t.Fatalf("TIME_ZONE=%q: %v", sessTZ, err)
		}
		var n int
		for rows.Next() {
			var got time.Time
			var shown string
			if err = rows.Scan(&got, &shown); err != nil {
				t.Fatalf("TIME_ZONE=%q: %v", sessTZ, err)
			}
			t.Logf("TIME_ZONE=%q: session shows %s, got %s", sessTZ, shown, got.Format(time.RFC3339Nano))
			if !got.Equal(want[n]) {
				t.Errorf("TIME_ZONE=%q: got %s, wanted %s", sessTZ,
					got.Format(time.RFC3339Nano), want[n].Format(time.RFC3339Nano))
			}
			n++
		}
		if err = rows.Close(); err != nil {
			t.Errorf("TIME_ZONE=%q: %v", sessTZ, err)
		}
		if n != len(want) {
			t.Errorf("TIME_ZONE=%q: got %d rows, wanted %d", sessTZ, n, len(want))
		}
	}
}
