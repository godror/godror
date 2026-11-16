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

// TestDateSessionTimezone checks that DATE values are written and read as
// the wall clock in the session time zone, as the server itself converts
// between DATE and TIMESTAMP WITH TIME ZONE (issue #495);
// and that the timezone connection parameter overrides this.
func TestDateSessionTimezone(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	// Sessions of their own: the pool must not get a session with an altered time zone.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	setup := sql.OpenDB(godror.NewConnector(P))
	defer setup.Close()

	tbl := "test_date_sesstz" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err = setup.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(3), d DATE, tstz TIMESTAMP WITH TIME ZONE)",
	); err != nil {
		t.Fatal(err)
	}
	defer setup.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	budapest, err := time.LoadLocation("Europe/Budapest")
	if err != nil {
		t.Fatal(err)
	}
	const layout = "2006-01-02 15:04:05"
	summer := time.Date(2021, 7, 1, 12, 34, 56, 0, time.UTC)
	winter := time.Date(2021, 1, 15, 12, 34, 56, 0, time.UTC)
	var id int
	for _, tC := range []struct {
		SessTZ string         // ALTER SESSION SET TIME_ZONE on session init
		ConnTZ *time.Location // the timezone connection parameter
		Want   *time.Location // the zone of DATE values
	}{
		{SessTZ: "-07:00", Want: time.FixedZone("", -7*3600)},
		{SessTZ: "+05:30", Want: time.FixedZone("", 5*3600+1800)},
		{SessTZ: "Europe/Budapest", Want: budapest},
		{SessTZ: "-07:00", ConnTZ: time.FixedZone("+03:00", 3*3600), Want: time.FixedZone("", 3*3600)},
	} {
		func() {
			P := P
			P.Timezone = tC.ConnTZ
			P.SetSessionParamOnInit("TIME_ZONE", tC.SessTZ)
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			prefix := "TIME_ZONE=" + tC.SessTZ
			if tC.ConnTZ != nil {
				prefix += " timezone=" + tC.ConnTZ.String()
			}

			for _, v := range []time.Time{summer, winter} {
				id++
				wantWall := v.In(tC.Want).Format(layout)

				// Written by the default bind: the wall clock in the wanted zone.
				if _, err = conn.ExecContext(ctx,
					"INSERT INTO "+tbl+" (id, d) VALUES (:1, :2)", id, v,
				); err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				var stored string
				var got time.Time
				if err = conn.QueryRowContext(ctx,
					"SELECT TO_CHAR(d, 'YYYY-MM-DD HH24:MI:SS'), d FROM "+tbl+" WHERE id = :1", id,
				).Scan(&stored, &got); err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				if stored != wantWall {
					t.Errorf("%s: %s is stored in DATE as %s, wanted %s", prefix, v.Format(layout), stored, wantWall)
				}
				if !got.Equal(v) {
					t.Errorf("%s: DATE read back as %s, wanted %s", prefix, got, v)
				}
				if tC.ConnTZ != nil {
					continue // the server does not know about the timezone parameter
				}

				// Written as DATE: the server converts it to the same instant.
				id++
				stmt, err := conn.PrepareContext(ctx, "INSERT INTO "+tbl+" (id, tstz) VALUES (:1, :2)")
				if err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				_, err = stmt.ExecContext(ctx, id, v, godror.BindTimeAsDate())
				stmt.Close()
				if err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				if err = conn.QueryRowContext(ctx,
					"SELECT TO_CHAR(SYS_EXTRACT_UTC(tstz), 'YYYY-MM-DD HH24:MI:SS') FROM "+tbl+" WHERE id = :1", id,
				).Scan(&stored); err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				if want := v.Format(layout); stored != want {
					t.Errorf("%s: %s is stored by BindTimeAsDate in TIMESTAMP WITH TIME ZONE as %s UTC", prefix, want, stored)
				}

				// A DATE that the server made from the instant is read as that instant.
				if err = conn.QueryRowContext(ctx,
					"SELECT CAST(CAST(:1 AS TIMESTAMP WITH LOCAL TIME ZONE) AS DATE) FROM DUAL", v,
				).Scan(&got); err != nil {
					t.Fatalf("%s: %v", prefix, err)
				}
				if !got.Equal(v) {
					t.Errorf("%s: the server's DATE is read as %s, wanted %s", prefix, got, v)
				}
			}
		}()
	}
}
