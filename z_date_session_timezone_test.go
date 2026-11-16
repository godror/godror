// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

// TestOutTimestampTZOffset checks that a TIMESTAMP WITH (LOCAL) TIME ZONE
// value read through an OUT bind is the instant the server has sent:
// its offset must be obeyed, whatever the connection's time zone is.
func TestOutTimestampTZOffset(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)

	// Sessions of their own, as in TestDateSessionTimezone.
	setup := sql.OpenDB(godror.NewConnector(P))
	defer setup.Close()
	tbl := "test_out_tstz" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err = setup.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (tstz TIMESTAMP WITH TIME ZONE, tsltz TIMESTAMP WITH LOCAL TIME ZONE)",
	); err != nil {
		t.Fatal(err)
	}
	defer setup.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	for _, sessTZ := range []string{"", "-07:00", "+05:45"} {
		func() {
			P := P
			if sessTZ != "" {
				P.SetSessionParamOnInit("TIME_ZONE", sessTZ)
			}
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			prefix := "TIME_ZONE=" + sessTZ

			for _, tC := range []struct {
				Lit  string
				Want time.Time
			}{
				{"2021-07-01 12:34:56 +00:00", time.Date(2021, 7, 1, 12, 34, 56, 0, time.UTC)},
				{"2021-01-15 12:34:56 +00:00", time.Date(2021, 1, 15, 12, 34, 56, 0, time.UTC)},
				{"2021-07-01 12:34:56 +01:00", time.Date(2021, 7, 1, 11, 34, 56, 0, time.UTC)},
				{"2021-01-15 12:34:56 +02:00", time.Date(2021, 1, 15, 10, 34, 56, 0, time.UTC)},
				{"2021-07-01 12:34:56 +09:00", time.Date(2021, 7, 1, 3, 34, 56, 0, time.UTC)},
				{"2021-07-01 12:34:56 -07:00", time.Date(2021, 7, 1, 19, 34, 56, 0, time.UTC)},
				{"2021-07-01 12:34:56 +05:45", time.Date(2021, 7, 1, 6, 49, 56, 0, time.UTC)},
			} {
				var got time.Time
				if _, err = conn.ExecContext(ctx,
					"BEGIN :1 := TIMESTAMP '"+tC.Lit+"'; END;", sql.Out{Dest: &got},
				); err != nil {
					t.Fatalf("%s: %s: %v", prefix, tC.Lit, err)
				}
				if !got.Equal(tC.Want) {
					t.Errorf("%s: %s through sql.Out is %s", prefix, tC.Lit, got.Format(time.RFC3339))
				}

				var tstz, tsltz sql.NullTime
				if _, err = conn.ExecContext(ctx,
					"INSERT INTO "+tbl+" (tstz, tsltz) VALUES (TIMESTAMP '"+tC.Lit+"', TIMESTAMP '"+tC.Lit+"')"+
						" RETURNING tstz, tsltz INTO :1, :2",
					sql.Out{Dest: &tstz}, sql.Out{Dest: &tsltz},
				); err != nil {
					t.Fatalf("%s: %s: %v", prefix, tC.Lit, err)
				}
				if !tstz.Time.Equal(tC.Want) {
					t.Errorf("%s: %s RETURNING a TIMESTAMP WITH TIME ZONE is %s", prefix, tC.Lit, tstz.Time.Format(time.RFC3339))
				}
				if !tsltz.Time.Equal(tC.Want) {
					t.Errorf("%s: %s RETURNING a TIMESTAMP WITH LOCAL TIME ZONE is %s", prefix, tC.Lit, tsltz.Time.Format(time.RFC3339))
				}
			}
		}()
	}
}

// TestSessionTimezoneOnInitFunc checks that a session whose time zone is set
// by an OnInit function gets its own zone, not the cached one of another
// sql.DB of the same database, and that it does not leave its zone for others.
func TestSessionTimezoneOnInitFunc(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	plain, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	plain.StandaloneConnection = godror.Bool(true)
	withFunc := plain
	withFunc.OnInit = func(ctx context.Context, c driver.ConnPrepareContext) error {
		st, err := c.PrepareContext(ctx, "ALTER SESSION SET TIME_ZONE='-07:00'")
		if err != nil {
			return err
		}
		defer st.Close()
		_, err = st.(driver.StmtExecContext).ExecContext(ctx, nil)
		return err
	}

	type namedParams struct {
		Name string
		P    godror.ConnectionParams
	}
	A, B := namedParams{"plain", plain}, namedParams{"OnInit func", withFunc}
	for _, order := range [][]namedParams{{A, B}, {B, A}} {
		func() {
			// A driver of its own has an empty time zone cache.
			drv := godror.NewDriver()
			defer drv.Close()
			for i, x := range order {
				db := sql.OpenDB(drv.NewConnector(x.P))
				defer db.Close()
				// CURRENT_DATE is the wall clock in the session time zone.
				var d, ts time.Time
				if err := db.QueryRowContext(ctx,
					"SELECT CURRENT_DATE, CURRENT_TIMESTAMP FROM DUAL",
				).Scan(&d, &ts); err != nil {
					t.Fatal(err)
				}
				if diff := d.Sub(ts).Round(time.Minute); diff != 0 {
					t.Errorf("%s (connected as %d.): CURRENT_DATE is read as %s, %s off from CURRENT_TIMESTAMP",
						x.Name, i+1, d.Format(time.RFC3339), diff)
				}
			}
		}()
	}
}
