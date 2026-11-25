// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestBindTimeGregorianGap binds times on 1582-10-05 .. 1582-10-14, the days
// that Oracle's calendar does not have, as DATE.
func TestBindTimeGregorianGap(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	// godror writes the wall clock of a time.Time in the connection's time
	// zone into a DATE, so pin that zone to UTC: with a session east of UTC,
	// 1582-10-14 23:59:59 UTC falls on the existing 1582-10-15, and there is
	// nothing to refuse.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.Timezone = time.UTC
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl, objName := "test_gregorian_gap"+tblSuffix, "test_gregorian_gap_o"+tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	for _, qry := range []string{
		"CREATE TABLE " + tbl + " (id NUMBER(6), c DATE)",
		"CREATE OR REPLACE TYPE " + objName + " AS OBJECT (d DATE)",
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	defer func() {
		testDb.Exec("DROP TABLE " + tbl + " PURGE")
		testDb.Exec("DROP TYPE " + objName + " FORCE")
	}()
	objType, err := godror.GetObjectType(ctx, conn, objName)
	if err != nil {
		t.Fatal(err)
	}
	defer objType.Close()

	ins := "INSERT INTO " + tbl + " (id, c) VALUES (:1, :2)"
	const arrQry = `DECLARE
  TYPE date_tab_typ IS TABLE OF DATE INDEX BY PLS_INTEGER;
  v date_tab_typ := :1;
BEGIN
  :2 := TO_CHAR(v(1), 'YYYY-MM-DD');
END;`

	for i, tc := range []struct {
		tm    time.Time
		valid bool
	}{
		{time.Date(1582, 10, 4, 12, 34, 56, 0, time.UTC), true},
		{time.Date(1582, 10, 5, 0, 0, 0, 0, time.UTC), false},
		{time.Date(1582, 10, 10, 12, 34, 56, 0, time.UTC), false},
		{time.Date(1582, 10, 14, 23, 59, 59, 0, time.UTC), false},
		{time.Date(1582, 10, 15, 0, 0, 0, 0, time.UTC), true},
	} {
		check := func(what string, err error) {
			if tc.valid && err != nil {
				t.Errorf("%s %v: %+v", what, tc.tm, err)
			} else if !tc.valid && !errors.Is(err, godror.ErrBadDate) {
				t.Errorf("%s %v: got %v, wanted ErrBadDate", what, tc.tm, err)
			}
		}
		_, err := conn.ExecContext(ctx, ins, 10*i, tc.tm, godror.BindTimeAsDate())
		check("BindTimeAsDate", err)
		_, err = conn.ExecContext(ctx, ins, 10*i+1, tc.tm)
		check("default bind", err)
		var s string
		_, err = conn.ExecContext(ctx, arrQry, []time.Time{tc.tm}, sql.Out{Dest: &s}, godror.PlSQLArrays)
		check("PL/SQL array", err)
		obj, err := objType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		check("object attribute", obj.Set("D", tc.tm))
		obj.Close()
	}

	// Nothing invalid has been stored.
	var n int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+tbl+" WHERE TO_CHAR(c, 'YYYY-MM-DD') NOT IN ('1582-10-04', '1582-10-15')",
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d invalid dates have been stored", n)
	}
}
