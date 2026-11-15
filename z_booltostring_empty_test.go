// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestBoolToStringEmpty binds bools with a BoolToString that maps one of the
// values to the empty string, which is NULL in Oracle.
func TestBoolToStringEmpty(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_bts_empty" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(3), flag VARCHAR2(1))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	qry := "INSERT INTO " + tbl + " (id, flag) VALUES (:1, :2) RETURNING flag INTO :3"
	for i, tC := range []struct {
		trueVal, falseVal string
	}{
		{"Y", ""}, {"", "N"},
	} {
		b2s := godror.BoolToString(tC.trueVal, tC.falseVal)
		// A panic here would leave the statement and the connection locked.
		for j, b := range []bool{true, false} {
			got := !b
			if _, err := testDb.ExecContext(ctx, qry, 10*i+j, b, sql.Out{Dest: &got}, b2s); err != nil {
				t.Fatalf("%q: %v: %+v", tC, b, err)
			}
			if got != b {
				t.Errorf("%q: bound %t, got back %t", tC, b, got)
			}
		}
		in := []bool{false, true, false}
		if _, err := testDb.ExecContext(ctx,
			"INSERT INTO "+tbl+" (id, flag) VALUES (:1, :2)",
			[]int{10*i + 2, 10*i + 3, 10*i + 4}, in, b2s,
		); err != nil {
			t.Fatalf("%q: %v: %+v", tC, in, err)
		}
	}

	rows, err := testDb.QueryContext(ctx, "SELECT flag FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s sql.NullString
		if err = rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s.String)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"Y", "", "", "Y", "", "", "N", "N", "", "N"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got flags %q, wanted %q", got, want)
	}
}
