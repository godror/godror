// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

//go:build go1.27

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestScanColumnMatchesConvertAssign(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_scancol" + tblSuffix
	_, _ = testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+
		" (id NUMBER(3), vc VARCHAR2(80), ch CHAR(3), ni NUMBER(15), nd NUMBER(18,2), nf NUMBER, dt DATE, ts TIMESTAMP, bd BINARY_DOUBLE)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	for _, qry := range []string{
		"INSERT INTO " + tbl + " (id) VALUES (1)",
		"INSERT INTO " + tbl + " VALUES (2, 'árvíztűrő tükörfúrógép', 'ab', -123456789012345, -0.5, 3.14159265358979323846, DATE '2026-03-08', TIMESTAMP '2026-11-01 23:59:59.123456', 1.5)",
		"INSERT INTO " + tbl + " VALUES (3, 'x', 'abc', 0, 12345678.9, 42, DATE '0001-01-01', TIMESTAMP '9999-12-31 00:00:00', -2)",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(err)
		}
	}

	dests := map[string]func() any{
		"string":         func() any { return new(string) },
		"sql.NullString": func() any { return new(sql.NullString) },
		"int64":          func() any { return new(int64) },
		"int":            func() any { return new(int) },
		"sql.NullInt64":  func() any { return new(sql.NullInt64) },
		"godror.Number":  func() any { return new(godror.Number) },
		"time.Time":      func() any { return new(time.Time) },
		"sql.NullTime":   func() any { return new(sql.NullTime) },
		"float64":        func() any { return new(float64) },
	}

	rows, err := testDb.QueryContext(ctx, "SELECT * FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]any, len(columns))
	targets := make([]any, len(columns))
	for rows.Next() {
		for i := range raw {
			targets[i] = &raw[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		for i, column := range columns {
			for name, newDest := range dests {
				got := newDest()
				for j := range targets {
					targets[j] = new(any)
				}
				targets[i] = got
				gotErr := rows.Scan(targets...)

				want := newDest()
				wantErr := sql.ConvertAssign(driver.ScanContext{}, want, raw[i])

				if (gotErr == nil) != (wantErr == nil) {
					t.Errorf("row %v %s into %s: got error %v, want error %v", raw[0], column, name, gotErr, wantErr)
					continue
				}
				if gotErr == nil && !reflect.DeepEqual(got, want) {
					t.Errorf("row %v %s into %s: got %#v, want %#v", raw[0], column, name, got, want)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
