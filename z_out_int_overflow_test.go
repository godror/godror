// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestOutIntOverflow(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	tbl := "test_ovf" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(10))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (:1)", []int{1, 100, 300, 70000}); err != nil {
		t.Fatal(err)
	}
	const returning = " SET id=id WHERE id <= :1 RETURNING id INTO :2"

	// Values that fit.
	var (
		i8   int8
		i16  int16
		i32  int32
		u8   uint8
		u16  uint16
		u32  uint32
		n32  sql.NullInt32
		i8s  []int8
		u16s []uint16
	)
	if _, err := testDb.ExecContext(ctx,
		"BEGIN :1 := -128; :2 := -32768; :3 := -2147483648; :4 := 255; :5 := 65535; :6 := 4294967295; :7 := 2147483647; END;",
		sql.Out{Dest: &i8}, sql.Out{Dest: &i16}, sql.Out{Dest: &i32},
		sql.Out{Dest: &u8}, sql.Out{Dest: &u16}, sql.Out{Dest: &u32},
		sql.Out{Dest: &n32},
	); err != nil {
		t.Fatal(err)
	}
	if i8 != -128 || i16 != -32768 || i32 != -2147483648 ||
		u8 != 255 || u16 != 65535 || u32 != 4294967295 ||
		n32 != (sql.NullInt32{Int32: 2147483647, Valid: true}) {
		t.Errorf("got %d, %d, %d, %d, %d, %d, %+v", i8, i16, i32, u8, u16, u32, n32)
	}
	if _, err := testDb.ExecContext(ctx, "UPDATE "+tbl+returning, 100, sql.Out{Dest: &i8s}); err != nil {
		t.Fatal(err)
	} else if len(i8s) != 2 || i8s[0]+i8s[1] != 101 {
		t.Errorf("got %v, wanted 1 and 100", i8s)
	}

	// Values that do not fit.
	for _, tC := range []struct {
		dest any
		val  int64
	}{
		{&i8, 300}, {&i8, -129},
		{&i16, 32768},
		{&i32, 5000000000}, {&i32, -2147483649},
		{&u8, 256},
		{&u16, 70000},
		{&u32, 4294967296},
		{&n32, 2147483648},
		{&i8s, 300},
		{&u16s, 70000},
	} {
		qry, args := "BEGIN :1 := "+strconv.FormatInt(tC.val, 10)+"; END;", []any{sql.Out{Dest: tC.dest}}
		switch tC.dest.(type) {
		case *[]int8, *[]uint16:
			qry, args = "UPDATE "+tbl+returning, []any{tC.val, sql.Out{Dest: tC.dest}}
		}
		if _, err := testDb.ExecContext(ctx, qry, args...); !errors.Is(err, strconv.ErrRange) {
			t.Errorf("OUT %d into %T: got %v, wanted strconv.ErrRange", tC.val, tC.dest, err)
		}
	}
}
