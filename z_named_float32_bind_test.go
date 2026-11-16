// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"
)

type (
	namedInt8    int8
	namedFloat32 float32
	namedFloat64 float64
	namedInt32   int32
	namedUint16  uint16
)

// TestNamedFloat32Bind checks that a value of a named float32 type
// is bound as the same value of float32 is.
func TestNamedFloat32Bind(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	var nilF32 *namedFloat32
	f32, f64, i32, u16 := namedFloat32(1.5), namedFloat64(-2.25), namedInt32(-42), namedUint16(42)
	for _, tC := range []struct {
		name        string
		named, base any
	}{
		{"float32", f32, float32(1.5)},
		{"*float32", &f32, float32(1.5)},
		{"nil *float32", nilF32, nil},
		{"float64", f64, float64(-2.25)},
		{"*float64", &f64, float64(-2.25)},
		{"int32", i32, int32(-42)},
		{"*int32", &i32, int32(-42)},
		{"uint16", u16, uint16(42)},
	} {
		// DUMP shows the bytes the database got.
		const qry = "SELECT DUMP(:1) FROM DUAL"
		var got, want string
		if err := testDb.QueryRowContext(ctx, qry, tC.named).Scan(&got); err != nil {
			t.Errorf("%s: %T: %+v", tC.name, tC.named, err)
			continue
		}
		if err := testDb.QueryRowContext(ctx, qry, tC.base).Scan(&want); err != nil {
			t.Fatalf("%s: %T: %+v", tC.name, tC.base, err)
		}
		if got != want {
			t.Errorf("%s: %T was bound as %q, %T as %q", tC.name, tC.named, got, tC.base, want)
		}
	}

}

// TestNamedNumberOut checks that a named number type can be an OUT bind.
func TestNamedNumberOut(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const qry = "BEGIN :1 := :1 * 2; END;"
	f32, f64, i32, u16 := namedFloat32(1.5), namedFloat64(-2.25), namedInt32(-42), namedUint16(42)
	for _, tC := range []struct {
		dest any
		want any
	}{
		{&f32, namedFloat32(3)},
		{&f64, namedFloat64(-4.5)},
		{&i32, namedInt32(-84)},
		{&u16, namedUint16(84)},
	} {
		if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: tC.dest, In: true}); err != nil {
			t.Errorf("%T: %+v", tC.dest, err)
			continue
		}
		var got any
		switch x := tC.dest.(type) {
		case *namedFloat32:
			got = *x
		case *namedFloat64:
			got = *x
		case *namedInt32:
			got = *x
		case *namedUint16:
			got = *x
		}
		if got != tC.want {
			t.Errorf("%T: got %v, wanted %v", tC.dest, got, tC.want)
		}
	}

	// A value that does not fit is an error, as for the base type.
	i8 := namedInt8(100)
	if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: &i8, In: true}); !errors.Is(err, strconv.ErrRange) {
		t.Errorf("%T: got %d with %v, wanted strconv.ErrRange", i8, i8, err)
	}
}
