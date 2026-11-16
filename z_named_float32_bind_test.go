// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"
)

type (
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
