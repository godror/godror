// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

type (
	nilPtrFloat64 float64
	nilPtrFloat32 float32
	nilPtrInt     int
	nilPtrValuer  struct{ n int64 }
	nilPtrPValuer struct{ n int64 }
)

func (v nilPtrValuer) Value() (driver.Value, error)   { return v.n, nil }
func (v *nilPtrPValuer) Value() (driver.Value, error) { return v.n, nil }

// A nil pointer is NULL, as in database/sql.
func TestNilPointerValuerNull(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	f64, i := nilPtrFloat64(25), nilPtrInt(25)
	for _, tC := range []struct {
		in   any
		want string
	}{
		{(*nilPtrFloat64)(nil), ""},
		{(*nilPtrFloat32)(nil), ""},
		{(*nilPtrInt)(nil), ""},
		{(*float64)(nil), ""},
		{(*nilPtrValuer)(nil), ""},
		{(*nilPtrPValuer)(nil), ""},

		{&f64, "25"},
		{&i, "25"},
		{f64, "25"},
		{&nilPtrValuer{n: 25}, "25"},
		{nilPtrValuer{n: 25}, "25"},
		{nilPtrValuer{}, "0"},
		{&nilPtrPValuer{n: 25}, "25"},
	} {
		var got sql.NullString
		if err := testDb.QueryRowContext(ctx, "SELECT TO_CHAR(TO_NUMBER(:1)) FROM DUAL", tC.in).Scan(&got); err != nil {
			t.Errorf("%T(%v): %v", tC.in, tC.in, err)
		} else if got.String != tC.want {
			t.Errorf("%T(%v): got %q, wanted %q", tC.in, tC.in, got.String, tC.want)
		}
	}
}
