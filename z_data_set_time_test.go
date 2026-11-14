// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestDataSetTimeBadDate checks that Data.SetTime with a time outside
// Oracle's date range does not panic, and that using that Data later
// gives ErrBadDate instead of writing a value.
func TestDataSetTimeBadDate(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	typName := strings.ToUpper("test_r3a_dst_obj" + tblSuffix)
	collName := strings.ToUpper("test_r3a_dst_tab" + tblSuffix)
	testDb.ExecContext(ctx, "DROP TYPE "+typName)
	testDb.ExecContext(ctx, "DROP TYPE "+collName)
	if _, err := testDb.ExecContext(ctx, "CREATE TYPE "+typName+" AS OBJECT (d DATE)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TYPE " + typName)
	if _, err := testDb.ExecContext(ctx, "CREATE TYPE "+collName+" AS TABLE OF DATE"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TYPE " + collName)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	typ, err := godror.GetObjectType(ctx, conn, typName)
	if err != nil {
		t.Fatal(err)
	}
	defer typ.Close()
	collTyp, err := godror.GetObjectType(ctx, conn, collName)
	if err != nil {
		t.Fatal(err)
	}
	defer collTyp.Close()

	good := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, bad := range []time.Time{
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(-4713, 1, 1, 0, 0, 0, 0, time.UTC), // Oracle year -4714
	} {
		var d godror.Data
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SetTime(%v) panicked: %v", bad, r)
				}
			}()
			d.SetTime(bad)
		}()

		obj, err := typ.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()
		if err := obj.Set("D", good); err != nil {
			t.Fatal(err)
		}
		if err := obj.SetAttribute("D", &d); !errors.Is(err, godror.ErrBadDate) {
			t.Errorf("SetAttribute(%v): got %v, wanted ErrBadDate", bad, err)
		}
		if v, err := obj.Get("D"); err != nil {
			t.Fatal(err)
		} else if got, _ := v.(time.Time); !got.Equal(good) {
			t.Errorf("attribute changed to %v (%T), wanted %v", v, v, good)
		}

		coll, err := collTyp.NewCollection()
		if err != nil {
			t.Fatal(err)
		}
		defer coll.Close()
		if err := coll.AppendData(&d); !errors.Is(err, godror.ErrBadDate) {
			t.Errorf("AppendData(%v): got %v, wanted ErrBadDate", bad, err)
		}
		if err := coll.Append(good); err != nil {
			t.Fatal(err)
		}
		if err := coll.SetItem(0, &d); !errors.Is(err, godror.ErrBadDate) {
			t.Errorf("SetItem(%v): got %v, wanted ErrBadDate", bad, err)
		}
		if n, err := coll.Len(); err != nil {
			t.Fatal(err)
		} else if n != 1 {
			t.Errorf("collection has %d elements, wanted 1", n)
		}

		// A later valid value clears the error.
		d.SetTime(good)
		if err := obj.SetAttribute("D", &d); err != nil {
			t.Errorf("SetAttribute(%v) after SetTime(%v): %v", good, bad, err)
		}
	}
}
