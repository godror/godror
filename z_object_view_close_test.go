// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectViewClose checks that closing an Object got from a collection
// element or an object attribute does not change the parent object.
// Such objects point into the parent's memory.
func TestObjectViewClose(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	elemName := "TEST_OVC_T" + tblSuffix
	collName := "TEST_OVC_TT" + tblSuffix
	wrapName := "TEST_OVC_W" + tblSuffix
	cleanup := func() {
		for _, nm := range []string{wrapName, collName, elemName} {
			testDb.ExecContext(context.Background(), "DROP TYPE "+nm)
		}
	}
	cleanup()
	for _, qry := range []string{
		"CREATE TYPE " + elemName + " AS OBJECT (a NUMBER, b VARCHAR2(10))",
		"CREATE TYPE " + collName + " AS TABLE OF " + elemName,
		"CREATE TYPE " + wrapName + " AS OBJECT (x " + elemName + ", items " + collName + ")",
	} {
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	defer cleanup()

	elemType, err := godror.GetObjectType(ctx, conn, elemName)
	if err != nil {
		t.Fatal(err)
	}
	defer elemType.Close()
	collType, err := godror.GetObjectType(ctx, conn, collName)
	if err != nil {
		t.Fatal(err)
	}
	defer collType.Close()
	wrapType, err := godror.GetObjectType(ctx, conn, wrapName)
	if err != nil {
		t.Fatal(err)
	}
	defer wrapType.Close()

	newElem := func(t *testing.T, a int, b string) *godror.Object {
		t.Helper()
		elem, err := elemType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		if err = elem.Set("A", a); err != nil {
			t.Fatal(err)
		}
		if err = elem.Set("B", b); err != nil {
			t.Fatal(err)
		}
		return elem
	}
	newColl := func(t *testing.T) godror.ObjectCollection {
		t.Helper()
		coll, err := collType.NewCollection()
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 3; i++ {
			elem := newElem(t, i, fmt.Sprintf("b%d", i))
			err = coll.AppendObject(elem)
			elem.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		return coll
	}
	getA := func(t *testing.T, obj *godror.Object) string {
		t.Helper()
		v, err := obj.Get("A")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(v)
	}

	t.Run("element", func(t *testing.T) {
		coll := newColl(t)
		defer coll.Close()
		for i := 0; i < 2; i++ {
			v, err := coll.Get(0)
			if err != nil {
				t.Fatal(err)
			}
			elem := v.(*godror.Object)
			got := getA(t, elem)
			if err = elem.Close(); err != nil {
				t.Fatal(err)
			}
			if got != "1" {
				t.Fatalf("%d. A of element 0: got %q, wanted 1", i, got)
			}
		}
	})

	type elemStruct struct {
		A int    `godror:"A"`
		B string `godror:"B"`
	}
	want := []elemStruct{{1, "b1"}, {2, "b2"}, {3, "b3"}}

	t.Run("StructScan", func(t *testing.T) {
		coll := newColl(t)
		defer coll.Close()
		for i := 0; i < 2; i++ {
			var got []elemStruct
			if err := godror.StructScan(&got, coll.Object); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%d. StructScan: got %+v, wanted %+v", i, got, want)
			}
		}
	})

	t.Run("attribute", func(t *testing.T) {
		wrap, err := wrapType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer wrap.Close()
		elem := newElem(t, 1, "b1")
		err = wrap.Set("X", elem)
		elem.Close()
		if err != nil {
			t.Fatal(err)
		}
		coll := newColl(t)
		err = wrap.Set("ITEMS", coll.Object)
		coll.Close()
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 2; i++ {
			v, err := wrap.Get("X")
			if err != nil {
				t.Fatal(err)
			}
			x := v.(*godror.Object)
			got := getA(t, x)
			if err = x.Close(); err != nil {
				t.Fatal(err)
			}
			if got != "1" {
				t.Errorf("%d. X.A: got %q, wanted 1", i, got)
			}

			if v, err = wrap.Get("ITEMS"); err != nil {
				t.Fatal(err)
			}
			items := v.(*godror.ObjectCollection)
			n, err := items.Len()
			if err != nil {
				t.Fatal(err)
			}
			if err = items.Close(); err != nil {
				t.Fatal(err)
			}
			if n != 3 {
				t.Errorf("%d. len(ITEMS): got %d, wanted 3", i, n)
			}
		}

		var got struct {
			X     elemStruct   `godror:"X"`
			Items []elemStruct `godror:"ITEMS"`
		}
		for i := 0; i < 2; i++ {
			if err := godror.StructScan(&got, wrap); err != nil {
				t.Fatal(err)
			}
			if got.X != want[0] || !reflect.DeepEqual(got.Items, want) {
				t.Errorf("%d. StructScan: got %+v", i, got)
			}
		}
	})
}
