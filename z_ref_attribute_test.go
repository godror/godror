// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Object types with a REF attribute or element must load, and the other
// attributes must be readable; only the REF itself is not supported.
func TestObjectRefAttribute(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sfx := strings.ToUpper(tblSuffix)
	target, typ, coll, tbl := "TEST_REFATTR_T"+sfx, "TEST_REFATTR_O"+sfx, "TEST_REFATTR_C"+sfx, "TEST_REFATTR_TBL"+sfx
	cleanup := func() {
		for _, qry := range []string{
			"DROP TABLE " + tbl,
			"DROP TYPE " + coll + " FORCE", "DROP TYPE " + typ + " FORCE", "DROP TYPE " + target + " FORCE",
		} {
			testDb.ExecContext(context.Background(), qry)
		}
	}
	cleanup()
	defer cleanup()
	for _, qry := range []string{
		"CREATE TYPE " + target + " AS OBJECT (id NUMBER(9))",
		"CREATE TYPE " + typ + " AS OBJECT (id NUMBER(9), r REF " + target + ")",
		"CREATE TYPE " + coll + " AS TABLE OF REF " + target,
		"CREATE TABLE " + tbl + " OF " + target,
		"INSERT INTO " + tbl + " VALUES (" + target + "(7))",
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	ot, err := godror.GetObjectType(ctx, conn, typ)
	if err != nil {
		t.Fatal(err)
	}
	defer ot.Close()
	if _, ok := ot.Attributes["R"]; !ok {
		t.Errorf("attributes: %q, wanted R, too", ot.AttributeNames())
	}
	newObj, err := ot.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer newObj.Close()
	if err = newObj.Set("ID", 3); err != nil {
		t.Error(err)
	}

	for _, qry := range []string{
		"SELECT " + typ + "(1, NULL) FROM DUAL",
		"SELECT " + typ + "(1, REF(x)) FROM " + tbl + " x",
	} {
		var v any
		if err := conn.QueryRowContext(ctx, qry).Scan(&v); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		obj, ok := v.(*godror.Object)
		if !ok {
			t.Fatalf("%s: got %T, wanted *godror.Object", qry, v)
		}
		if id, err := obj.Get("ID"); err != nil {
			t.Errorf("%s: get ID: %+v", qry, err)
		} else if id != int64(1) {
			t.Errorf("%s: got ID=%#v, wanted 1", qry, id)
		}
		if got, err := obj.Get("R"); !errors.Is(err, godror.ErrNotSupported) {
			t.Errorf("%s: get R: got (%v, %v), wanted ErrNotSupported", qry, got, err)
		}
		obj.Close()
	}

	var v any
	qry := "SELECT " + coll + "(REF(x)) FROM " + tbl + " x"
	if err := conn.QueryRowContext(ctx, qry).Scan(&v); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	obj, ok := v.(*godror.Object)
	if !ok {
		t.Fatalf("%s: got %T, wanted *godror.Object", qry, v)
	}
	defer obj.Close()
	if n, err := obj.Collection().Len(); err != nil || n != 1 {
		t.Errorf("%s: got length (%d, %v), wanted 1", qry, n, err)
	}
	if got, err := obj.Collection().Get(0); !errors.Is(err, godror.ErrNotSupported) {
		t.Errorf("%s: get element: got (%v, %v), wanted ErrNotSupported", qry, got, err)
	}
}
