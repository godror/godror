// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// An Object or ObjectCollection created by the caller and bound as an argument
// belongs to the caller: it must be usable after the Exec.
func TestBoundObjectNotClosed(t *testing.T) {
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	objName := strings.ToUpper("test_boc_o" + tblSuffix)
	collName := strings.ToUpper("test_boc_l" + tblSuffix)
	cleanup := func() {
		testDb.ExecContext(context.Background(), "DROP TYPE "+collName+" FORCE")
		testDb.ExecContext(context.Background(), "DROP TYPE "+objName+" FORCE")
	}
	cleanup()
	defer cleanup()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + objName + " AS OBJECT (id NUMBER(9))",
		"CREATE OR REPLACE TYPE " + collName + " AS TABLE OF NUMBER(9)",
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	objType, err := godror.GetObjectType(ctx, conn, objName)
	if err != nil {
		t.Fatal(err)
	}
	newObj := func(t *testing.T, id int) *godror.Object {
		t.Helper()
		obj, err := objType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		if err := obj.Set("ID", id); err != nil {
			t.Fatal(err)
		}
		return obj
	}
	checkID := func(t *testing.T, obj *godror.Object, want int64) {
		t.Helper()
		if got, err := obj.Get("ID"); err != nil || got != want {
			t.Errorf("object ID is %v (%v), wanted %d", got, err, want)
		}
	}
	// -1 means the server got NULL.
	getIDQry := "DECLARE v " + objName + " := :1; BEGIN :2 := CASE WHEN v IS NULL THEN -1 ELSE v.id END; END;"

	t.Run("pointer", func(t *testing.T) {
		obj := newObj(t, 42)
		defer obj.Close()
		for i := 0; i < 2; i++ {
			var got int64
			if _, err := conn.ExecContext(ctx, getIDQry, obj, sql.Out{Dest: &got}); err != nil {
				t.Fatalf("%d. %s: %+v", i, getIDQry, err)
			}
			if got != 42 {
				t.Errorf("%d. the server got ID=%d, wanted 42", i, got)
			}
			checkID(t, obj, 42)
		}
	})

	t.Run("value", func(t *testing.T) {
		obj := newObj(t, 42)
		for i := 0; i < 2; i++ {
			var got int64
			if _, err := conn.ExecContext(ctx, getIDQry, *obj, sql.Out{Dest: &got}); err != nil {
				t.Fatalf("%d. %s: %+v", i, getIDQry, err)
			}
			if got != 42 {
				t.Errorf("%d. the server got ID=%d, wanted 42", i, got)
			}
			checkID(t, obj, 42)
		}
		if err := obj.Close(); err != nil {
			t.Error(err)
		}
	})

	t.Run("inout", func(t *testing.T) {
		obj := newObj(t, 42)
		defer obj.Close()
		qry := "DECLARE v " + objName + " := :1; BEGIN v.id := v.id + 1; :1 := v; END;"
		for i := 0; i < 2; i++ {
			if _, err := conn.ExecContext(ctx, qry, sql.Out{Dest: obj, In: true}); err != nil {
				t.Fatalf("%d. %s: %+v", i, qry, err)
			}
			checkID(t, obj, int64(43+i))
		}
	})

	t.Run("collection", func(t *testing.T) {
		collType, err := godror.GetObjectType(ctx, conn, collName)
		if err != nil {
			t.Fatal(err)
		}
		coll, err := collType.NewCollection()
		if err != nil {
			t.Fatal(err)
		}
		defer coll.Close()
		for i := 0; i < 3; i++ {
			if err := coll.Append(i); err != nil {
				t.Fatal(err)
			}
		}
		qry := "DECLARE v " + collName + " := :1; BEGIN :2 := CASE WHEN v IS NULL THEN -1 ELSE v.COUNT END; END;"
		for i := 0; i < 2; i++ {
			var got int64
			if _, err := conn.ExecContext(ctx, qry, coll, sql.Out{Dest: &got}); err != nil {
				t.Fatalf("%d. %s: %+v", i, qry, err)
			}
			if got != 3 {
				t.Errorf("%d. the server got %d elements, wanted 3", i, got)
			}
			if n, err := coll.Len(); err != nil || n != 3 {
				t.Errorf("%d. collection length is %d (%v), wanted 3", i, n, err)
			}
		}
	})
}
