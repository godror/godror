// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectBooleanFromServer reads BOOLEAN collection elements and object
// attributes of an object that the database has built.
func TestObjectBooleanFromServer(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	collName, objName := "test_boolelt_c"+tblSuffix, "test_boolelt_o"+tblSuffix
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + collName + " AS TABLE OF BOOLEAN",
		"CREATE OR REPLACE TYPE " + objName + " AS OBJECT (a BOOLEAN, b BOOLEAN)",
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Skipf("BOOLEAN in SQL types needs Oracle 23ai: %s: %+v", qry, err)
		}
	}
	defer func() {
		testDb.Exec("DROP TYPE " + collName + " FORCE")
		testDb.Exec("DROP TYPE " + objName + " FORCE")
	}()

	queryObject := func(qry string) *godror.Object {
		var v any
		if err := conn.QueryRowContext(ctx, qry).Scan(&v); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		obj, ok := v.(*godror.Object)
		if !ok {
			t.Fatalf("%s: got %T, wanted *godror.Object", qry, v)
		}
		return obj
	}

	coll := queryObject("SELECT " + collName + "(TRUE, FALSE, TRUE) FROM DUAL")
	defer coll.Close()
	for i, want := range []bool{true, false, true} {
		var d godror.Data
		if err := coll.Collection().GetItem(&d, i); err != nil {
			t.Fatal(err)
		}
		if got := d.GetBool(); got != want {
			t.Errorf("element %d: got %t, wanted %t", i, got, want)
		}
	}

	obj := queryObject("SELECT " + objName + "(TRUE, FALSE) FROM DUAL")
	defer obj.Close()
	for name, want := range map[string]bool{"A": true, "B": false} {
		got, err := obj.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("attribute %s: got %v, wanted %t", name, got, want)
		}
	}
}
