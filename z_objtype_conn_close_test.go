// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectTypeConnClose selects an object that has an object-typed
// attribute and a collection attribute on a pooled connection, with and
// without getting its ObjectType first, then closes the connection.
// Closing the cached ObjectTypes must not release the statement's
// dpiObjectType (heap-use-after-free under -asan).
func TestObjectTypeConnClose(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	sub := "TEST_OTUAF_SUB" + tblSuffix
	coll := "TEST_OTUAF_COLL" + tblSuffix
	par := "TEST_OTUAF_PAR" + tblSuffix
	dropTypes := func() {
		for _, nm := range []string{par, coll, sub} {
			testDb.ExecContext(context.Background(), "DROP TYPE "+nm+" FORCE")
		}
	}
	dropTypes()
	defer dropTypes()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + sub + " AS OBJECT (a NUMBER)",
		"CREATE OR REPLACE TYPE " + coll + " AS TABLE OF " + sub,
		"CREATE OR REPLACE TYPE " + par + " AS OBJECT (id NUMBER, s " + sub + ", c " + coll + ")",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	for i := 0; i < 4; i++ {
		cx, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 1 {
			// The type is cached by GetObjectType before the query.
			ot, err := godror.GetObjectType(ctx, cx, par)
			if err != nil {
				t.Fatal(err)
			}
			obj, err := ot.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			obj.Close()
		}
		var v any
		if err := cx.QueryRowContext(ctx, "SELECT "+par+"(1, "+sub+"(2), "+coll+"("+sub+"(3))) FROM DUAL").Scan(&v); err != nil {
			t.Fatal(err)
		}
		got := v.(*godror.Object)
		if id, err := got.Get("ID"); err != nil {
			t.Fatal(err)
		} else if fmt.Sprint(id) != "1" {
			t.Errorf("%d. got ID=%#v, wanted 1", i, id)
		}
		got.Close()
		// Make database/sql close the driver conn.
		cx.Raw(func(any) error { return driver.ErrBadConn })
		cx.Close()
	}
}
