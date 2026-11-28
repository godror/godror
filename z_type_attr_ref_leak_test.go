// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectTypeAttrRefLeak gets object types that refer to the same
// object type more than once, on the connections of a pool of two sessions,
// closing the connection after each round. A leaked dpiObjectType holds a
// reference to its dpiConn, so the session is never given back and the pool
// runs dry.
func TestObjectTypeAttrRefLeak(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	addr := "TEST_TARL_ADDR" + tblSuffix
	one := "TEST_TARL_ONE" + tblSuffix
	cust := "TEST_TARL_CUST" + tblSuffix
	list := "TEST_TARL_LIST" + tblSuffix
	dropTypes := func() {
		for _, nm := range []string{list, cust, one, addr} {
			testDb.ExecContext(context.Background(), "DROP TYPE "+nm+" FORCE")
		}
	}
	dropTypes()
	defer dropTypes()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + addr + " AS OBJECT (city VARCHAR2(100))",
		"CREATE OR REPLACE TYPE " + one + " AS OBJECT (id NUMBER(9), billing " + addr + ")",
		"CREATE OR REPLACE TYPE " + cust + " AS OBJECT (id NUMBER(9), billing " + addr + ", shipping " + addr + ")",
		"CREATE OR REPLACE TYPE " + list + " AS TABLE OF " + addr,
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	// The schema-qualified name is the key that the types found through
	// another type are cached under.
	var addrFull string
	{
		cx, err := testDb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ot, err := godror.GetObjectType(ctx, cx, addr)
		if err != nil {
			cx.Close()
			t.Fatal(err)
		}
		addrFull = ot.FullName()
		cx.Close()
	}

	for i, tC := range []struct {
		name  string
		types []string
	}{
		{"two attributes", []string{cust}},
		{"attribute type cached", []string{addrFull, one}},
		{"element type cached", []string{addrFull, list}},
		{"attribute type got later", []string{one, addr}},
	} {
		t.Run(tC.name, func(t *testing.T) {
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.StandaloneConnection = godror.Bool(false)
			P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 2, 1
			// The odd timeout gives every subtest a pool of its own.
			P.WaitTimeout = 3*time.Second + time.Duration(i+11)*time.Millisecond
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			// Close the driver connection on every release, to give the session back.
			db.SetMaxIdleConns(0)

			for i := range 4 {
				cx, err := db.Conn(ctx)
				if err != nil {
					t.Fatalf("%d. no session in the pool of two, though every connection was closed: %+v", i, err)
				}
				for _, nm := range tC.types {
					// The connection caches the types, and closes them when it is closed.
					if _, err := godror.GetObjectType(ctx, cx, nm); err != nil {
						t.Fatalf("%s: %+v", nm, err)
					}
				}
				if err = cx.Close(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
