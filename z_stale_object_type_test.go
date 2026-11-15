// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// When an object type is replaced while a connection has it cached,
// the statement fails with ORA-00902, and the connection must not be
// reused: the next one sees the new type.
func TestStaleObjectType(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	typ := strings.ToUpper("test_stale_ot" + tblSuffix)
	cleanup := func() {
		testDb.ExecContext(context.Background(), "DROP TYPE "+typ+" FORCE")
	}
	cleanup()
	defer cleanup()
	qry := "CREATE TYPE " + typ + " AS OBJECT (id NUMBER(9), name VARCHAR2(100))"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}

	// A pool of one connection, to get the same one each time.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxOpenConns(1)

	// use binds a new object of typ, and returns the number of the attributes.
	use := func() (int, error) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ot, err := godror.GetObjectType(ctx, conn, typ)
		if err != nil {
			t.Fatal(err)
		}
		obj, err := ot.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()
		if err = obj.Set("ID", 3); err != nil {
			t.Fatal(err)
		}
		var s string
		_, err = conn.ExecContext(ctx,
			"DECLARE v "+typ+" := :1; BEGIN :2 := TO_CHAR(v.id); END;",
			obj, sql.Out{Dest: &s})
		if err == nil && s != "3" {
			t.Errorf("got %q, wanted 3", s)
		}
		return len(ot.Attributes), err
	}

	if _, err = use(); err != nil {
		t.Fatal(err)
	}

	// The type is replaced on another connection.
	qry = "CREATE OR REPLACE TYPE " + typ + " FORCE AS OBJECT (id NUMBER(9), name VARCHAR2(100), extra DATE)"
	if _, err = testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}

	// The cached type is stale now, the first use fails.
	_, err = use()
	var oe *godror.OraErr
	if !errors.As(err, &oe) || oe.Code() != 902 {
		t.Fatalf("got %+v, wanted ORA-00902", err)
	}

	n, err := use()
	if err != nil {
		t.Fatalf("after the type has been replaced: %+v", err)
	}
	if n != 3 {
		t.Errorf("got %d attributes, wanted 3", n)
	}
}
