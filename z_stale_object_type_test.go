// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
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

type staleOTWriter struct {
	ID int32 `godror:"ID"`
}

// The name of the type is different in each run.
var staleOTWriterTyp string

func (staleOTWriter) ObjectTypeName() string { return staleOTWriterTyp }
func (w staleOTWriter) WriteObject(o *godror.Object) error {
	return godror.StructWriteObject(o, &w)
}

// A statement that fails with ORA-00902 is executed again only if the error
// can come from a replaced object type: what the statement itself has raised
// must not run it twice. And the object types the caller holds stay usable.
func TestInvalidDatatypeRetry(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	sfx := strings.ToUpper(tblSuffix + "_" + strconv.FormatInt(time.Now().UnixNano()%1e12, 36))
	staleOTWriterTyp = "TEST_STALE_W" + sfx
	other, tbl := "TEST_STALE_O"+sfx, "TEST_STALE_T"+sfx
	defer func() {
		for _, qry := range []string{
			"DROP TABLE " + tbl + " PURGE", "DROP TYPE " + other + " FORCE", "DROP TYPE " + staleOTWriterTyp + " FORCE",
		} {
			testDb.ExecContext(context.Background(), qry)
		}
	}()
	for _, qry := range []string{
		"CREATE TYPE " + staleOTWriterTyp + " AS OBJECT (id NUMBER(9))",
		"CREATE TYPE " + other + " AS OBJECT (id NUMBER(9))",
		"CREATE TABLE " + tbl + " (id NUMBER(9))",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The caller holds an object type of the connection.
	ot, err := godror.GetObjectType(ctx, conn, other)
	if err != nil {
		t.Fatal(err)
	}
	var want int
	check := func(name string) {
		t.Helper()
		want++
		var got int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: the block has run %d times, wanted once", name, 1+got-want)
			want = got
		}
		obj, err := ot.NewObject()
		if err != nil {
			t.Fatalf("%s: the caller's ObjectType is not usable: %+v", name, err)
		}
		obj.Close()
	}

	// The blocks insert a row (and commit), ...
	block := func(decl, rest string) string {
		return "DECLARE PRAGMA AUTONOMOUS_TRANSACTION; v " + decl + " := :1; BEGIN INSERT INTO " + tbl +
			" (id) VALUES (1); COMMIT; " + rest + " END;"
	}
	// ... then these fail with ORA-00902 by themselves.
	const raise = "EXECUTE IMMEDIATE 'CREATE TABLE test_stale_x (c test_stale_no_such_type)';"
	for _, tC := range []struct {
		name, qry string
		arg       any
	}{
		{"no object bind", block("NUMBER", raise), 1},
		{"object bind", block(staleOTWriterTyp, raise), staleOTWriter{ID: 1}},
	} {
		_, err := conn.ExecContext(ctx, tC.qry, tC.arg)
		var oe *godror.OraErr
		if !errors.As(err, &oe) || oe.Code() != 902 {
			t.Fatalf("%s: got %+v, wanted ORA-00902", tC.name, err)
		}
		check(tC.name)
		rows, err := conn.QueryContext(ctx, tC.qry, tC.arg)
		if err == nil {
			rows.Close()
		}
		if !errors.As(err, &oe) || oe.Code() != 902 {
			t.Fatalf("%s: query got %+v, wanted ORA-00902", tC.name, err)
		}
		check(tC.name + " query")
	}

	// An ObjectWriter of a replaced type is bound again with the new type.
	qry := block(staleOTWriterTyp, "")
	if _, err = conn.ExecContext(ctx, qry, staleOTWriter{ID: 1}); err != nil {
		t.Fatal(err)
	}
	check("before the type is replaced")
	replace := "CREATE OR REPLACE TYPE " + staleOTWriterTyp + " FORCE AS OBJECT (id NUMBER(9), extra DATE)"
	if _, err = testDb.ExecContext(ctx, replace); err != nil {
		t.Fatalf("%s: %+v", replace, err)
	}
	if _, err = conn.ExecContext(ctx, qry, staleOTWriter{ID: 1}); err != nil {
		t.Fatalf("after the type has been replaced: %+v", err)
	}
	check("after the type has been replaced")
}
