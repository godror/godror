// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectLobRelease checks that the LOBs read from object attributes and
// collection elements are released: every dpiLob holds a reference to its
// connection, so a leaked one keeps the session from returning to the pool.
// It checks that a Lob left unread is closed with its Object, too.
func TestObjectLobRelease(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	objName, collName := "TEST_OBJLOB_OT"+tblSuffix, "TEST_OBJLOB_CT"+tblSuffix
	drop := func() {
		testDb.ExecContext(context.Background(), "DROP TYPE "+collName)
		testDb.ExecContext(context.Background(), "DROP TYPE "+objName)
	}
	drop()
	defer drop()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + objName + " AS OBJECT (id NUMBER, c CLOB, b BLOB)",
		"CREATE OR REPLACE TYPE " + collName + " AS TABLE OF CLOB",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	// A pool with a single session: if the session is not returned,
	// the next db.Conn fails after WaitTimeout.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 0, 1, 1
	P.WaitTimeout = 3 * time.Second
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	text := strings.Repeat("0123456789abcdef", 100000) // > 1 MiB
	const qry = "SELECT " + "%OBJ%(1, c, TO_BLOB(UTL_RAW.CAST_TO_RAW('blob'))), %COLL%(c, TO_CLOB('second')) FROM (SELECT :1 AS c FROM DUAL)"
	query := strings.NewReplacer("%OBJ%", objName, "%COLL%", collName).Replace(qry)
	var a [8]byte
	for round := 0; round < 3; round++ {
		cx, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("round %d: the session did not return to the pool: %v", round, err)
		}
		var o, c any
		if err = cx.QueryRowContext(ctx, query, godror.Lob{IsClob: true, Reader: strings.NewReader(text)}).Scan(&o, &c); err != nil {
			t.Fatal(err)
		}
		obj, coll := o.(*godror.Object), c.(*godror.Object).Collection()

		var unread []*godror.Lob
		// Read to the end.
		for _, get := range []func() (any, error){
			func() (any, error) { return obj.Get("C") },
			func() (any, error) { return coll.Get(0) },
		} {
			v, err := get()
			if err != nil {
				t.Fatal(err)
			}
			if got, err := io.ReadAll(v.(*godror.Lob)); err != nil {
				t.Fatal(err)
			} else if string(got) != text {
				t.Errorf("got %d bytes, wanted %d", len(got), len(text))
			}
			// Read again, and leave it unread.
			if v, err = get(); err != nil {
				t.Fatal(err)
			}
			unread = append(unread, v.(*godror.Lob))
		}
		// Left half read.
		v, err := obj.Get("B")
		if err != nil {
			t.Fatal(err)
		}
		lob := v.(*godror.Lob)
		if n, err := lob.Read(a[:2]); err != nil || string(a[:n]) != "bl" {
			t.Errorf("read BLOB: got %q, %v", a[:n], err)
		}
		unread = append(unread, lob)
		// Closed by the caller.
		if v, err = coll.Get(1); err != nil {
			t.Fatal(err)
		}
		if err = v.(*godror.Lob).Reader.(io.Closer).Close(); err != nil {
			t.Error(err)
		}

		obj.Close()
		coll.Close()
		for i, lob := range unread {
			if _, err := lob.Read(a[:]); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%d. Read after Object.Close: got %v, wanted ErrLOBClosed", i, err)
			}
		}

		// Make database/sql close the connection, to release the session.
		cx.Close()
		db.SetMaxIdleConns(0)
		db.SetMaxIdleConns(2)
	}
}
