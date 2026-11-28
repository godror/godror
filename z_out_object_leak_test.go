// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOutObjectLeak receives an object through an OUT parameter on the
// connections of a pool of two sessions, and closes the object and the
// connection after each round. A leaked dpiObject holds a reference to its
// dpiConn, so the session is never given back and the pool runs dry.
func TestOutObjectLeak(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	typ := "TEST_OUTLEAK" + tblSuffix
	coll := "TEST_OUTLEAK_C" + tblSuffix
	dropTypes := func() {
		testDb.ExecContext(context.Background(), "DROP TYPE "+coll+" FORCE")
		testDb.ExecContext(context.Background(), "DROP TYPE "+typ+" FORCE")
	}
	dropTypes()
	defer dropTypes()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + typ + " AS OBJECT (a NUMBER(9))",
		"CREATE OR REPLACE TYPE " + coll + " AS TABLE OF " + typ,
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	objQry := "BEGIN IF :1 IS NOT NULL THEN :2 := " + typ + "(:3); END IF; END;"
	for i, tC := range []struct {
		name string
		typ  string
		// f fills obj, and returns the wanted value of its "A" attribute,
		// or -1 to skip that check.
		f func(cx *sql.Conn, obj *godror.Object) (want int, err error)
	}{
		{"once", typ, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			_, err := cx.ExecContext(ctx, objQry, 1, sql.Out{Dest: obj}, 1)
			return 1, err
		}},
		{"twice", typ, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			if _, err := cx.ExecContext(ctx, objQry, 1, sql.Out{Dest: obj}, 1); err != nil {
				return 0, err
			}
			_, err := cx.ExecContext(ctx, objQry, 1, sql.Out{Dest: obj}, 2)
			return 2, err
		}},
		{"prepared", typ, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			stmt, err := cx.PrepareContext(ctx, objQry)
			if err != nil {
				return 0, err
			}
			defer stmt.Close()
			if _, err := stmt.ExecContext(ctx, 1, sql.Out{Dest: obj}, 1); err != nil {
				return 0, err
			}
			// The variable is reused: this returns the same dpiObject.
			_, err = stmt.ExecContext(ctx, 1, sql.Out{Dest: obj}, 2)
			return 2, err
		}},
		{"null", typ, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			_, err := cx.ExecContext(ctx, objQry, nil, sql.Out{Dest: obj}, 1)
			return -1, err
		}},
		{"inout", typ, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			_, err := cx.ExecContext(ctx, "DECLARE v "+typ+" := :1; BEGIN v.a := 3; :1 := v; END;", sql.Out{Dest: obj, In: true})
			return -1, err
		}},
		{"collection", coll, func(cx *sql.Conn, obj *godror.Object) (int, error) {
			c := obj.Collection()
			_, err := cx.ExecContext(ctx, "BEGIN :1 := "+coll+"("+typ+"(1)); END;", sql.Out{Dest: &c})
			return -1, err
		}},
	} {
		t.Run(tC.name, func(t *testing.T) {
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.StandaloneConnection = godror.Bool(false)
			P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 2, 1
			// The odd timeout gives every subtest a pool of its own.
			P.WaitTimeout = 3*time.Second + time.Duration(i+1)*time.Millisecond
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			// Close the driver connection on every release, to give the session back.
			db.SetMaxIdleConns(0)

			for i := range 4 {
				cx, err := db.Conn(ctx)
				if err != nil {
					t.Fatalf("%d. no session in the pool of two, though every connection was closed: %+v", i, err)
				}
				ot, err := godror.GetObjectType(ctx, cx, tC.typ)
				if err != nil {
					t.Fatal(err)
				}
				obj, err := ot.NewObject()
				if err != nil {
					t.Fatal(err)
				}
				want, err := tC.f(cx, obj)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				if want >= 0 {
					if got, err := obj.Get("A"); err != nil {
						t.Error(err)
					} else if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("got A=%v, wanted %d", got, want)
					}
				}
				if err = obj.Close(); err != nil {
					t.Error(err)
				}
				if err = cx.Close(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
