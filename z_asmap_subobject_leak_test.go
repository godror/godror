// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestAsMapSubObjectLeak converts an object that has an object attribute and
// a collection of objects, on the connections of a pool of two sessions, and
// closes the object and the connection after each round. A sub-object that
// the conversion leaves open holds a reference to the dpiConn, so the session
// is never given back and the pool runs dry.
func TestAsMapSubObjectLeak(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	sub := "TEST_AMSL_SUB" + tblSuffix
	item := "TEST_AMSL_ITEM" + tblSuffix
	list := "TEST_AMSL_LIST" + tblSuffix
	outer := "TEST_AMSL_OUTER" + tblSuffix
	dropTypes := func() {
		for _, nm := range []string{outer, list, item, sub} {
			testDb.ExecContext(context.Background(), "DROP TYPE "+nm+" FORCE")
		}
	}
	dropTypes()
	defer dropTypes()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + sub + " AS OBJECT (id NUMBER(9))",
		"CREATE OR REPLACE TYPE " + item + " AS OBJECT (id NUMBER(9))",
		"CREATE OR REPLACE TYPE " + list + " AS TABLE OF " + item,
		"CREATE OR REPLACE TYPE " + outer + " AS OBJECT (s " + sub + ", l " + list + ")",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	qry := "SELECT " + outer + "(" + sub + "(1), " + list + "(" + item + "(2), " + item + "(3))) FROM DUAL"

	const wantMap = "map[L:[map[ID:2] map[ID:3]] S:map[ID:1]]"
	const wantJSON = `{"L":[{"ID":2},{"ID":3}],"S":{"ID":1}}`
	for i, tC := range []struct {
		name, want string
		f          func(obj *godror.Object) (string, error)
	}{
		{"control", "", func(obj *godror.Object) (string, error) { return "", nil }},
		{"AsMap", wantMap, func(obj *godror.Object) (string, error) {
			m, err := obj.AsMap(true)
			return fmt.Sprint(m), err
		}},
		{"ToJSON", wantJSON, func(obj *godror.Object) (string, error) {
			var buf strings.Builder
			err := obj.ToJSON(&buf)
			return buf.String(), err
		}},
		{"String", wantJSON, func(obj *godror.Object) (string, error) {
			_, s, _ := strings.Cut(obj.String(), "{")
			return "{" + s, nil
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
			P.WaitTimeout = 3*time.Second + time.Duration(i+21)*time.Millisecond
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			// Close the driver connection on every release, to give the session back.
			db.SetMaxIdleConns(0)

			for i := 0; i < 4; i++ {
				cx, err := db.Conn(ctx)
				if err != nil {
					t.Fatalf("%d. no session in the pool of two, though every connection was closed: %+v", i, err)
				}
				var v any
				if err = cx.QueryRowContext(ctx, qry).Scan(&v); err != nil {
					t.Fatalf("%s: %+v", qry, err)
				}
				obj := v.(*godror.Object)
				// Twice: closing the sub-objects must leave obj intact.
				for j := 0; j < 2; j++ {
					if got, err := tC.f(obj); err != nil {
						t.Error(err)
					} else if got != tC.want {
						t.Errorf("%d. got %s, wanted %s", j, got, tC.want)
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
