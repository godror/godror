// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectNumberLongText reads NUMBER object attributes and collection
// elements whose decimal text is longer than 39 characters.
func TestObjectNumberLongText(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	objName, collName := "test_numbuf_o"+tblSuffix, "test_numbuf_c"+tblSuffix
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + objName + " AS OBJECT (n NUMBER)",
		"CREATE OR REPLACE TYPE " + collName + " AS TABLE OF NUMBER",
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	defer func() {
		testDb.Exec("DROP TYPE " + objName + " FORCE")
		testDb.Exec("DROP TYPE " + collName + " FORCE")
	}()

	for _, lit := range []string{
		"-0.5",
		"12345678901234567890123456789012345678",
		"1/3",
		"-1/3",
		"-1234567890123456789.1234567890123456789",
		"1e39",
		"1e-40",
		"-9.999999999999999999999999999999999999e125",
		"-1.234567890123456789012345678901234567e-130",
	} {
		var wantText string
		var obj, coll any
		qry := "SELECT TO_CHAR(" + lit + ", 'TME'), " + objName + "(" + lit + "), " + collName + "(" + lit + ") FROM DUAL"
		if err := conn.QueryRowContext(ctx, qry).Scan(&wantText, &obj, &coll); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		want, ok := new(big.Rat).SetString(wantText)
		if !ok {
			t.Fatalf("%s: cannot parse %q", lit, wantText)
		}
		check := func(what string, v any, err error) {
			if err != nil {
				t.Errorf("%s %s: %+v", what, lit, err)
				return
			}
			if b, ok := v.([]byte); ok { // the collection gives the text as bytes
				v = string(b)
			}
			got, ok := new(big.Rat).SetString(fmt.Sprint(v))
			if !ok || got.Cmp(want) != 0 {
				t.Errorf("%s %s: got %v, wanted %s", what, lit, v, wantText)
			}
		}
		o := obj.(*godror.Object)
		v, err := o.Get("N")
		check("attribute", v, err)
		o.Close()
		c := coll.(*godror.Object)
		v, err = c.Collection().Get(0)
		check("element", v, err)
		c.Close()
	}
}
