// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"iter"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

type (
	bindReuseItem struct {
		ID    int32  `godror:"ID"`
		Name  string `godror:"NAME"`
		Extra string `godror:"EXTRA"`
	}
	// bindReuseList is written with SliceWriteObject.
	bindReuseList struct{ Items []bindReuseItem }
	// bindReuseColl is an ObjectCollectionWriter.
	bindReuseColl struct{ Items []bindReuseItem }
)

// The names of the types are different in each run: see bindReuseSetup.
var bindReuseTyp, bindReuseTypL string

func (bindReuseItem) ObjectTypeName() string { return bindReuseTyp }
func (p bindReuseItem) WriteObject(o *godror.Object) error {
	return godror.StructWriteObject(o, &p)
}
func (bindReuseList) ObjectTypeName() string { return bindReuseTypL }
func (l bindReuseList) WriteObject(o *godror.Object) error {
	return godror.SliceWriteObject(o, l.Items)
}
func (bindReuseColl) ObjectTypeName() string { return bindReuseTypL }
func (c bindReuseColl) Iter() iter.Seq[godror.ObjectWriter] {
	return func(yield func(godror.ObjectWriter) bool) {
		for _, item := range c.Items {
			if !yield(item) {
				return
			}
		}
	}
}

// bindReuseSetup creates an object type, a collection type of it, and
//
//   - a function that returns what it got in a collection: "count:id/name/extra,...",
//   - a procedure that returns what it got in an object ("id/name/extra"),
//     and sets its extra attribute.
//
// Every name and DDL text is unique, so neither a replaced type nor a repeated
// DDL can disturb the connections of testDb.
func bindReuseSetup(t *testing.T, ctx context.Context) (fn, proc string) {
	t.Helper()
	sfx := strings.ToUpper(tblSuffix + "_" + strconv.FormatInt(time.Now().UnixNano()%1e12, 36))
	bindReuseTyp, bindReuseTypL = "TEST_BRO"+sfx, "TEST_BRL"+sfx
	fn, proc = "TEST_BRF"+sfx, "TEST_BRP"+sfx
	t.Cleanup(func() {
		for _, qry := range []string{
			"DROP PROCEDURE " + proc, "DROP FUNCTION " + fn,
			"DROP TYPE " + bindReuseTypL + " FORCE", "DROP TYPE " + bindReuseTyp + " FORCE",
		} {
			testDb.ExecContext(context.Background(), qry)
		}
	})
	for _, qry := range []string{
		"CREATE TYPE " + bindReuseTyp + " AS OBJECT (id NUMBER(9), name VARCHAR2(100), extra VARCHAR2(100))",
		"CREATE TYPE " + bindReuseTypL + " AS TABLE OF " + bindReuseTyp,
		"CREATE FUNCTION " + fn + "(p IN " + bindReuseTypL + ") RETURN VARCHAR2 IS s VARCHAR2(4000); BEGIN " +
			"IF p IS NULL THEN RETURN 'NULL'; END IF; s := p.COUNT||':'; " +
			"FOR i IN 1..p.COUNT LOOP s := s||p(i).id||'/'||p(i).name||'/'||p(i).extra||','; END LOOP; " +
			"RETURN s; END;",
		"CREATE PROCEDURE " + proc + "(p IN OUT " + bindReuseTyp + ", seen OUT VARCHAR2) IS BEGIN " +
			"seen := p.id||'/'||p.name||'/'||p.extra; p.extra := 'srv'||p.id; END;",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	return fn, proc
}

// bindReuseBoth calls f with an exec function that executes qry as a prepared
// statement, and with one that executes it with Exec on one connection,
// which uses the statement cache of the connection.
func bindReuseBoth(t *testing.T, ctx context.Context, qry string, f func(t *testing.T, conn *sql.Conn, exec func(args ...any) error)) {
	for _, mode := range []string{"prepared", "exec"} {
		t.Run(mode, func(t *testing.T) {
			conn, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exec := func(args ...any) error {
				_, err := conn.ExecContext(ctx, qry, args...)
				return err
			}
			if mode == "prepared" {
				stmt, err := conn.PrepareContext(ctx, qry)
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
				exec = func(args ...any) error {
					_, err := stmt.ExecContext(ctx, args...)
					return err
				}
			}
			f(t, conn, exec)
		})
	}
}

// A collection bound with fewer elements than at the previous execution
// of the statement must not send the elements of the previous one.
func TestBindShorterCollection(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	fn, _ := bindReuseSetup(t, ctx)

	bindReuseBoth(t, ctx, "BEGIN :1 := "+fn+"(:2); END;", func(t *testing.T, _ *sql.Conn, exec func(args ...any) error) {
		for _, kind := range []string{"SliceWriteObject", "ObjectCollectionWriter"} {
			for _, n := range []int{3, 1, 0, 2} {
				items := make([]bindReuseItem, n)
				want := strconv.Itoa(n) + ":"
				for i := range items {
					id := 10*n + i
					items[i] = bindReuseItem{ID: int32(id), Name: "n", Extra: "x"}
					want += strconv.Itoa(id) + "/n/x,"
				}
				var arg any = bindReuseList{Items: items}
				if kind == "ObjectCollectionWriter" {
					arg = bindReuseColl{Items: items}
				}
				var got string
				if err := exec(sql.Out{Dest: &got}, arg); err != nil {
					t.Fatalf("%s %d: %+v", kind, n, err)
				}
				if got != want {
					t.Errorf("%s with %d elements: the server got %q, wanted %q", kind, n, got, want)
				}
			}
		}
	})
}
