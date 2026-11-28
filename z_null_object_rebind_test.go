// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

type nullObjRebindT struct {
	ID   int32  `godror:"ID"`
	Name string `godror:"NAME"`
}

var nullObjRebindTyp string

func (nullObjRebindT) ObjectTypeName() string { return nullObjRebindTyp }
func (t nullObjRebindT) WriteObject(o *godror.Object) error {
	return godror.StructWriteObject(o, &t)
}

// TestNullObjectRebind binds a NULL object through a statement that has
// bound an object before: the server must get NULL, not the previous object.
func TestNullObjectRebind(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	sfx := strings.ToUpper(tblSuffix + "_" + strconv.FormatInt(time.Now().UnixNano()%1e12, 36))
	typ, coll := "TEST_NOR_T"+sfx, "TEST_NOR_L"+sfx
	fun, funL, proc := "test_nor_f"+sfx, "test_nor_fl"+sfx, "test_nor_p"+sfx
	nullObjRebindTyp = typ
	cleanup := func() {
		for _, qry := range []string{
			"DROP PROCEDURE " + proc,
			"DROP FUNCTION " + funL, "DROP FUNCTION " + fun,
			"DROP TYPE " + coll + " FORCE", "DROP TYPE " + typ + " FORCE",
		} {
			testDb.ExecContext(context.Background(), qry)
		}
	}
	cleanup()
	defer cleanup()
	for _, qry := range []string{
		"CREATE TYPE " + typ + " AS OBJECT (id NUMBER(9), name VARCHAR2(30))",
		"CREATE TYPE " + coll + " AS TABLE OF NUMBER(9)",
		"CREATE FUNCTION " + fun + "(p IN " + typ + `) RETURN VARCHAR2 IS BEGIN
  IF p IS NULL THEN RETURN 'NULL'; END IF;
  RETURN p.id||':'||p.name;
END;`,
		"CREATE FUNCTION " + funL + "(p IN " + coll + `) RETURN VARCHAR2 IS
  r VARCHAR2(1000);
BEGIN
  IF p IS NULL THEN RETURN 'NULL'; END IF;
  FOR i IN 1..p.COUNT LOOP r := r||p(i)||','; END LOOP;
  RETURN '['||r||']';
END;`,
		"CREATE PROCEDURE " + proc + "(p IN OUT " + typ + ", seen OUT VARCHAR2) IS BEGIN seen := " + fun + "(p); END;",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	for _, mode := range []string{"prepared", "exec"} {
		t.Run(mode, func(t *testing.T) {
			conn, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			// execer returns the function that executes qry, as a prepared
			// statement, or with the same text each time (the Exec cache).
			execer := func(qry string) func(args ...any) {
				if mode == "exec" {
					return func(args ...any) {
						t.Helper()
						if _, err := conn.ExecContext(ctx, qry, args...); err != nil {
							t.Fatalf("%s: %+v", qry, err)
						}
					}
				}
				stmt, err := conn.PrepareContext(ctx, qry)
				if err != nil {
					t.Fatalf("%s: %+v", qry, err)
				}
				t.Cleanup(func() { stmt.Close() })
				return func(args ...any) {
					t.Helper()
					if _, err := stmt.ExecContext(ctx, args...); err != nil {
						t.Fatalf("%s: %+v", qry, err)
					}
				}
			}
			ot, err := godror.GetObjectType(ctx, conn, typ)
			if err != nil {
				t.Fatal(err)
			}
			newObj := func(id int) *godror.Object {
				t.Helper()
				obj, err := ot.NewObject()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { obj.Close() })
				if err = obj.Set("ID", id); err != nil {
					t.Fatal(err)
				}
				if err = obj.Set("NAME", "n"+strconv.Itoa(id)); err != nil {
					t.Fatal(err)
				}
				return obj
			}
			null := func() *godror.Object { return &godror.Object{ObjectType: ot} }
			var got string

			// IN: the caller's objects, the objects of a writer, and NULLs.
			exec := execer("BEGIN :1 := " + fun + "(:2); END;")
			for i, tC := range []struct {
				arg  any
				want string
			}{
				{null(), "NULL"},
				{newObj(1), "1:n1"},
				{null(), "NULL"},
				{null(), "NULL"},
				{nullObjRebindT{ID: 2, Name: "n2"}, "2:n2"},
				{null(), "NULL"},
				{nullObjRebindT{ID: 3, Name: "n3"}, "3:n3"},
				{*null(), "NULL"},
				{newObj(4), "4:n4"},
			} {
				exec(sql.Out{Dest: &got}, tC.arg)
				if got != tC.want {
					t.Errorf("IN %d. %T: the server got %q, wanted %q", i, tC.arg, got, tC.want)
				}
			}

			// IN OUT
			exec = execer("BEGIN " + proc + "(:1, :2); END;")
			for i, tC := range []struct {
				arg  *godror.Object
				want string
			}{
				{newObj(5), "5:n5"},
				{null(), "NULL"},
				{newObj(6), "6:n6"},
				{null(), "NULL"},
			} {
				exec(sql.Out{Dest: tC.arg, In: true}, sql.Out{Dest: &got})
				if got != tC.want {
					t.Errorf("IN OUT %d.: the server got %q, wanted %q", i, got, tC.want)
				}
			}

			// A collection.
			cot, err := godror.GetObjectType(ctx, conn, coll)
			if err != nil {
				t.Fatal(err)
			}
			exec = execer("BEGIN :1 := " + funL + "(:2); END;")
			for i, want := range []string{"[1,2,]", "NULL", "[3,]", "NULL"} {
				arg := &godror.Object{ObjectType: cot}
				if want != "NULL" {
					if arg, err = cot.NewObject(); err != nil {
						t.Fatal(err)
					}
					defer arg.Close()
					for s := range strings.SplitSeq(strings.Trim(want, "[,]"), ",") {
						n, _ := strconv.Atoi(s)
						if err = arg.Collection().Append(n); err != nil {
							t.Fatal(err)
						}
					}
				}
				exec(sql.Out{Dest: &got}, arg)
				if got != want {
					t.Errorf("collection %d.: the server got %q, wanted %q", i, got, want)
				}
			}
		})
	}
}
