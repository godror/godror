// Copyright 2026 Tamás Gulácsi.
//
// SPDX-License-Identifier: UPL-1.0 or Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror"
)

func TestOracallNestedNumTT(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		t.Context(), 60*time.Second,
	)
	defer cancel()

	pkg := "test_oracall_nest"
	dropPkg := func() {
		testDb.ExecContext(context.Background(), `DROP PACKAGE `+pkg)
	}

	keepPackage, _ := strconv.ParseBool(os.Getenv("KEEP_PACKAGE"))
	if !keepPackage {
		pkg += tblSuffix
		defer dropPkg()
	}
	dropPkg()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// set up
	if err := func() error {
		// create on separate connection
		conn, err := testDb.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		for _, qry := range []string{
			`CREATE OR REPLACE PACKAGE ` + pkg + ` AS
    TYPE num_tt IS TABLE OF NUMBER;
    TYPE num_tt_tt IS TABLE OF num_tt INDEX BY PLS_INTEGER;

    PROCEDURE p_flat(p IN num_tt, s OUT NUMBER);
    PROCEDURE p_in(p IN num_tt_tt, s OUT NUMBER);
    PROCEDURE p_out(p OUT num_tt_tt);
    PROCEDURE p_inout(p IN OUT num_tt_tt);

		PROCEDURE p_out_empty(p OUT num_tt_tt);
		PROCEDURE p_inout_noop(p IN OUT num_tt_tt);

	PROCEDURE p_out_probe(
    p_case IN PLS_INTEGER,
    p OUT num_tt_tt
	);

	TYPE num_tt_nt IS TABLE OF num_tt;
	PROCEDURE p_out_nt(p OUT num_tt_nt);

	TYPE num_va IS VARRAY(10) OF NUMBER;
	TYPE num_va_aa IS TABLE OF num_va INDEX BY PLS_INTEGER;
	PROCEDURE p_out_va(p OUT num_va_aa);
END;
`,

			`CREATE OR REPLACE PACKAGE BODY ` + pkg + ` AS

    PROCEDURE p_flat(p IN num_tt, s OUT NUMBER) IS
        i PLS_INTEGER;
    BEGIN
        s := 0;
        i := p.FIRST;
        WHILE i IS NOT NULL LOOP
            s := s + p(i);
            i := p.NEXT(i);
        END LOOP;
    END;

    PROCEDURE p_in(p IN num_tt_tt, s OUT NUMBER) IS
        i PLS_INTEGER;
        j PLS_INTEGER;
    BEGIN
        s := 0;
        i := p.FIRST;
        WHILE i IS NOT NULL LOOP
            IF p(i) IS NOT NULL THEN
                j := p(i).FIRST;
                WHILE j IS NOT NULL LOOP
                    s := s + p(i)(j);
                    j := p(i).NEXT(j);
                END LOOP;
            END IF;
            i := p.NEXT(i);
        END LOOP;
    END;

    PROCEDURE p_out(p OUT num_tt_tt) IS
    BEGIN
        p(1) := num_tt(11, 22);
        p(3) := num_tt(33);
    END;

    PROCEDURE p_inout(p IN OUT num_tt_tt) IS
    BEGIN
        p(3) := num_tt(33);
    END;

	PROCEDURE p_out_empty(p OUT num_tt_tt) IS
	BEGIN
	    NULL;
	END;

	PROCEDURE p_inout_noop(p IN OUT num_tt_tt) IS
	BEGIN
	    NULL;
	END;

	PROCEDURE p_out_probe(
    p_case IN PLS_INTEGER,
    p OUT num_tt_tt
	) IS
	BEGIN
	    CASE p_case
	        WHEN 0 THEN
	            NULL;

	        WHEN 1 THEN
	            p(1) := NULL;

	        WHEN 2 THEN
	            p(1) := num_tt();

	        WHEN 3 THEN
	            p(1) := num_tt(11);

	        WHEN 4 THEN
	            p(1) := num_tt(11);
	            p(2) := num_tt(22);

	        WHEN 5 THEN
	            p(1) := num_tt(11);
	            p(3) := num_tt(22);

	        WHEN 6 THEN
	            p(3) := num_tt(11);

	        ELSE
	            RAISE_APPLICATION_ERROR(-20001, 'Invalid test case');
	    END CASE;
	END;

	PROCEDURE p_out_nt(p OUT num_tt_nt) IS
	BEGIN
    p := num_tt_nt();
    p.EXTEND;
    p(1) := num_tt();
	END;

	PROCEDURE p_out_va(p OUT num_va_aa) IS
	BEGIN
    p(1) := num_va();
	END;
END;
`,
		} {
			if _, err := conn.ExecContext(ctx, qry); err != nil {
				return fmt.Errorf("%s: %+v", qry, err)
			}
		}
		if ces, err := godror.GetCompileErrors(ctx, conn, false); err != nil {
			return err
		} else if len(ces) != 0 {
			return fmt.Errorf("compile errors: %+v", ces)
		}
		return conn.Close()
	}(); err != nil {
		t.Fatal(err)
	}

	numTtTt, err := godror.GetObjectType(
		ctx, conn, pkg+".NUM_TT_TT",
	)
	if err != nil {
		t.Fatal(err)
	}

	innerType := numTtTt.CollectionOf
	if innerType == nil || innerType.CollectionOf == nil {
		t.Fatal("missing collection element metadata")
	}

	leaf := innerType.CollectionOf
	t.Logf(
		"leaf: name=%q full=%q oracle=%v native=%v isObject=%v\n",
		leaf.Name,
		leaf.FullName(),
		leaf.OracleTypeNum,
		leaf.NativeTypeNum,
		leaf.IsObject(),
	)

	// Control case: a single-level collection.
	t.Run("single-level", func(t *testing.T) {
		flatType, err := godror.GetObjectType(
			ctx, conn, pkg+".NUM_TT",
		)
		if err != nil {
			t.Fatal(err)
		}

		flat, err := flatType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer flat.Close()

		if err := flat.Collection().Append(int64(11)); err != nil {
			t.Fatal(err)
		}
		if err := flat.Collection().Append(int64(22)); err != nil {
			t.Fatal(err)
		}

		var flatSum int64
		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_FLAT(:1,:2); END;",
			flat, sql.Out{Dest: &flatSum},
		); err != nil {
			t.Errorf("FLAT: sum=%d err=%+v", flatSum, err)
		}
	})

	// Nested collection.
	t.Run("nested", func(t *testing.T) {
		outer, err := numTtTt.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer outer.Close()

		inner, err := innerType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer inner.Close()

		for _, n := range []int64{11, 22} {
			var d godror.Data
			if err := d.Set(n); err != nil {
				t.Fatal(err)
			}
			if err := inner.Collection().AppendData(&d); err != nil {
				t.Fatal(err)
			}
		}

		if err := outer.Collection().AppendObject(inner); err != nil {
			t.Fatal(err)
		}

		var sum int64
		_, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_IN(:1,:2); END;",
			outer, sql.Out{Dest: &sum},
		)
		t.Logf("IN: sum=%d err=%v\n", sum, err)
	})

	// Separate OUT test, no nested input parameter.
	t.Run("count", func(t *testing.T) {
		var count int64
		qry := `
DECLARE
    v ` + pkg + `.NUM_TT_TT;
BEGIN
    ` + pkg + `.P_OUT(v);
    :1 := v.COUNT;
END;`
		if _, err := conn.ExecContext(ctx, qry, sql.Out{Dest: &count}); err != nil {
			t.Errorf("count error=%+v", err)
		} else {
			t.Logf("count=%d", count)
		}
	})

	t.Run("out_empty", func(t *testing.T) {
		out, err := numTtTt.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_OUT_EMPTY(:1); END;",
			sql.Out{Dest: out},
		); err != nil {
			t.Errorf("OUT: err=%+v", err)
		} else if n, e := out.Collection().Len(); e != nil {
			t.Errorf("length: err=%+v", e)
		} else {
			t.Logf("OUT_EMPTY length=%d err=%v\n", n, e)
		}
	})

	// Separate IN OUT test.
	t.Run("inout_noop", func(t *testing.T) {
		out, err := numTtTt.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_INOUT_NOOP(:1); END;",
			sql.Out{Dest: out, In: true},
		); err != nil {
			t.Errorf("INOUT_NOOP: err=%+v", err)
		}
	})

	t.Run("out", func(t *testing.T) {
		out, err := numTtTt.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_OUT(:1); END;",
			sql.Out{Dest: out},
		); err != nil {
			t.Errorf("OUT: err=%+v", err)
		} else if n, e := out.Collection().Len(); e != nil {
			t.Errorf("length: err=%+v", e)
		} else {
			t.Logf("OUT length=%d err=%v\n", n, e)
		}
	})

	t.Run("out_nt", func(t *testing.T) {
		typ, err := godror.GetObjectType(
			ctx, conn, pkg+".NUM_TT_NT",
		)
		if err != nil {
			t.Fatal(err)
		}
		out, err := typ.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_OUT_NT(:1); END;",
			sql.Out{Dest: out},
		); err != nil {
			t.Errorf("OUT_NT: err=%+v", err)
		} else if n, e := out.Collection().Len(); e != nil {
			t.Errorf("length: err=%+v", e)
		} else {
			t.Logf("OUT_NT length=%d err=%v\n", n, e)
		}
	})

	t.Run("out_va", func(t *testing.T) {
		typ, err := godror.GetObjectType(
			ctx, conn, pkg+".NUM_VA_AA",
		)
		if err != nil {
			t.Fatal(err)
		}
		out, err := typ.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_OUT_VA(:1); END;",
			sql.Out{Dest: out},
		); err != nil {
			t.Errorf("OUT_VA: err=%+v", err)
		} else if n, e := out.Collection().Len(); e != nil {
			t.Errorf("length: err=%+v", e)
		} else {
			t.Logf("OUT_VA length=%d err=%v\n", n, e)
		}
	})

	// Separate IN OUT test.
	t.Run("inout", func(t *testing.T) {
		out, err := numTtTt.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()

		if _, err = conn.ExecContext(
			ctx, "BEGIN "+pkg+".P_INOUT(:1); END;",
			sql.Out{Dest: out, In: true},
		); err != nil {
			t.Errorf("INOUT: err=%+v", err)
		}
	})

	t.Run("probe", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			mode int
		}{
			{"empty", 0},
			{"null-inner", 1},
			{"empty-inner", 2},
			{"single-inner", 3},
			{"dense-outer", 4},
			{"sparse-outer", 5},
			{"index-3-only", 6},
		} {
			t.Run(tc.name, func(t *testing.T) {
				out, err := numTtTt.NewObject()
				if err != nil {
					t.Fatal(err)
				}
				defer out.Close()

				_, err = conn.ExecContext(
					ctx,
					"BEGIN "+pkg+".p_out_probe(:1,:2); END;",
					tc.mode,
					sql.Out{Dest: out},
				)
				if err != nil {
					t.Errorf("case %d: %v", tc.mode, err)
					return
				}

				coll := out.Collection()
				n, err := coll.Len()
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("case=%d length=%d", tc.mode, n)

				if n > 0 {
					first, err := coll.First()
					if err != nil {
						t.Fatal(err)
					}
					last, err := coll.Last()
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("first=%d last=%d", first, last)
				}
			})
		}
	})

}

// z_plsql_types_nested_test.py
/*
import os
import oracledb

if os.environ.get("ORACLE_MODE") == "thick":
    oracledb.init_oracle_client()

conn = oracledb.connect(
    user=os.environ["ORA_USER"],
    password=os.environ["ORA_PASSWORD"],
    dsn=os.environ["ORA_DSN"],
)

print("Mode:", "thin" if conn.thin else "thick")
print("Database:", conn.version)

outer_type = conn.gettype("DEMO.TEST_ORACALL_NEST.NUM_TT_TT")

with conn.cursor() as cur:
    for case in (0, 1, 2, 3):
        out = cur.var(outer_type)

        try:
            cur.execute(
                """
                BEGIN
                    TEST_ORACALL_NEST.P_OUT_PROBE(:case_num, :result);
                END;
                """,
                case_num=case,
                result=out,
            )
            value = out.getvalue()
            print(case, "OK", value.asdict() if value else None)

        except oracledb.Error as exc:
            print(case, "FAIL", exc)

conn.close()
*/
