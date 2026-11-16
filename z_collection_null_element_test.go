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

type (
	collNullKid struct {
		ID   sql.NullInt64 `godror:"ID"`
		Tags []string      `godror:"TAGS"`
	}
	collNullRec struct {
		Strs  []string          `godror:"STRS"`
		Nums  []sql.NullInt64   `godror:"NUMS"`
		Dates []godror.NullTime `godror:"DATES"`
		Kids  []collNullKid     `godror:"KIDS"`
	}
	collNullStrs []string
)

var collNullTyp = map[string]string{}

func (collNullRec) ObjectTypeName() string { return collNullTyp["REC"] }
func (r collNullRec) WriteObject(o *godror.Object) error {
	return godror.StructWriteObject(o, &r)
}
func (collNullStrs) ObjectTypeName() string { return collNullTyp["STRS"] }
func (s collNullStrs) WriteObject(o *godror.Object) error {
	return godror.SliceWriteObject(o, []string(s))
}

// TestCollectionNullElement writes collections with NULL elements (and empty
// strings) through one statement several times: the bind object is reused,
// and a NULL must be written at a position that has an element already.
func TestCollectionNullElement(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	sfx := strings.ToUpper(tblSuffix + "_" + strconv.FormatInt(time.Now().UnixNano()%1e12, 36))
	names := map[string]string{}
	order := []string{"STRS", "NUMS", "DATES", "KIDS", "KID", "REC", "FS", "F"}
	for _, k := range order {
		names[k] = "TEST_CNE_" + k + sfx
	}
	collNullTyp = names
	repl := func(s string) string {
		for _, k := range order {
			s = strings.ReplaceAll(s, "%"+k+"%", names[k])
		}
		return s
	}
	cleanup := func() {
		for _, qry := range []string{
			"DROP FUNCTION %F%", "DROP FUNCTION %FS%", "DROP TYPE %REC% FORCE", "DROP TYPE %KIDS% FORCE",
			"DROP TYPE %KID% FORCE", "DROP TYPE %DATES% FORCE", "DROP TYPE %NUMS% FORCE", "DROP TYPE %STRS% FORCE",
		} {
			testDb.ExecContext(context.Background(), repl(qry))
		}
	}
	cleanup()
	defer cleanup()
	for _, qry := range []string{
		"CREATE TYPE %STRS% AS TABLE OF VARCHAR2(30)",
		"CREATE TYPE %NUMS% AS TABLE OF NUMBER(9)",
		"CREATE TYPE %DATES% AS TABLE OF DATE",
		"CREATE TYPE %KID% AS OBJECT (id NUMBER(9), tags %STRS%)",
		"CREATE TYPE %KIDS% AS TABLE OF %KID%",
		"CREATE TYPE %REC% AS OBJECT (strs %STRS%, nums %NUMS%, dates %DATES%, kids %KIDS%)",
		`CREATE FUNCTION %FS%(p IN %STRS%) RETURN VARCHAR2 IS
  r VARCHAR2(1000);
BEGIN
  IF p IS NULL THEN RETURN '-'; END IF;
  FOR i IN 1..p.COUNT LOOP r := r||NVL(p(i), '~')||','; END LOOP;
  RETURN '['||r||']';
END;`,
		`CREATE FUNCTION %F%(p IN %REC%) RETURN VARCHAR2 IS
  r VARCHAR2(4000);
BEGIN
  r := %FS%(p.strs)||' [';
  FOR i IN 1..p.nums.COUNT LOOP r := r||NVL(TO_CHAR(p.nums(i)), '~')||','; END LOOP;
  r := r||'] [';
  FOR i IN 1..p.dates.COUNT LOOP r := r||NVL(TO_CHAR(p.dates(i), 'YYYYMMDD'), '~')||','; END LOOP;
  r := r||'] [';
  FOR i IN 1..p.kids.COUNT LOOP
    r := r||NVL(TO_CHAR(p.kids(i).id), '~')||%FS%(p.kids(i).tags)||',';
  END LOOP;
  RETURN r||']';
END;`,
	} {
		if _, err := testDb.ExecContext(ctx, repl(qry)); err != nil {
			t.Fatalf("%s: %+v", repl(qry), err)
		}
	}

	// The values: 0 and "" are NULL.
	strs := func(ss []string) string {
		var buf strings.Builder
		for _, s := range ss {
			if s == "" {
				s = "~"
			}
			buf.WriteString(s + ",")
		}
		return "[" + buf.String() + "]"
	}
	num := func(n int) sql.NullInt64 { return sql.NullInt64{Int64: int64(n), Valid: n != 0} }
	numS := func(n int) string {
		if n == 0 {
			return "~"
		}
		return strconv.Itoa(n)
	}
	mk := func(ss []string, nn []int) (collNullRec, string) {
		rec := collNullRec{Strs: ss}
		var nums, dates, kids strings.Builder
		for i, n := range nn {
			rec.Nums = append(rec.Nums, num(n))
			nums.WriteString(numS(n) + ",")
			var d godror.NullTime
			if n == 0 {
				dates.WriteString("~,")
			} else {
				d = godror.NullTime{Valid: true, Time: time.Date(2024, 1, n, 12, 0, 0, 0, time.Local)}
				dates.WriteString(d.Time.Format("20060102") + ",")
			}
			rec.Dates = append(rec.Dates, d)
			// The tags of the kids are the strings, rotated.
			tags := append(append([]string(nil), ss[i%len(ss):]...), ss[:i%len(ss)]...)
			rec.Kids = append(rec.Kids, collNullKid{ID: num(n), Tags: tags})
			kids.WriteString(numS(n) + strs(tags) + ",")
		}
		return rec, strs(ss) + " [" + nums.String() + "] [" + dates.String() + "] [" + kids.String() + "]"
	}
	type value struct {
		strs []string
		nums []int
	}
	values := []value{
		{[]string{"", "a"}, []int{0, 1}},
		{[]string{"b", "c"}, []int{2, 3}},
		{[]string{"d", ""}, []int{4, 0}},
		{[]string{"", "", "e", ""}, []int{0, 0, 5, 0}},
		{[]string{"", "x", ""}, []int{0, 9, 0}},
		{[]string{""}, []int{0}},
		{[]string{"", "f"}, []int{0, 6}},
		{[]string{"", ""}, []int{0, 0}},
		{[]string{"g", "h"}, []int{7, 8}},
	}

	for _, mode := range []string{"prepared", "exec"} {
		t.Run(mode, func(t *testing.T) {
			conn, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			execer := func(qry string) func(args ...any) error {
				if mode == "exec" {
					return func(args ...any) error {
						_, err := conn.ExecContext(ctx, qry, args...)
						return err
					}
				}
				stmt, err := conn.PrepareContext(ctx, qry)
				if err != nil {
					t.Fatalf("%s: %+v", qry, err)
				}
				t.Cleanup(func() { stmt.Close() })
				return func(args ...any) error {
					_, err := stmt.ExecContext(ctx, args...)
					return err
				}
			}
			execRec := execer(repl("BEGIN :1 := %F%(:2); END;"))
			execStrs := execer(repl("BEGIN :1 := %FS%(:2); END;"))
			for i, v := range values {
				rec, want := mk(v.strs, v.nums)
				var got string
				if err := execRec(sql.Out{Dest: &got}, rec); err != nil {
					t.Errorf("%d. struct %q %v: %+v", i, v.strs, v.nums, err)
				} else if got != want {
					t.Errorf("%d. struct: the server got\n\t%s, wanted\n\t%s", i, got, want)
				}
				if err := execStrs(sql.Out{Dest: &got}, collNullStrs(v.strs)); err != nil {
					t.Errorf("%d. slice %q: %+v", i, v.strs, err)
				} else if want = strs(v.strs); got != want {
					t.Errorf("%d. slice: the server got %s, wanted %s", i, got, want)
				}
			}
		})
	}
}
