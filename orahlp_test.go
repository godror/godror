// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"github.com/google/go-cmp/cmp"
)

func TestMapToSlice(t *testing.T) {
	for i, tc := range []struct {
		in, await string
		params    []interface{}
	}{
		{
			`SELECT NVL(MAX(F_dazon), :dazon) FROM T_spl_level
			WHERE (to_char(CURRENT_DATE, 'HH:MM') = '00:42' AND F_spl_azon = :lev_azon OR --:lev_azon OR
			       F_ssz = 0 AND F_lev_azon = /*:lev_azon*/:lev_azon)`,
			`SELECT NVL(MAX(F_dazon), :1) FROM T_spl_level
			WHERE (to_char(CURRENT_DATE, 'HH:MM') = '00:42' AND F_spl_azon = :2 OR --:lev_azon OR
			       F_ssz = 0 AND F_lev_azon = /*:lev_azon*/:3)`,
			[]interface{}{"dazon", "lev_azon", "lev_azon"},
		},

		{
			`INSERT INTO PERSON(NAME) VALUES('hello') RETURNING ID INTO :ID`,
			`INSERT INTO PERSON(NAME) VALUES('hello') RETURNING ID INTO :1`,
			[]interface{}{"ID"},
		},

		{
			`DECLARE
  i1 PLS_INTEGER;
  i2 PLS_INTEGER;
  v001 BRUNO.DB_WEB_ELEKTR.KOTVENY_REC_TYP;

BEGIN
  v001.dijkod := :p002#dijkod;

  DB_web.sendpreoffer_31101(p_kotveny=>v001);

  :p002#dijkod := v001.dijkod;

END;
`,
			`DECLARE
  i1 PLS_INTEGER;
  i2 PLS_INTEGER;
  v001 BRUNO.DB_WEB_ELEKTR.KOTVENY_REC_TYP;

BEGIN
  v001.dijkod := :1;

  DB_web.sendpreoffer_31101(p_kotveny=>v001);

  :2 := v001.dijkod;

END;
`,
			[]interface{}{"p002#dijkod", "p002#dijkod"},
		},
	} {

		got, params := godror.MapToSlice(tc.in, func(s string) interface{} { return s })
		d := cmp.Diff(tc.await, got)
		if d != "" {
			t.Errorf("%d. diff:\n%s", i, d)
		}
		if !reflect.DeepEqual(params, tc.params) {
			t.Errorf("%d. params: got\n\t%#v,\nwanted\n\t%#v.", i, params, tc.params)
		}
	}
}

func TestConnPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ConnPool"), 10*time.Second)
	defer cancel()

	p := godror.NewConnPool(testDb, 2)
	defer p.Close()

	var savedC1 *sql.Conn
	func() {
		c1, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = c1.Close(); err != nil {
			t.Fatal(err)
		}
		if c1, err = p.Conn(ctx); err != nil {
			t.Fatal(err)
		}
		if err = c1.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		savedC1 = c1.Conn
		c2, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		if c1 == c2 {
			t.Fatal("c1==c2")
		}

		old := c2.Conn
		if err := c2.Close(); err != nil {
			t.Fatal(err)
		}
		if c2, err = p.Conn(ctx); err != nil {
			t.Fatal(err)
		}
		if c2.Conn != old {
			t.Errorf("re-acquire got %p wanted %p", c2.Conn, old)
		}
		defer c2.Close()
		old = nil
		_ = old

		c3, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c3.Close()
		if err = c3.PingContext(ctx); err != nil {
			t.Error(err)
		}
	}()

	if err := savedC1.PingContext(ctx); err != nil {
		t.Error(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := savedC1.PingContext(ctx); err == nil {
		t.Log("connection should be closed after the pool is closed, but works")
	}
}
