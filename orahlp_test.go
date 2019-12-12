// Copyright 2017 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package goracle

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMapToSlice(t *testing.T) {
	for i, tc := range []struct {
		in, await string
		params    []interface{}
	}{
		{
			`SELECT NVL(MAX(F_dazon), :dazon) FROM T_spl_level
			WHERE (F_spl_azon = :lev_azon OR --:lev_azon OR
			       F_ssz = 0 AND F_lev_azon = /*:lev_azon*/:lev_azon)`,
			`SELECT NVL(MAX(F_dazon), :1) FROM T_spl_level
			WHERE (F_spl_azon = :2 OR --:lev_azon OR
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

		got, params := MapToSlice(tc.in, func(s string) interface{} { return s })
		d := cmp.Diff(tc.await, got)
		if d != "" {
			t.Errorf("%d. diff:\n%s", i, d)
		}
		if !reflect.DeepEqual(params, tc.params) {
			t.Errorf("%d. params: got\n\t%#v,\nwanted\n\t%#v.", i, params, tc.params)
		}
	}
}
