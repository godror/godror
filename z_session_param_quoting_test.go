// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestSessionParamQuoting sets session parameters with SetSessionParamOnInit
// and with NewSessionIniter: the session must get exactly the value, or the
// statement must fail; a value must never set another parameter.
func TestSessionParamQuoting(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()
	const qry = `SELECT TO_CHAR(DATE '2026-01-02'), TO_CHAR(1.5), SESSIONTIMEZONE,
		SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'), SYS_CONTEXT('USERENV', 'CURRENT_EDITION_NAME') FROM DUAL`
	const date, num, tz, schema, edition = 0, 1, 2, 3, 4
	// what a plain session has
	var plain [5]string
	get := func(P godror.ConnectionParams) (got [5]string, err error) {
		P.StandaloneConnection = godror.Bool(true)
		db := sql.OpenDB(godror.NewConnector(P))
		defer db.Close()
		err = db.QueryRowContext(ctx, qry).Scan(&got[0], &got[1], &got[2], &got[3], &got[4])
		return got, err
	}
	P0, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if plain, err = get(P0); err != nil {
		t.Fatal(err)
	}
	const inject = ` NLS_NUMERIC_CHARACTERS=', '`
	for _, tC := range []struct {
		key, value string
		col        int
		want       string // "" means that the statement must fail
	}{
		{"NLS_DATE_FORMAT", `YYYY "it's" MM`, date, "2026 it's 01"},
		{"NLS_DATE_FORMAT", `YYYY;MM"(x)'"`, date, "2026;01(x)'"},
		{"NLS_DATE_FORMAT", `YYYY"'` + inject + `"`, date, "2026'" + inject},
		{"nls_date_format", "YYYY", date, "2026"},
		{"NLS_NUMERIC_CHARACTERS", ", ", num, "1,5"},
		{"TIME_ZONE", "+05:30", tz, "+05:30"},
		{"TIME_ZONE", "UTC", tz, "UTC"},
		{"TIME_ZONE", "dbtimezone", num, plain[num]},
		{"TIME_ZONE", "LOCAL" + inject, 0, ""},
		{"CURRENT_SCHEMA", "SYSTEM", schema, "SYSTEM"},
		{"current_schema", `"SYSTEM"`, schema, "SYSTEM"},
		{"CURRENT_SCHEMA", "SYSTEM" + inject, 0, ""},
		{"CURRENT_SCHEMA", `"SYSTEM"` + inject, 0, ""},
		{"EDITION", "ORA$BASE", edition, "ORA$BASE"},
		{"EDITION", "ORA$BASE" + inject, 0, ""},
		{"ISOLATION_LEVEL", "SERIALIZABLE", num, plain[num]},
		{"ISOLATION_LEVEL", "READ COMMITTED", num, plain[num]},
		{"ISOLATION_LEVEL", "SERIALIZABLE" + inject, 0, ""},
		{"CONSTRAINTS", "IMMEDIATE", num, plain[num]},
		{"TIMED_STATISTICS", "TRUE", num, plain[num]},
		{"TIMED_STATISTICS", "TRUE" + inject, 0, ""},
		{"OPTIMIZER_INDEX_COST_ADJ", "50", num, plain[num]},
		{"CURSOR_SHARING", "FORCE", num, plain[num]},
	} {
		viaParams, viaIniter := P0, P0
		viaParams.SetSessionParamOnInit(tC.key, tC.value)
		viaIniter.OnInit = godror.NewSessionIniter(map[string]string{tC.key: tC.value})
		for name, P := range map[string]godror.ConnectionParams{"SetSessionParamOnInit": viaParams, "NewSessionIniter": viaIniter} {
			got, err := get(P)
			if tC.want == "" {
				var oe *godror.OraErr
				if !errors.As(err, &oe) {
					t.Errorf("%s %s=%q: got %q, %v; wanted an ORA error", name, tC.key, tC.value, got, err)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s %s=%q: %v", name, tC.key, tC.value, err)
				continue
			}
			want := plain
			want[tC.col] = tC.want
			if tC.key == "TIME_ZONE" && tC.col != tz {
				want[tz] = got[tz]
			}
			if got != want {
				t.Errorf("%s %s=%q: got %q, wanted %q", name, tC.key, tC.value, got, want)
			}
		}
	}
}
