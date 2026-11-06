// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestBindTimeAsDateStoresSameDate(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	saoPaulo, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatal(err)
	}
	values := []godror.NullTime{
		{Time: time.Date(2026, 11, 5, 10, 20, 30, 0, saoPaulo), Valid: true},
		{Time: time.Date(2018, 11, 4, 0, 30, 0, 0, saoPaulo), Valid: true},
		{Time: time.Date(2018, 11, 4, 1, 0, 0, 0, saoPaulo), Valid: true},
		{Time: time.Date(2019, 2, 16, 23, 0, 0, 0, saoPaulo), Valid: true},
		{Time: time.Date(2019, 2, 16, 23, 30, 0, 0, saoPaulo), Valid: true},
		{Time: time.Date(2026, 1, 1, 12, 0, 0, 999999999, saoPaulo), Valid: true},
		{Time: time.Date(2026, 6, 30, 23, 59, 59, 500000000, time.UTC), Valid: true},
		{Time: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		{Time: time.Date(9999, 12, 31, 20, 0, 0, 0, time.UTC), Valid: true},
		{},
	}

	for _, tz := range []*time.Location{saoPaulo, time.UTC} {
		t.Run(tz.String(), func(t *testing.T) {
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.Timezone = tz
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()

			tbl := "test_bind_date" + tblSuffix
			_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
			if _, err := db.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(4), via VARCHAR2(10), d DATE)"); err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE "+tbl) }()

			ids := make([]int, len(values))
			for i := range ids {
				ids[i] = i
			}
			insert := "INSERT INTO " + tbl + " (id, via, d) VALUES (:1, :2, :3)"
			for _, via := range []string{"tstz", "date"} {
				vias := make([]string, len(values))
				for i := range vias {
					vias[i] = via
				}
				args := []any{ids, vias, values}
				if via == "date" {
					args = append(args, godror.BindTimeAsDate())
				}
				if _, err := db.ExecContext(ctx, insert, args...); err != nil {
					t.Fatalf("%s: %+v", via, err)
				}
				for i, value := range values {
					args := []any{100 + i, via, value}
					if via == "date" {
						args = append(args, godror.BindTimeAsDate())
					}
					if _, err := db.ExecContext(ctx, insert, args...); err != nil {
						t.Fatalf("%s %d: %+v", via, i, err)
					}
				}
			}

			rows, err := db.QueryContext(ctx, "SELECT a.id, DUMP(a.d), DUMP(b.d) FROM "+tbl+" a JOIN "+tbl+" b ON b.id = a.id"+
				" WHERE a.via = 'tstz' AND b.via = 'date' ORDER BY a.id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var compared int
			for rows.Next() {
				var id int
				var tstz, date string
				if err := rows.Scan(&id, &tstz, &date); err != nil {
					t.Fatal(err)
				}
				compared++
				if tstz != date {
					value := values[id%100]
					t.Errorf("%d (%v): TIMESTAMP_TZ stored %s, DATE stored %s", id, value, tstz, date)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if want := 2 * len(values); compared != want {
				t.Fatalf("compared %d rows, wanted %d", compared, want)
			}
		})
	}
}
