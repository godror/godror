// Copyright 2021 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror"
)

func BenchmarkSelect113(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const tbl = "test_bench_113"
	{
		testDb.ExecContext(ctx, "DROP TABLE "+tbl)
		const qry = "CREATE TABLE " + tbl + " (F_cust_id NUMBER(19) NOT NULL, F_email VARCHAR2(255) NOT NULL, F_email_id NUMBER(19) NOT NULL)"
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			b.Fatalf("%s: %+v", qry, err)
		}
		defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}
	{
		const qry = "INSERT /*+ APPEND */ INTO " + tbl + " (F_cust_id, F_email, F_email_id) VALUES (:1, :2, :3)"
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatalf("%s: %+v", qry, err)
		}
		defer stmt.Close()
		ids, emailIDs := make([]uint64, 1000), make([]uint64, 1000)
		emails := make([]string, 1000)
		var n uint64
		t := time.Now()
		for i := 0; i < 10; i++ {
			start := uint64(i * 2000)
			for j := uint64(0); j < 1000; j++ {
				ids[j], emailIDs[j] = start+2*j, start+2*j+1
				emails[j] = strconv.FormatUint(start+2*j+1, 10) + "@example.com"
			}
			if _, err := stmt.ExecContext(ctx, ids, emails, emailIDs); err != nil {
				b.Fatalf("%s: %+v", qry, err)
			}
			n += 1000
		}
		b.Logf("Inserted %d records into %s in %s.", n, tbl, time.Since(t))
	}

	const qry = "SELECT F_cust_id, F_email, F_email_id FROM " + tbl + " ORDER BY DBMS_RANDOM.RANDOM"
	b.Log(qry)
	b.StartTimer()
	b.Run("simple", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := testDb.QueryContext(ctx, qry)
			if err != nil {
				b.Fatalf("%s: %+v", qry, err)
			}
			fetchRows(b, rows, (i+1)*100, i == 0)
		}
	})

	for i := 2; i < 8192; i *= 8 {
		arraySize := i
		b.Run("prefetch-"+strconv.Itoa(i), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rows, err := testDb.QueryContext(ctx, qry, godror.FetchArraySize(arraySize), godror.PrefetchCount(arraySize+1))
				if err != nil {
					b.Fatalf("%s: %+v", qry, err)
				}
				fetchRows(b, rows, (i+1)*100, i == 0)
			}
		})
	}
}

func fetchRows(b *testing.B, rows *sql.Rows, maxRows int, first bool) {
	if maxRows == 0 {
		rows.Close()
		return
	}
	defer rows.Close()
	var n uint64
	var t time.Time
	if first {
		t = time.Now()
	}
	for rows.Next() {
		var id, emailID uint64
		var email string
		if err := rows.Scan(&id, &email, &emailID); err != nil {
			rows.Close()
			b.Fatalf("%+v", err)
		}
		if first && !t.IsZero() {
			b.ReportMetric(float64(time.Since(t)/time.Microsecond), "firstRecordMicros")
			t = time.Time{}
		}
		n++
		if n == uint64(maxRows) {
			break
		}
	}
	rows.Close()
	b.ReportMetric(float64(n), "record")
	//b.Logf("Selected %d records in %s.", n, time.Since(t))
}
