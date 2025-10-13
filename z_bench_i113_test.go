// Copyright 2020 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror"
)

func BenchmarkSelect113(b *testing.B) {
	StopConnStats()
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
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

	const qry = "SELECT /*+ FIRST_ROWS(1) */ F_cust_id, F_email, F_email_id FROM " + tbl
	const qry1 = "SELECT F_cust_id, F_email, F_email_id FROM " + tbl + " FETCH FIRST 1 ROW ONLY"
	b.Log(qry)
	F := func(b *testing.B, qry string, i int, params ...interface{}) {
		rows, err := testDb.QueryContext(ctx, qry, params...)
		if err != nil {
			b.Fatalf("%s: %+v", qry, err)
		}
		fetchRows(b, rows, (i+1)*100, i == 0)
	}

	b.ResetTimer()
	b.StartTimer()
	b.Run("a=?,p=?", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			F(b, qry, i)
			F(b, qry1, i)
		}
	})
	b.Run("a=?,p=2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			F(b, qry, i, godror.PrefetchCount(2))
			F(b, qry1, i, godror.PrefetchCount(2))
		}
	})

	b.Run("a=?,p=128", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			F(b, qry, i, godror.PrefetchCount(128))
			F(b, qry1, i, godror.PrefetchCount(128))
		}
	})

	for i := 128; i <= 8192; i *= 2 {
		arraySize := i
		b.Run(fmt.Sprintf("a=%d,p=%d", i, i+1), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				F(b, qry, i, godror.FetchArraySize(arraySize), godror.PrefetchCount(arraySize+1))
				F(b, qry1, i, godror.FetchArraySize(arraySize), godror.PrefetchCount(arraySize+1))
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
	var bytes int64
	var t time.Time
	var d1 time.Duration
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
		if first && d1 == 0 {
			d1 = time.Since(t)
		}
		n++
		bytes += 8 + int64(len(email)) + 8
		if n == uint64(maxRows) {
			if first && d1 != 0 {
				b.ReportMetric(float64(d1/time.Microsecond), "firstRecMs")
			}
			b.SetBytes(bytes)
			break
		}
	}
	if first && n == 1 && d1 != 0 {
		b.ReportMetric(float64(time.Since(t)/time.Microsecond), "oneRowMs")
	}
	rows.Close()
	//b.Logf("Selected %d records in %s.", n, time.Since(t))
}
