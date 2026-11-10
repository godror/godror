// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

//go:build linux

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func BenchmarkArrayInsertStrings(b *testing.B) {
	benchArrayInsert(b, "test_arr_str", "s1 VARCHAR2(40), s2 VARCHAR2(40), s3 VARCHAR2(40), n1 NUMBER, b1 RAW(40)",
		func(rows int) []any {
			s1, s2, s3 := make([]string, rows), make([]string, rows), make([]string, rows)
			n1, b1 := make([]godror.Number, rows), make([][]byte, rows)
			for i := range rows {
				s1[i] = strconv.Itoa(i)
				s2[i] = strings.Repeat("x", 1+i%40)
				if i%10 != 0 {
					s3[i] = fmt.Sprintf("row-%08d", i)
				}
				n1[i] = godror.Number(strconv.Itoa(i * 7))
				b1[i] = []byte(s2[i])
			}
			return []any{s1, s2, s3, n1, b1}
		})
}

func BenchmarkArrayInsertNumbersDates(b *testing.B) {
	benchArrayInsert(b, "test_arr_num", "i1 NUMBER(15), i2 NUMBER(15), f1 BINARY_DOUBLE, d1 DATE, d2 DATE",
		func(rows int) []any {
			i1, i2 := make([]int64, rows), make([]sql.NullInt64, rows)
			f1 := make([]float64, rows)
			d1, d2 := make([]time.Time, rows), make([]godror.NullTime, rows)
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)
			for i := range rows {
				i1[i] = int64(i) * 7919
				i2[i] = sql.NullInt64{Int64: int64(i), Valid: i%10 != 0}
				f1[i] = float64(i) / 7
				d1[i] = start.AddDate(0, 0, i%300)
				d2[i] = godror.NullTime{Time: d1[i], Valid: i%10 != 0}
			}
			return []any{i1, i2, f1, d1, d2, godror.BindTimeAsDate()}
		})
}

func benchArrayInsert(b *testing.B, name, columns string, values func(rows int) []any) {
	ctx, cancel := testContext(b, 10*time.Minute)
	defer cancel()
	tbl := name + tblSuffix
	_, _ = testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" ("+columns+")"); err != nil {
		b.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	qry := "INSERT INTO " + tbl + " VALUES (:1, :2, :3, :4, :5)"

	for _, rows := range []int{100, 1000, 10000} {
		args := values(rows)
		b.Run(strconv.Itoa(rows), func(b *testing.B) {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			stmt, err := tx.PrepareContext(ctx, qry)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ReportAllocs()
			start := processCPU()
			b.ResetTimer()
			for b.Loop() {
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					b.Fatal(err)
				}
			}
			cpu := processCPU() - start
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N*rows), "cpu-ns/row")
		})
	}
}

func BenchmarkSelectScan(b *testing.B) {
	ctx, cancel := testContext(b, 10*time.Minute)
	defer cancel()
	const rows = 100000
	tbl := "test_sel_scan" + tblSuffix
	_, _ = testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+
		" (s1 VARCHAR2(40), s2 VARCHAR2(40), i1 NUMBER(15), i2 NUMBER(15), d1 NUMBER(18,2), t1 DATE)"); err != nil {
		b.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+
		" SELECT 'row-'||level, RPAD('x', MOD(level, 40) + 1, 'y'), level * 7919, DECODE(MOD(level, 10), 0, NULL, level),"+
		" ROUND(level / 7, 2), DATE '2026-01-01' + MOD(level, 300) FROM DUAL CONNECT BY level <= "+strconv.Itoa(rows)); err != nil {
		b.Fatal(err)
	}
	qry := "SELECT s1, s2, i1, i2, d1, t1 FROM " + tbl

	var s1, s2 string
	var i1 int64
	var i2 sql.NullInt64
	var d1 godror.Number
	var t1 time.Time
	values := make([]any, 6)
	for name, dest := range map[string][]any{
		"any":   {&values[0], &values[1], &values[2], &values[3], &values[4], &values[5]},
		"typed": {&s1, &s2, &i1, &i2, &d1, &t1},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			start := processCPU()
			n := 0
			for b.Loop() {
				r, err := testDb.QueryContext(ctx, qry, godror.FetchArraySize(1000), godror.PrefetchCount(1001))
				if err != nil {
					b.Fatal(err)
				}
				for r.Next() {
					if err := r.Scan(dest...); err != nil {
						b.Fatal(err)
					}
					n++
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64((processCPU()-start).Nanoseconds())/float64(n), "cpu-ns/row")
		})
	}
}

func processCPU() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
