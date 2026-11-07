// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

//go:build linux

package godror_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func BenchmarkArrayInsertStrings(b *testing.B) {
	ctx, cancel := testContext(b, 10*time.Minute)
	defer cancel()
	tbl := "test_arr_str" + tblSuffix
	_, _ = testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (s1 VARCHAR2(40), s2 VARCHAR2(40), s3 VARCHAR2(40), n1 NUMBER, b1 RAW(40))"); err != nil {
		b.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	qry := "INSERT INTO " + tbl + " (s1, s2, s3, n1, b1) VALUES (:1, :2, :3, :4, :5)"

	for _, rows := range []int{100, 1000, 10000} {
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
			var before, after syscall.Rusage
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			b.ResetTimer()
			for b.Loop() {
				if _, err := stmt.ExecContext(ctx, s1, s2, s3, n1, b1); err != nil {
					b.Fatal(err)
				}
			}
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			cpu := time.Duration(after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano())
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N*rows), "cpu-ns/row")
		})
	}
}
