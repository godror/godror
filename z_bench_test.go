package goracle_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	goracle "gopkg.in/goracle.v2"
)

func BenchmarkPlSQLArrayInsert25(b *testing.B) {
	defer func() {
		testDb.Exec("DROP TABLE tst_bench_25_tbl")
		testDb.Exec("DROP PACKAGE tst_bench_25")
	}()

	for _, qry := range []string{
		`DROP TABLE tst_bench_25_tbl`,
		`CREATE TABLE tst_bench_25_tbl (dt DATE, st VARCHAR2(255),
		  ip NUMBER(12), zone NUMBER(3), plan NUMBER(3), banner NUMBER(3),
		  referrer VARCHAR2(255), country VARCHAR2(80), region VARCHAR2(10))`,

		`CREATE OR REPLACE PACKAGE tst_bench_25 IS
TYPE cx_array_date IS TABLE OF DATE INDEX BY BINARY_INTEGER;

TYPE cx_array_string IS TABLE OF VARCHAR2 (1000) INDEX BY BINARY_INTEGER;

TYPE cx_array_num IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;

PROCEDURE P_BULK_INSERT_IMP (VIMP_DATES       cx_array_date,
                                VIMP_KEYS        cx_array_string,
                                VIMP_IP          cx_array_num,
                                VIMP_ZONE        cx_array_num,
                                VIMP_PLAN        cx_array_num,
                                VIMP_BANNER      cx_array_num,
                                VIMP_REFERRER    cx_array_string,
                                VIMP_COUNTRY     cx_array_string,
                                VIMP_REGION      cx_array_string);
END;`,
		`CREATE OR REPLACE PACKAGE BODY tst_bench_25 IS
PROCEDURE P_BULK_INSERT_IMP (VIMP_DATES       cx_array_date,
                             VIMP_KEYS        cx_array_string,
                             VIMP_IP          cx_array_num,
                             VIMP_ZONE        cx_array_num,
                             VIMP_PLAN        cx_array_num,
                             VIMP_BANNER      cx_array_num,
                             VIMP_REFERRER    cx_array_string,
                             VIMP_COUNTRY     cx_array_string,
                             VIMP_REGION      cx_array_string) IS
  i PLS_INTEGER;
BEGIN
  i := vimp_dates.FIRST;
  WHILE i IS NOT NULL LOOP
    INSERT INTO tst_bench_25_tbl
	  (dt, st, ip, zone, plan, banner, referrer, country, region)
	  VALUES (vimp_dates(i), vimp_keys(i), vimp_ip(i), vimp_zone(i), vimp_plan(i),
	          vimp_banner(i), vimp_referrer(i), vimp_country(i), vimp_region(i));
    i := vimp_dates.NEXT(i);
  END LOOP;

END;

END tst_bench_25;`,
	} {

		if _, err := testDb.Exec(qry); err != nil {
			if strings.HasPrefix(qry, "DROP TABLE ") {
				continue
			}
			b.Fatal(errors.Wrap(err, qry))
		}
	}

	qry := `BEGIN tst_bench_25.P_BULK_INSERT_IMP (:1, :2, :3, :4, :5, :6, :7, :8, :9); END;`

	pt1 := time.Now()
	n := 512
	dates := make([]time.Time, n)
	keys := make([]string, n)
	ips := make([]int, n)
	zones := make([]int, n)
	plans := make([]int, n)
	banners := make([]int, n)
	referrers := make([]string, n)
	countries := make([]string, n)
	regions := make([]string, n)
	for i := range dates {
		dates[i] = pt1.Add(time.Duration(i) * time.Second)
		keys[i] = "key"
		ips[i] = 123456
		zones[i] = i % 256
		plans[i] = (i / 2) % 1000
		banners[i] = (i * 3) % 1000
		referrers[i] = "referrer"
		countries[i] = "country"
		regions[i] = "region"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tx.ExecContext(ctx, qry,
			goracle.PlSQLArrays,
			dates, keys, ips, zones, plans, banners, referrers, countries, regions,
		); err != nil {
			if strings.Contains(err.Error(), "PLS-00905") || strings.Contains(err.Error(), "ORA-06508") {
				b.Log(goracle.GetCompileErrors(testDb, false))
			}
			//b.Log(dates, keys, ips, zones, plans, banners, referrers, countries, regions)
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func shortenFloat(s string) string {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return s
	}
	for j := i + 1; j < len(s); j++ {
		if s[j] != '0' {
			return s
		}
	}
	return s[:i]
}

const bFloat = 12345.6789

func BenchmarkSprintfFloat(b *testing.B) {
	var length int64
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("%f", bFloat)
		s = shortenFloat(s)
		length += int64(len(s))
	}
}

/*
func BenchmarkAppendFloat(b *testing.B) {
	var length int64
	for i := 0; i < b.N; i++ {
		s := printFloat(bFloat)
		length += int64(len(s))
	}
}
*/
