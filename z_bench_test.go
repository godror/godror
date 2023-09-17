// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// go install && go test -c && ./godror.v2.test -test.run=^$ -test.bench=Insert25 -test.cpuprofile=/tmp/insert25.prof && go tool pprof ./godror.v2.test /tmp/insert25.prof

func BenchmarkPlSQLArrayInsert25(b *testing.B) {
	defer func() {
		//testDb.Exec("DROP TABLE tst_bench_25_tbl")
		testDb.Exec("DROP PACKAGE tst_bench_25")
	}()

	for _, qry := range []string{
		//`DROP TABLE tst_bench_25_tbl`,
		/*`CREATE TABLE tst_bench_25_tbl (dt DATE, st VARCHAR2(255),
		  ip NUMBER(12), zone NUMBER(3), plan NUMBER(3), banner NUMBER(3),
		  referrer VARCHAR2(255), country VARCHAR2(80), region VARCHAR2(10))`,*/

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
  /*
    INSERT INTO tst_bench_25_tbl
	  (dt, st, ip, zone, plan, banner, referrer, country, region)
	  VALUES (vimp_dates(i), vimp_keys(i), vimp_ip(i), vimp_zone(i), vimp_plan(i),
	          vimp_banner(i), vimp_referrer(i), vimp_country(i), vimp_region(i));
  */
    i := vimp_dates.NEXT(i);
  END LOOP;

END;

END tst_bench_25;`,
	} {

		if _, err := testDb.Exec(qry); err != nil {
			if strings.HasPrefix(qry, "DROP TABLE ") {
				continue
			}
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
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
	ctx = godror.ContextWithLogger(ctx, nil)
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	b.ResetTimer()
	for i := 0; i < b.N; i += n {
		if _, err := tx.ExecContext(ctx, qry,
			godror.PlSQLArrays,
			dates, keys, ips, zones, plans, banners, referrers, countries, regions,
		); err != nil {
			if strings.Contains(err.Error(), "PLS-00905") || strings.Contains(err.Error(), "ORA-06508") {
				b.Log(godror.GetCompileErrors(ctx, testDb, false))
			}
			//b.Log(dates, keys, ips, zones, plans, banners, referrers, countries, regions)
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// go install && go test -c && ./godror.v2.test -test.run=^. -test.bench=InOut -test.cpuprofile=/tmp/inout.prof && go tool pprof -cum ./godror.v2.test /tmp/inout.prof

func BenchmarkPlSQLArrayInOut(b *testing.B) {
	defer func() {
		testDb.Exec("DROP PACKAGE tst_bench_inout")
	}()

	for _, qry := range []string{
		`CREATE OR REPLACE PACKAGE tst_bench_inout IS
TYPE cx_array_date IS TABLE OF DATE INDEX BY BINARY_INTEGER;

TYPE cx_array_string IS TABLE OF VARCHAR2 (1000) INDEX BY BINARY_INTEGER;

TYPE cx_array_num IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;

PROCEDURE P_BULK_INSERT_IMP (VIMP_DATES       IN OUT NOCOPY cx_array_date,
                             VIMP_KEYS        IN OUT NOCOPY cx_array_string,
                             VIMP_IP          IN OUT NOCOPY cx_array_num,
                             VIMP_ZONE        IN OUT NOCOPY cx_array_num,
                             VIMP_PLAN        IN OUT NOCOPY cx_array_num,
                             VIMP_BANNER      IN OUT NOCOPY cx_array_num,
                             VIMP_REFERRER    IN OUT NOCOPY cx_array_string,
                             VIMP_COUNTRY     IN OUT NOCOPY cx_array_string,
                             VIMP_REGION      IN OUT NOCOPY cx_array_string);
END;`,
		`CREATE OR REPLACE PACKAGE BODY tst_bench_inout IS
PROCEDURE P_BULK_INSERT_IMP (VIMP_DATES       IN OUT NOCOPY cx_array_date,
                             VIMP_KEYS        IN OUT NOCOPY cx_array_string,
                             VIMP_IP          IN OUT NOCOPY cx_array_num,
                             VIMP_ZONE        IN OUT NOCOPY cx_array_num,
                             VIMP_PLAN        IN OUT NOCOPY cx_array_num,
                             VIMP_BANNER      IN OUT NOCOPY cx_array_num,
                             VIMP_REFERRER    IN OUT NOCOPY cx_array_string,
                             VIMP_COUNTRY     IN OUT NOCOPY cx_array_string,
                             VIMP_REGION      IN OUT NOCOPY cx_array_string) IS
  i PLS_INTEGER;
BEGIN
  i := vimp_dates.FIRST;
  WHILE i IS NOT NULL LOOP
    vimp_dates(i) := vimp_dates(i) + 1;
	vimp_keys(i) := vimp_keys(i)||' '||i;
	vimp_ip(i) := -vimp_ip(i);
	vimp_zone(i) := -vimp_zone(i);
	vimp_plan(i) := -vimp_plan(i);
	vimp_banner(i) := -vimp_banner(i);
	vimp_referrer(i) := vimp_referrer(i)||' '||i;
	vimp_country(i) := vimp_country(i)||' '||i;
	vimp_region(i) := vimp_region(i)||' '||i;
    i := vimp_dates.NEXT(i);
  END LOOP;

END;

END tst_bench_inout;`,
	} {

		if _, err := testDb.Exec(qry); err != nil {
			if strings.HasPrefix(qry, "DROP TABLE ") {
				continue
			}
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}

	qry := `BEGIN tst_bench_inout.P_BULK_INSERT_IMP (:1, :2, :3, :4, :5, :6, :7, :8, :9); END;`

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
	ctx = godror.ContextWithLogger(ctx, nil)
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	params := []interface{}{
		godror.PlSQLArrays,
		sql.Out{Dest: &dates, In: true},
		sql.Out{Dest: &keys, In: true},
		sql.Out{Dest: &ips, In: true},
		sql.Out{Dest: &zones, In: true},
		sql.Out{Dest: &plans, In: true},
		sql.Out{Dest: &banners, In: true},
		sql.Out{Dest: &referrers, In: true},
		sql.Out{Dest: &countries, In: true},
		sql.Out{Dest: &regions, In: true},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += n {
		if _, err := tx.ExecContext(ctx, qry, params...); err != nil {
			if strings.Contains(err.Error(), "PLS-00905") || strings.Contains(err.Error(), "ORA-06508") {
				b.Log(godror.GetCompileErrors(ctx, testDb, false))
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
	b.Logf("total length: %d", length)
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

func createGeoTable(tableName string, rowCount int) error {
	var cnt int64
	if err := testDb.QueryRow(
		"SELECT COUNT(0) FROM " + tableName, //nolint:gas
	).Scan(&cnt); err == nil && cnt == int64(rowCount) {
		return nil
	}
	testDb.Exec("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
	testDb.Exec("DROP TABLE " + tableName)
	if _, err := testDb.Exec(`CREATE TABLE ` + tableName + ` (` + //nolint:gas
		` id NUMBER(9) NOT NULL,
	"RECORD_ID" NUMBER(*,0) NOT NULL ENABLE,
	"PERSON_ID" NUMBER(*,0),
	"PERSON_ACCOUNT_ID" NUMBER(*,0),
	"ORGANIZATION_ID" NUMBER(*,0),
	"ORGANIZATION_MEMBERSHIP_ID" NVARCHAR2(45),
	"LOCATION" NVARCHAR2(2000) NOT NULL ENABLE,
	"DEVICE_ID" NVARCHAR2(45),
	"DEVICE_REGISTRATION_ID" NVARCHAR2(500),
	"DEVICE_NAME" NVARCHAR2(45),
	"DEVICE_TYPE" NVARCHAR2(45),
	"DEVICE_OS_NAME" NVARCHAR2(45),
	"DEVICE_TOKEN" NVARCHAR2(45),
	"DEVICE_OTHER_DETAILS" NVARCHAR2(100)
	)`,
	); err != nil {
		return err
	}
	testData := [][]string{
		{"1", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5518407 104.0685472)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"2", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5520498 104.0686355)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"3", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5517747 104.0684895)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"4", "8.64522675633357E16", "8.64522734353613E16", "", "1220457", "POINT(30.55187 104.06856)", "3A9D1838-3B2D-4119-9E07-77C6CDAC53C5", "noUwBnWojdY:APA91bE8aGLEECS9_Q1EKrp8i2B36H1X8GwIj3v58KUcuXglhf0rXJb8Ez5meQ6D5MgTAQghYEe3s9vOntU3pYPQoc6ASNw3QzhzQevAqlMQC2ukUMNyLD8Rve-IA1-6lttsCXYsYIKh", "User3’s iPhone", "iPhone", "iPhone OS", "", "DeviceID:3A9D1838-3B2D-4119-9E07-77C6CDAC53C5, SystemVersion:8.4, LocalizedModel:iPhone"},
		{"5", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5517458 104.0685809)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"6", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.551802 104.0685301)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"7", "8.64522675633357E16", "8.64522734353613E16", "", "1220457", "POINT(30.55187 104.06856)", "3A9D1838-3B2D-4119-9E07-77C6CDAC53C5", "noUwBnWojdY:APA91bE8aGLEECS9_Q1EKrp8i2B36H1X8GwIj3v58KUcuXglhf0rXJb8Ez5meQ6D5MgTAQghYEe3s9vOnt,3pYPQoc6ASNw3QzhzQevAqlMQC2ukUMNyLD8Rve-IA1-6lttsCXYsYIKh", "User3’s iPhone", "iPhone", "iPhone OS", "", "DeviceID:3A9D1838-3B2D-4119-9E07-77C6CDAC53C5, SystemVersion:8.4, LocalizedModel:iPhone"},
		{"8", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.551952 104.0685893)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"9", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5518439 104.0685473)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
		{"10", "8.37064876162908E16", "8.37064898728264E16", "12", "6506", "POINT(30.5518439 104.0685473)", "a71223186cef459b", "", "Samsung SCH-I545", "Mobile", "Android 4.4.2", "", ""},
	}
	cols := make([]interface{}, len(testData[0])+1)
	for i := range cols {
		cols[i] = make([]string, rowCount)
	}
	for i := 0; i < rowCount; i++ {
		row := testData[i%len(testData)]
		for j, col := range cols {
			if j == 0 {
				(col.([]string))[i] = strconv.Itoa(i)
			} else {
				(col.([]string))[i] = row[j-1]
			}
		}
	}

	stmt, err := testDb.Prepare("INSERT INTO " + tableName + //nolint:gas
		` (ID,RECORD_ID,PERSON_ID,PERSON_ACCOUNT_ID,ORGANIZATION_ID,ORGANIZATION_MEMBERSHIP_ID,
   LOCATION,DEVICE_ID,DEVICE_REGISTRATION_ID,DEVICE_NAME,DEVICE_TYPE,
   DEVICE_OS_NAME,DEVICE_TOKEN,DEVICE_OTHER_DETAILS)
   VALUES (:1,:2,:3,:4,:5,
           :6,:7,:8,:9,:10,
		   :11,:12, :13, :14)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.Exec(cols...); err != nil {
		return fmt.Errorf("%v\n%q", err, cols)
	}
	return nil
}

func TestSelectOrder(t *testing.T) {
	t.Parallel()
	const limit = 1013
	var cnt int64
	tbl := "user_objects"
	start := time.Now()
	if err := testDb.QueryRow(
		"SELECT count(0) FROM " + tbl, //nolint:gas
	).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s rowcount=%d (%s)", tbl, cnt, time.Since(start))
	if cnt == 0 {
		cnt = 10
		tbl = "(SELECT 1 FROM DUAL " + strings.Repeat("\nUNION ALL SELECT 1 FROM DUAL ", int(cnt)-1) + ")" //nolint:gas
	}
	qry := "SELECT ROWNUM FROM " + tbl //nolint:gas
	for i := cnt; i < limit; i *= cnt {
		qry += ", " + tbl
	}
	t.Logf("qry=%s", qry)
	rows, err := testDb.Query(qry)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var rn int
		if err = rows.Scan(&rn); err != nil {
			t.Fatal(err)
		}
		i++
		if rn != i {
			t.Errorf("got %d, wanted %d.", rn, i)
		}
		if i > limit {
			break
		}
	}
	for rows.Next() {
	}
}

// go test -c && ./godror.v2.test -test.run=^$ -test.bench=Date -test.cpuprofile=/tmp/cpu.prof && go tool pprof godror.v2.test /tmp/cpu.prof
func BenchmarkSelectDate(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; {
		b.StopTimer()
		rows, err := testDb.Query(`SELECT CAST(TO_DATE('2006-01-02 15:04:05', 'YYYY-MM-DD HH24:MI:SS') AS DATE) dt
		FROM
		(select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual),
		(select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual union all select 1 from dual)
		`)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for rows.Next() && i < b.N {
			var dt time.Time
			if err = rows.Scan(&dt); err != nil {
				rows.Close()
				b.Fatal(err)
			}
			i++
		}
		b.StopTimer()
		rows.Close()
	}
}

func BenchmarkSelectGeo(b *testing.B) {
	b.StopTimer()
	geoTableName := "test_geo" + tblSuffix
	const geoTableRowCount = 100000
	if err := createGeoTable(geoTableName, geoTableRowCount); err != nil {
		b.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + geoTableName)

	b.StartTimer()
	for i := 0; i < b.N; {
		rows, err := testDb.Query(
			"SELECT location FROM "+geoTableName, //nolint:gas
			godror.FetchArraySize(1024), godror.PrefetchCount(1025))
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
		var readBytes, recNo int64
		for rows.Next() && i < b.N {
			var loc string
			if err = rows.Scan(&loc); err != nil {
				rows.Close()
				b.Fatal(err)
			}
			i++
			readBytes += int64(len(loc))
			recNo++
		}
		rows.Close()
		b.SetBytes(readBytes / recNo)
	}
}

func BenchmarkSprintf(b *testing.B) {
	ss := make([]string, 1024)
	for i := int32(0); i < int32(b.N); i++ {
		ss[i%1024] = fmt.Sprintf("%d-%d", i%42, 1+i%12)
	}
	b.Log(ss[0])
}
func BenchmarkStrconv(b *testing.B) {
	ss := make([]string, 1024)
	for i := int32(0); i < int32(b.N); i++ {
		ss[i%1024] = strconv.Itoa(int(i%42)) + "-" + strconv.Itoa(int(1+i%12))
	}
	b.Log(ss[0])
}

func BenchmarkPlSqlObj(b *testing.B) {
	ctx, cancel := context.WithTimeout(testContext("BenchPlSqlObj"), 3*time.Minute)
	defer cancel()
	if err := createPackages(ctx); err != nil {
		b.Fatal(err)
	}
	defer dropPackages(ctx)

	cx, err := testDb.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer cx.Close()
	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	// Warm up type cache
	for _, nm := range []string{"TEST_PKG_TYPES.OSH_TABLE", "TEST_PKG_TYPES.NUMBER_LIST"} {
		if _, err := conn.GetObjectType(nm); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Struct", func(b *testing.B) {
		in := oshNumberList{NumberList: []float64{1, 2, 3}}
		var out oshSliceStruct
		for i := 0; i < b.N; i++ {
			const qry = `begin test_pkg_sample.test_osh(:1, :2); end;`
			_, err := cx.ExecContext(ctx, qry, in, sql.Out{Dest: &out})
			b.Logf("struct: %+v", out)
			if err != nil {
				b.Fatalf("%s: %+v", qry, err)
			} else if len(out.List) == 0 {
				b.Fatal("no records found")
			} else if out.List[0].ID != 1 || len(out.List[0].Numbers.NumberList) == 0 {
				b.Fatalf("wrong data from the array: %#v", out.List)
			}
		}
	})
	b.Run("Map", func(b *testing.B) {
		in := oshNumberList{NumberList: []float64{1, 2, 3}}
		ot, err := conn.GetObjectType("test_pkg_types.osh_table")
		if err != nil {
			b.Fatal(err)
		}
		out, err := ot.NewObject()
		if err != nil {
			b.Fatal(err)
		}
		defer out.Close()
		for i := 0; i < b.N; i++ {
			const qry = `begin test_pkg_sample.test_osh(:1, :2); end;`
			_, err := cx.ExecContext(ctx, qry, in, sql.Out{Dest: out})
			if err != nil {
				b.Fatalf("%d. %s: %+v", i, qry, err)
			}
			m, err := out.Collection().AsSlice(nil)
			b.Logf("%d. map of %+v: %+v", i, out, m)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSelectWide
//
// go test -run=^$ -bench=Wide -cpuprofile c.pprof -memprofile m.pprof -benchtime 30s
// go tool pprof c.pprof
// go tool pprof m.pprof
func BenchmarkSelectWide(b *testing.B) {
	b.StopTimer()
	StopConnStats()
	tblName := func(suffix string) string { return "tst_wide_n" + strings.Replace(suffix, ",", "_", 1) }
	for _, typ := range []string{"9", "18", "19", "20,4", "20,4B"} {
		typ := typ
		b.Run(typ, func(b *testing.B) {
			b.StopTimer()
			tbl := tblName(typ)
			colTyp := strings.TrimRight(typ, "B")
			createWideTable := `CREATE TABLE ` + tbl + ` (
	"NE_KEY" NUMBER(19,0), 
	"STATISTIC_TIME" DATE, 
	"YEAR" NUMBER(4,0), 
	"MONTH" NUMBER(2,0), 
	"DAY" NUMBER(2,0), 
	"HOUR" NUMBER(2,0), 
	"INSERT_TIME" DATE, 
	"VENDOR_ID" NUMBER(4,0), 
	"COLL_SOURCE" NUMBER(10,0), 
	"COL_001" NUMBER(` + colTyp + `), 
	"COL_002" NUMBER(` + colTyp + `), 
	"COL_003" NUMBER(` + colTyp + `), 
	"COL_004" NUMBER(` + colTyp + `), 
	"COL_005" NUMBER(` + colTyp + `), 
	"COL_006" NUMBER(` + colTyp + `), 
	"COL_007" NUMBER(` + colTyp + `), 
	"COL_008" NUMBER(` + colTyp + `), 
	"COL_009" NUMBER(` + colTyp + `), 
	"COL_010" NUMBER(` + colTyp + `), 
	"COL_011" NUMBER(` + colTyp + `), 
	"COL_012" NUMBER(` + colTyp + `), 
	"COL_013" NUMBER(` + colTyp + `), 
	"COL_014" NUMBER(` + colTyp + `), 
	"COL_015" NUMBER(` + colTyp + `), 
	"COL_016" NUMBER(` + colTyp + `), 
	"COL_017" NUMBER(` + colTyp + `), 
	"COL_018" NUMBER(` + colTyp + `), 
	"COL_019" NUMBER(` + colTyp + `), 
	"COL_020" NUMBER(` + colTyp + `), 
	"COL_021" NUMBER(` + colTyp + `), 
	"COL_022" NUMBER(` + colTyp + `), 
	"COL_023" NUMBER(` + colTyp + `), 
	"COL_024" NUMBER(` + colTyp + `), 
	"COL_025" NUMBER(` + colTyp + `), 
	"COL_026" NUMBER(` + colTyp + `), 
	"COL_027" NUMBER(` + colTyp + `), 
	"COL_028" NUMBER(` + colTyp + `), 
	"COL_029" NUMBER(` + colTyp + `), 
	"COL_030" NUMBER(` + colTyp + `), 
	"COL_031" NUMBER(` + colTyp + `), 
	"COL_032" NUMBER(` + colTyp + `), 
	"COL_033" NUMBER(` + colTyp + `), 
	"COL_034" NUMBER(` + colTyp + `), 
	"COL_035" NUMBER(` + colTyp + `), 
	"COL_036" NUMBER(` + colTyp + `), 
	"COL_037" NUMBER(` + colTyp + `), 
	"COL_038" NUMBER(` + colTyp + `), 
	"COL_039" NUMBER(` + colTyp + `), 
	"COL_040" NUMBER(` + colTyp + `), 
	"COL_041" NUMBER(` + colTyp + `), 
	"COL_042" NUMBER(` + colTyp + `), 
	"COL_043" NUMBER(` + colTyp + `), 
	"COL_044" NUMBER(` + colTyp + `), 
	"COL_045" NUMBER(` + colTyp + `), 
	"COL_046" NUMBER(` + colTyp + `), 
	"COL_047" NUMBER(` + colTyp + `), 
	"COL_048" NUMBER(` + colTyp + `), 
	"COL_049" NUMBER(` + colTyp + `), 
	"COL_050" NUMBER(` + colTyp + `), 
	"COL_051" NUMBER(` + colTyp + `), 
	"COL_052" NUMBER(` + colTyp + `), 
	"COL_053" NUMBER(` + colTyp + `), 
	"COL_054" NUMBER(` + colTyp + `), 
	"COL_055" NUMBER(` + colTyp + `), 
	"COL_056" NUMBER(` + colTyp + `), 
	"COL_057" NUMBER(` + colTyp + `), 
	"COL_058" NUMBER(` + colTyp + `), 
	"COL_059" NUMBER(` + colTyp + `), 
	"COL_060" NUMBER(` + colTyp + `), 
	"COL_061" NUMBER(` + colTyp + `), 
	"COL_062" NUMBER(` + colTyp + `), 
	"COL_063" NUMBER(` + colTyp + `), 
	"COL_064" NUMBER(` + colTyp + `), 
	"COL_065" NUMBER(` + colTyp + `), 
	"COL_066" NUMBER(` + colTyp + `), 
	"COL_067" NUMBER(` + colTyp + `), 
	"COL_068" NUMBER(` + colTyp + `), 
	"COL_069" NUMBER(` + colTyp + `), 
	"COL_070" NUMBER(` + colTyp + `), 
	"COL_071" NUMBER(` + colTyp + `), 
	"COL_072" NUMBER(` + colTyp + `), 
	"COL_073" NUMBER(` + colTyp + `), 
	"COL_074" NUMBER(` + colTyp + `), 
	"COL_075" NUMBER(` + colTyp + `), 
	"COL_076" NUMBER(` + colTyp + `), 
	"COL_077" NUMBER(` + colTyp + `), 
	"COL_078" NUMBER(` + colTyp + `), 
	"COL_079" NUMBER(` + colTyp + `), 
	"COL_080" NUMBER(` + colTyp + `), 
	"COL_081" NUMBER(` + colTyp + `), 
	"COL_082" NUMBER(` + colTyp + `), 
	"COL_083" NUMBER(` + colTyp + `), 
	"COL_084" NUMBER(` + colTyp + `), 
	"COL_085" NUMBER(` + colTyp + `), 
	"COL_086" NUMBER(` + colTyp + `), 
	"COL_087" NUMBER(` + colTyp + `), 
	"COL_088" NUMBER(` + colTyp + `), 
	"COL_089" NUMBER(` + colTyp + `), 
	"COL_090" NUMBER(` + colTyp + `), 
	"COL_091" NUMBER(` + colTyp + `), 
	"COL_092" NUMBER(` + colTyp + `), 
	"COL_093" NUMBER(` + colTyp + `), 
	"COL_094" NUMBER(` + colTyp + `), 
	"COL_095" NUMBER(` + colTyp + `), 
	"COL_096" NUMBER(` + colTyp + `), 
	"COL_097" NUMBER(` + colTyp + `), 
	"COL_098" NUMBER(` + colTyp + `), 
	"COL_099" NUMBER(` + colTyp + `), 
	"COL_100" NUMBER(` + colTyp + `)
   )`
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			_, _ = testDb.ExecContext(ctx, "DROP TABLE "+tbl)
			if _, err := testDb.ExecContext(ctx, createWideTable); err != nil {
				b.Fatalf("%s: %+v", createWideTable, err)
			}
			defer func() { _, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl) }()

			insQry := `DECLARE
  v_want SIMPLE_INTEGER := :1;
  
  v_ne_key CONSTANT NUMBER(19) := DBMS_RANDOM.value(0, 9999999999999999999);
  v_now CONSTANT DATE := SYSDATE;
  v_year CONSTANT SIMPLE_INTEGER := TO_CHAR(v_now, 'YYYY');
  v_month CONSTANT SIMPLE_INTEGER := TO_CHAR(v_now, 'MM');
  v_day CONSTANT SIMPLE_INTEGER := TO_CHAR(v_now, 'DD');
  v_hour CONSTANT SIMPLE_INTEGER := TO_CHAR(v_now, 'HH24');
  v_vendor_id CONSTANT SIMPLE_INTEGER := DBMS_RANDOM.VALUE(0, 9999);
  v_coll_source CONSTANT NUMBER(10) := DBMS_RANDOM.VALUE(0, 9999999999);
BEGIN
  SELECT v_want - COUNT(0) INTO v_want FROM ` + tbl + `;
  IF v_want <= 0 THEN
    RETURN;
  END IF;
  FOR i IN -v_want..-1 LOOP
    INSERT INTO ` + tbl + ` (
	  COL_001, COL_002, COL_003, COL_004, COL_005, COL_006, COL_007, COL_008, COL_009, COL_010, 
	  COL_011, COL_012, COL_013, COL_014, COL_015, COL_016, COL_017, COL_018, COL_019, COL_020, 
	  COL_021, COL_022, COL_023, COL_024, COL_025, COL_026, COL_027, COL_028, COL_029, COL_030, 
	  COL_031, COL_032, COL_033, COL_034, COL_035, COL_036, COL_037, COL_038, COL_039, COL_040, 
	  COL_041, COL_042, COL_043, COL_044, COL_045, COL_046, COL_047, COL_048, COL_049, COL_050, 
	  COL_051, COL_052, COL_053, COL_054, COL_055, COL_056, COL_057, COL_058, COL_059, COL_060, 
	  COL_061, COL_062, COL_063, COL_064, COL_065, COL_066, COL_067, COL_068, COL_069, COL_070, 
	  COL_071, COL_072, COL_073, COL_074, COL_075, COL_076, COL_077, COL_078, COL_079, COL_080, 
	  COL_081, COL_082, COL_083, COL_084, COL_085, COL_086, COL_087, COL_088, COL_089, COL_090, 
	  COL_091, COL_092, COL_093, COL_094, COL_095, COL_096, COL_097, COL_098, COL_099, COL_100,
	  NE_KEY, STATISTIC_TIME, YEAR, MONTH, DAY, HOUR, INSERT_TIME, VENDOR_ID, COLL_SOURCE 
    ) VALUES (
	  i*100-1+1, i*100-1+2, i*100-1+3, i*100-1+4, i*100-1+5, i*100-1+6, i*100-1+7, i*100-1+8, i*100-1+9, i*100-1+10, 
	  i*100-1+11, i*100-1+12, i*100-1+13, i*100-1+14, i*100-1+15, i*100-1+16, i*100-1+17, i*100-1+18, i*100-1+19, i*100-1+20, 
	  i*100-1+21, i*100-1+22, i*100-1+23, i*100-1+24, i*100-1+25, i*100-1+26, i*100-1+27, i*100-1+28, i*100-1+29, i*100-1+30, 
	  i*100-1+31, i*100-1+32, i*100-1+33, i*100-1+34, i*100-1+35, i*100-1+36, i*100-1+37, i*100-1+38, i*100-1+39, i*100-1+40, 
	  i*100-1+41, i*100-1+42, i*100-1+43, i*100-1+44, i*100-1+45, i*100-1+46, i*100-1+47, i*100-1+48, i*100-1+49, i*100-1+50, 
	  i*100-1+51, i*100-1+52, i*100-1+53, i*100-1+54, i*100-1+55, i*100-1+56, i*100-1+57, i*100-1+58, i*100-1+59, i*100-1+60, 
	  i*100-1+61, i*100-1+62, i*100-1+63, i*100-1+64, i*100-1+65, i*100-1+66, i*100-1+67, i*100-1+68, i*100-1+69, i*100-1+70, 
	  i*100-1+71, i*100-1+72, i*100-1+73, i*100-1+74, i*100-1+75, i*100-1+76, i*100-1+77, i*100-1+78, i*100-1+79, i*100-1+80, 
	  i*100-1+81, i*100-1+82, i*100-1+83, i*100-1+84, i*100-1+85, i*100-1+86, i*100-1+87, i*100-1+88, i*100-1+89, i*100-1+90, 
	  i*100-1+91, i*100-1+92, i*100-1+93, i*100-1+94, i*100-1+95, i*100-1+96, i*100-1+97, i*100-1+98, i*100-1+99, i*100-1+100,
      v_ne_key, v_now, v_year, v_month, v_day, v_hour, SYSDATE, v_vendor_id, v_coll_source);
  END LOOP;
END;`
			if _, err := testDb.ExecContext(ctx, insQry, 1_000); err != nil {
				b.Fatalf("%s: %+v", insQry, err)
			}

			qry := "SELECT * FROM " + tbl
			if strings.HasSuffix(typ, "B") {
				qry = `SELECT 
	  NE_KEY, STATISTIC_TIME, YEAR, MONTH, DAY, HOUR, INSERT_TIME, VENDOR_ID, COLL_SOURCE,
	  CAST(COL_001 AS BINARY_DOUBLE), CAST(COL_002 AS BINARY_DOUBLE), CAST(COL_003 AS BINARY_DOUBLE), CAST(COL_004 AS BINARY_DOUBLE), CAST(COL_005 AS BINARY_DOUBLE), CAST(COL_006 AS BINARY_DOUBLE), CAST(COL_007 AS BINARY_DOUBLE), CAST(COL_008 AS BINARY_DOUBLE), CAST(COL_009 AS BINARY_DOUBLE), CAST(COL_010 AS BINARY_DOUBLE), 
	  CAST(COL_011 AS BINARY_DOUBLE), CAST(COL_012 AS BINARY_DOUBLE), CAST(COL_013 AS BINARY_DOUBLE), CAST(COL_014 AS BINARY_DOUBLE), CAST(COL_015 AS BINARY_DOUBLE), CAST(COL_016 AS BINARY_DOUBLE), CAST(COL_017 AS BINARY_DOUBLE), CAST(COL_018 AS BINARY_DOUBLE), CAST(COL_019 AS BINARY_DOUBLE), CAST(COL_020 AS BINARY_DOUBLE), 
	  CAST(COL_021 AS BINARY_DOUBLE), CAST(COL_022 AS BINARY_DOUBLE), CAST(COL_023 AS BINARY_DOUBLE), CAST(COL_024 AS BINARY_DOUBLE), CAST(COL_025 AS BINARY_DOUBLE), CAST(COL_026 AS BINARY_DOUBLE), CAST(COL_027 AS BINARY_DOUBLE), CAST(COL_028 AS BINARY_DOUBLE), CAST(COL_029 AS BINARY_DOUBLE), CAST(COL_030 AS BINARY_DOUBLE), 
	  CAST(COL_031 AS BINARY_DOUBLE), CAST(COL_032 AS BINARY_DOUBLE), CAST(COL_033 AS BINARY_DOUBLE), CAST(COL_034 AS BINARY_DOUBLE), CAST(COL_035 AS BINARY_DOUBLE), CAST(COL_036 AS BINARY_DOUBLE), CAST(COL_037 AS BINARY_DOUBLE), CAST(COL_038 AS BINARY_DOUBLE), CAST(COL_039 AS BINARY_DOUBLE), CAST(COL_040 AS BINARY_DOUBLE), 
	  CAST(COL_041 AS BINARY_DOUBLE), CAST(COL_042 AS BINARY_DOUBLE), CAST(COL_043 AS BINARY_DOUBLE), CAST(COL_044 AS BINARY_DOUBLE), CAST(COL_045 AS BINARY_DOUBLE), CAST(COL_046 AS BINARY_DOUBLE), CAST(COL_047 AS BINARY_DOUBLE), CAST(COL_048 AS BINARY_DOUBLE), CAST(COL_049 AS BINARY_DOUBLE), CAST(COL_050 AS BINARY_DOUBLE), 
	  CAST(COL_051 AS BINARY_DOUBLE), CAST(COL_052 AS BINARY_DOUBLE), CAST(COL_053 AS BINARY_DOUBLE), CAST(COL_054 AS BINARY_DOUBLE), CAST(COL_055 AS BINARY_DOUBLE), CAST(COL_056 AS BINARY_DOUBLE), CAST(COL_057 AS BINARY_DOUBLE), CAST(COL_058 AS BINARY_DOUBLE), CAST(COL_059 AS BINARY_DOUBLE), CAST(COL_060 AS BINARY_DOUBLE), 
	  CAST(COL_061 AS BINARY_DOUBLE), CAST(COL_062 AS BINARY_DOUBLE), CAST(COL_063 AS BINARY_DOUBLE), CAST(COL_064 AS BINARY_DOUBLE), CAST(COL_065 AS BINARY_DOUBLE), CAST(COL_066 AS BINARY_DOUBLE), CAST(COL_067 AS BINARY_DOUBLE), CAST(COL_068 AS BINARY_DOUBLE), CAST(COL_069 AS BINARY_DOUBLE), CAST(COL_070 AS BINARY_DOUBLE), 
	  CAST(COL_071 AS BINARY_DOUBLE), CAST(COL_072 AS BINARY_DOUBLE), CAST(COL_073 AS BINARY_DOUBLE), CAST(COL_074 AS BINARY_DOUBLE), CAST(COL_075 AS BINARY_DOUBLE), CAST(COL_076 AS BINARY_DOUBLE), CAST(COL_077 AS BINARY_DOUBLE), CAST(COL_078 AS BINARY_DOUBLE), CAST(COL_079 AS BINARY_DOUBLE), CAST(COL_080 AS BINARY_DOUBLE), 
	  CAST(COL_081 AS BINARY_DOUBLE), CAST(COL_082 AS BINARY_DOUBLE), CAST(COL_083 AS BINARY_DOUBLE), CAST(COL_084 AS BINARY_DOUBLE), CAST(COL_085 AS BINARY_DOUBLE), CAST(COL_086 AS BINARY_DOUBLE), CAST(COL_087 AS BINARY_DOUBLE), CAST(COL_088 AS BINARY_DOUBLE), CAST(COL_089 AS BINARY_DOUBLE), CAST(COL_090 AS BINARY_DOUBLE), 
	  CAST(COL_091 AS BINARY_DOUBLE), CAST(COL_092 AS BINARY_DOUBLE), CAST(COL_093 AS BINARY_DOUBLE), CAST(COL_094 AS BINARY_DOUBLE), CAST(COL_095 AS BINARY_DOUBLE), CAST(COL_096 AS BINARY_DOUBLE), CAST(COL_097 AS BINARY_DOUBLE), CAST(COL_098 AS BINARY_DOUBLE), CAST(COL_099 AS BINARY_DOUBLE), CAST(COL_100 AS BINARY_DOUBLE)
				FROM ` + tbl
			}

			first := true
			var rowNum int64
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				qry := qry + " FETCH FIRST " + strconv.Itoa(b.N) + " ROWS ONLY"
				rows, err := testDb.QueryContext(ctx, qry, godror.PrefetchCount(17), godror.FetchArraySize(16))
				if err != nil {
					b.Fatalf("%s: %+v", qry, err)
				}
				defer rows.Close()
				var (
					statisticTime, insertTime                                             time.Time
					neKey, year, month, day, hour, vendorId, collSource                   string
					col1, col2, col3, col4, col5, col6, col7, col8, col9, col10           int64
					col11, col12, col13, col14, col15, col16, col17, col18, col19, col20  int64
					col21, col22, col23, col24, col25, col26, col27, col28, col29, col30  int64
					col31, col32, col33, col34, col35, col36, col37, col38, col39, col40  int64
					col41, col42, col43, col44, col45, col46, col47, col48, col49, col50  int64
					col51, col52, col53, col54, col55, col56, col57, col58, col59, col60  int64
					col61, col62, col63, col64, col65, col66, col67, col68, col69, col70  int64
					col71, col72, col73, col74, col75, col76, col77, col78, col79, col80  int64
					col81, col82, col83, col84, col85, col86, col87, col88, col89, col90  int64
					col91, col92, col93, col94, col95, col96, col97, col98, col99, col100 int64
				)

				for rows.Next() {
					if err := rows.Scan(&neKey, &statisticTime, &year, &month, &day, &hour,
						&insertTime, &vendorId, &collSource,
						&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10,
						&col11, &col12, &col13, &col14, &col15, &col16, &col17, &col18, &col19, &col20,
						&col21, &col22, &col23, &col24, &col25, &col26, &col27, &col28, &col29, &col30,
						&col31, &col32, &col33, &col34, &col35, &col36, &col37, &col38, &col39, &col40,
						&col41, &col42, &col43, &col44, &col45, &col46, &col47, &col48, &col49, &col50,
						&col51, &col52, &col53, &col54, &col55, &col56, &col57, &col58, &col59, &col60,
						&col61, &col62, &col63, &col64, &col65, &col66, &col67, &col68, &col69, &col70,
						&col71, &col72, &col73, &col74, &col75, &col76, &col77, &col78, &col79, &col80,
						&col81, &col82, &col83, &col84, &col85, &col86, &col87, &col88, &col89, &col90,
						&col91, &col92, &col93, &col94, &col95, &col96, &col97, &col98, &col99, &col100,
					); err != nil {
						b.Fatalf("%s: %+v", qry, err)
					}
					if first {
						first = false
						if col1 >= 0 {
							b.Logf("col1=%d col2=%d col100=%d", col1, col2, col100)
							b.Fatalf("%d got %d wanted <0", rowNum, col1)
						}
					}
					rowNum++
					b.SetBytes(int64(
						2*16 +
							len(neKey) + len(year) + len(month) + len(day) + len(hour) + len(vendorId) + len(collSource) +
							100*8,
					))
				}
			}
		})
	}
}

var benchmarkSelect301LogFh *os.File

func BenchmarkSelect301(b *testing.B) {
	StopConnStats()
	P, err := godror.ParseConnString(testConStr)
	if err != nil {
		b.Fatal(err)
	}
	P.PoolParams.MaxSessions = 1

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if benchmarkSelect301LogFh == nil {
		if benchmarkSelect301LogFh, err = os.CreateTemp("", "BenchmarkSelect301-*.txt"); err != nil {
			b.Fatal(err)
		}
		b.Log("alloc log: " + benchmarkSelect301LogFh.Name())
	}
	defer benchmarkSelect301LogFh.Sync()
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var m runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				fmt.Fprintf(benchmarkSelect301LogFh, "%d\n", m.Alloc)
			case <-ctx.Done():
				ticker.Stop()
				select {
				case <-ticker.C:
				default:
				}
				return
			}
		}
	}()
	var nm, typ, ts string
	var oid uint64
	for i := 0; i < b.N; {
		func() {
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			db.SetMaxIdleConns(0)
			db.SetMaxOpenConns(1)
			const qry = "SELECT A.object_name, A.object_id, A.object_type, SYSTIMESTAMP FROM all_objects A FETCH FIRST 10 ROWS ONLY"
			rows, err := db.QueryContext(ctx, qry, godror.FetchArraySize(1024), godror.PrefetchCount(1025))
			if err != nil {
				b.Fatalf("%s: %+v", qry, err)
			}
			defer rows.Close()
			for rows.Next() && i < b.N {
				if err = rows.Scan(&nm, &oid, &typ, &ts); err != nil {
					b.Fatalf("scan %s: %+v", qry, err)
				}
				i++
			}
		}()
	}
	b.Log(nm, oid, typ, ts)
}
