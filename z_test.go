// Copyright 2020, 2025 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/user"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/godror/godror/slog"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/sync/errgroup"

	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

var (
	testDb       *sql.DB
	testSystemDb *sql.DB
	tl           = &testLogger{}
	logger       *slog.Logger

	clientVersion, serverVersion godror.VersionInfo
	testConStr                   string
	testSystemConStr             string

	tblSuffix   string
	maxSessions = 16

	Verbose bool
)

const (
	useDefaultFetchValue = -99

	DefaultDSN         = "oracle://demo:demo@localhost:1521/freepdb1"
	DefaultSystemDSN   = "oracle://sys:system@localhost:1521/freepdb1?sysdba=1"
	DefaultMaxSessions = 2
)

// TestMain is called instead of the separate Test functions,
// to allow setup and teardown.
func TestMain(m *testing.M) {
	tearDown := setUp()
	rc := m.Run()
	tearDown()
	os.Exit(rc)
}

func setUp() func() {
	Verbose := slog.LevelError
	if s := os.Getenv("VERBOSE"); s != "" {
		if i, err := strconv.ParseInt(s, 10, 32); err == nil {
			if i > 1 {
				Verbose = slog.LevelDebug
			} else if i > 0 {
				Verbose = slog.LevelError
			}
		} else if b, _ := strconv.ParseBool(s); b {
			Verbose = slog.LevelInfo
		}
	}
	hsh := sha256.New()
	bi, ok := debug.ReadBuildInfo()
	if ok && bi != nil {
		ok = json.NewEncoder(hsh).Encode(*bi) == nil
	}
	if !ok {
		hsh.Write([]byte(runtime.Version()))
	}
	tblSuffix = fmt.Sprintf("_%x", hsh.Sum(nil)[:4])

	tl.enc = logfmt.NewEncoder(os.Stderr)
	logger = slog.New(slog.NewTextHandler(tl, &slog.HandlerOptions{Level: Verbose}))
	slog.SetDefault(logger)
	godror.SetLogger(logger)
	if tzName := os.Getenv("GODROR_TIMEZONE"); tzName != "" {
		var err error
		if time.Local, err = time.LoadLocation(tzName); err != nil {
			panic(fmt.Errorf("unknown GODROR_TIMEZONE=%q: %w", tzName, err))
		}
	}

	var tearDown []func()
	eDSN := os.Getenv("GODROR_TEST_DSN")
	eSysDSN := os.Getenv("GODROR_TEST_SYSTEM_DSN")
	{
		uid, _ := user.Current()
		fmt.Printf("eDSN=%s\nOSuser=%v\n", eDSN, uid)
	}

	if eDSN == "" {
		eDSN, eSysDSN = DefaultDSN, DefaultSystemDSN
		maxSessions = DefaultMaxSessions
		if i, _ := strconv.ParseInt(os.Getenv("GOMAXPROCS"), 10, 32); 0 < i && i <= int64(maxSessions) {
			fmt.Printf("GOMAXPROCS=%d\n", i)
		} else if e, _ := os.Executable(); e == "" {
			fmt.Println("executable=" + e)
		} else if false {
			fmt.Printf("Reexec with GOMAXPROCS=%d\n", maxSessions)
			syscall.Exec(e, os.Args[1:], append(os.Environ(), "GOMAXPROCS="+strconv.Itoa(maxSessions)))
			return nil
		}
	}

	P, err := dsn.Parse(eDSN)
	if err != nil {
		panic(fmt.Errorf("parse %q: %w", eDSN, err))
	}
	// fmt.Println("parsed:", P)
	P.CommonParams.Logger = logger
	P.CommonParams.EnableEvents = true
	if P.ConnParams.ConnClass == "" {
		P.ConnParams.ConnClass = "TestClassName"
	}
	if P.StandaloneConnection.Valid && P.StandaloneConnection.Bool && // Sharding does not work with pooled connection
		len(P.ConnParams.ShardingKey) == 0 {
		P.ConnParams.ShardingKey = []any{"gold", []byte("silver"), int(42)}
	}
	if P.PoolParams.MaxSessions < 1 || P.PoolParams.MaxSessions > maxSessions {
		P.PoolParams.MaxSessions = maxSessions
	}
	if P.PoolParams.MinSessions == 0 {
		P.PoolParams.MinSessions = P.PoolParams.MaxSessions
	}
	if P.PoolParams.SessionIncrement == 0 {
		P.PoolParams.SessionIncrement = 1
	}
	if P.PoolParams.WaitTimeout == 0 {
		P.PoolParams.WaitTimeout = 5 * time.Second
	}
	if P.PoolParams.MaxLifeTime == 0 {
		P.PoolParams.MaxLifeTime = 5 * time.Minute
	}
	if P.PoolParams.SessionTimeout == 0 {
		P.PoolParams.SessionTimeout = 1 * time.Minute
	}
	if strings.HasSuffix(strings.ToUpper(P.Username), " AS SYSDBA") {
		P.AdminRole, P.Username = godror.SysDBA, P.Username[:len(P.Username)-10]
	}
	testConStr = P.StringWithPassword()
	testDb = sql.OpenDB(godror.NewConnector(P))
	tearDown = append(tearDown, func() { testDb.Close() })
	ctx, cancel := context.WithTimeout(testContext("init"), 30*time.Second)
	defer cancel()

	if b, err := strconv.ParseBool(os.Getenv("DO_NOT_CONNECT")); b && err == nil {
		return func() {}
	}

	statTicks = make(chan time.Time)
	statTicker = time.NewTicker(60 * time.Second)
	go func() {
		time.Sleep(time.Second)
		statTicks <- time.Now()
		for t := range statTicker.C {
			statTicks <- t
		}
	}()

	fmt.Printf("export GODROR_TEST_DSN=%q\n", P.String())
	testDb.SetMaxOpenConns(P.PoolParams.MaxSessions)
	if P.StandaloneConnection.Valid && P.StandaloneConnection.Bool {
		testDb.SetMaxIdleConns(P.PoolParams.MaxSessions / 2)
		testDb.SetConnMaxLifetime(10 * time.Minute)
		go func() {
			for range statTicks {
				fmt.Fprintf(os.Stderr, "testDb: %+v\n", testDb.Stats())
			}
		}()
	} else {
		// Disable Go db connection pooling
		testDb.SetMaxIdleConns(0)
		testDb.SetConnMaxLifetime(0)
		go func() {
			for range statTicks {
				ctx, cancel := context.WithTimeout(testContext("poolStats"), time.Second)
				godror.Raw(ctx, testDb, func(c godror.Conn) error {
					poolStats, err := c.GetPoolStats()
					fmt.Fprintf(os.Stderr, "testDb: %+v: %s %v\n", testDb.Stats(), poolStats, err)
					return err
				})
				cancel()
			}
		}()
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
	err = testDb.PingContext(shortCtx)
	shortCancel()
	if err != nil {
		panic(err)
	}
	fmt.Println("#", P.String())
	fmt.Println("Version:", godror.Version)
	if err = godror.Raw(ctx, testDb, func(cx godror.Conn) error {
		if clientVersion, err = cx.ClientVersion(); err != nil {
			return err
		}
		fmt.Println("Client:", clientVersion.String(), "Timezone:", time.Local.String())
		if serverVersion, err = cx.ServerVersion(); err != nil {
			return err
		}
		dbTZ := cx.Timezone()
		fmt.Println("Server:", serverVersion.String(), "Timezone:", dbTZ.String())
		return nil
	}); err != nil {
		panic(fmt.Errorf("get version with %#v: %w", P, err))
	}

	if eSysDSN != "" {
		PSystem := P
		if ps, err := dsn.Parse(eSysDSN); err != nil {
			panic(fmt.Errorf("sysdsn: %q: %w", eSysDSN, err))
		} else {
			PSystem = ps
			PSystem.CommonParams.EnableEvents = true
			PSystem.ConnParams.ConnClass = P.ConnParams.ConnClass
			PSystem.ConnParams.ShardingKey = P.ConnParams.ShardingKey
			PSystem.PoolParams = P.PoolParams
		}
		testSystemConStr = PSystem.StringWithPassword()
		if eSysDSN == DefaultSystemDSN {
			db := sql.OpenDB(godror.NewConnector(PSystem))
			if err := func() error {
				defer db.Close()
				for _, qry := range []string{
					"GRANT EXECUTE ON SYS.DBMS_AQADM TO demo",
				} {
					if _, err := db.ExecContext(ctx, qry); err != nil {
						return fmt.Errorf("%s: %w", qry, err)
					}
				}
				return nil
			}(); err != nil {
				fmt.Printf("WARN: set up system db: %+v\n", err)
			}
		}
	}

	go func() {
		for range time.NewTicker(time.Second).C {
			runtime.GC()
		}
	}()

	return func() {
		for i := len(tearDown) - 1; i >= 0; i-- {
			tearDown[i]()
		}
	}
}

var statTicker *time.Ticker
var statTicks chan time.Time

func PrintConnStats() {
	if statTicks != nil {
		select {
		case statTicks <- time.Now():
		default:
		}
	}
}
func StopConnStats() {
	if statTicker != nil {
		statTicker.Stop()
	}
}

func testContext(name string) context.Context {
	ctx := godror.ContextWithTraceTag(context.Background(), godror.TraceTag{Module: "Test" + name})
	if Verbose {
		logger.Info("testContext", "name", name)
		ctx = godror.ContextWithLogger(ctx, logger)
	}
	return ctx
}

var bufPool = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, 1024)) }}

type testLogger struct {
	enc      *logfmt.Encoder
	Ts       []*testing.T
	beHelped []*testing.T
	mu       sync.RWMutex
}

func (tl *testLogger) Write(p []byte) (int, error) {
	s := string(p)
	if len(tl.Ts) == 0 {
		fmt.Println(string(p))
		return len(p), nil
	}
	for _, t := range tl.Ts {
		t.Helper()
		t.Log(s)
	}
	return len(p), nil
}
func (tl *testLogger) Enabled(context.Context, slog.Level) bool { return true }

func (tl *testLogger) Handle(ctx context.Context, r slog.Record) error {
	for _, t := range tl.Ts {
		t.Helper()
		t.Log(r.Message)
	}
	return nil
}

func (tl *testLogger) WithAttrs(attrs []slog.Attr) slog.Handler { return tl }
func (tl *testLogger) WithGroup(name string) slog.Handler       { return tl }

func (tl *testLogger) Log(args ...any) error {
	if tl.enc != nil {
		for i := 1; i < len(args); i += 2 {
			switch args[i].(type) {
			case string, fmt.Stringer:
			default:
				args[i] = fmt.Sprintf("%+v", args[i])
			}
		}
		tl.mu.Lock()
		tl.enc.Reset()
		tl.enc.EncodeKeyvals(args...)
		tl.enc.EndRecord()
		tl.mu.Unlock()
	}
	return tl.GetLog()(args)
}
func (tl *testLogger) GetLog() func(keyvals ...any) error {
	return func(keyvals ...any) error {
		buf := bufPool.Get().(*bytes.Buffer)
		defer bufPool.Put(buf)
		buf.Reset()
		if len(keyvals)%2 != 0 {
			keyvals = append(append(make([]any, 0, len(keyvals)+1), "msg"), keyvals...)
		}
		for i := 0; i < len(keyvals); i += 2 {
			fmt.Fprintf(buf, "%s=%#v ", keyvals[i], keyvals[i+1])
		}

		tl.mu.Lock()
		for _, t := range tl.beHelped {
			t.Helper()
		}
		tl.beHelped = tl.beHelped[:0]
		tl.mu.Unlock()

		tl.mu.RLock()
		defer tl.mu.RUnlock()
		for _, t := range tl.Ts {
			t.Helper()
			t.Log(buf.String())
		}

		return nil
	}
}
func (tl *testLogger) enableLogging(t *testing.T) func() {
	tl.mu.Lock()
	tl.Ts = append(tl.Ts, t)
	tl.beHelped = append(tl.beHelped, t)
	tl.mu.Unlock()

	return func() {
		tl.mu.Lock()
		defer tl.mu.Unlock()
		for i, f := range tl.Ts {
			if f == t {
				tl.Ts[i] = tl.Ts[0]
				tl.Ts = tl.Ts[1:]
				break
			}
		}
		for i, f := range tl.beHelped {
			if f == t {
				tl.beHelped[i] = tl.beHelped[0]
				tl.beHelped = tl.beHelped[1:]
				break
			}
		}
	}
}

func TestDescribeQuery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("DescribeQuery"), 10*time.Second)
	defer cancel()

	const qry = "SELECT * FROM user_tab_cols"
	cols, err := godror.DescribeQuery(ctx, testDb, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Log(cols)
}

func TestParseOnly(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ParseOnly"), 10*time.Second)
	defer cancel()

	tbl := "test_not_exist" + tblSuffix
	cnt := func() int {
		var cnt int64
		if err := testDb.QueryRowContext(ctx,
			"SELECT COUNT(0) FROM user_tables WHERE table_name = UPPER('"+tbl+"')").Scan(&cnt); //nolint:gas
		err != nil {
			t.Fatal(err)
		}
		return int(cnt)
	}

	if cnt() != 0 {
		if _, err := testDb.ExecContext(ctx, "DROP TABLE "+tbl); err != nil {
			t.Error(err)
		}
	}
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+"(t VARCHAR2(1))", godror.ParseOnly()); err != nil {
		t.Fatal(err)
	}
	if got := cnt(); got != 1 {
		t.Errorf("got %d, wanted 0", got)
	}
}

func TestInputArray(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("InputArray"), 10*time.Second)
	defer cancel()

	pkg := strings.ToUpper("test_in_pkg" + tblSuffix)
	qry := `CREATE OR REPLACE PACKAGE ` + pkg + ` AS
TYPE int_tab_typ IS TABLE OF BINARY_INTEGER INDEX BY PLS_INTEGER;
TYPE num_tab_typ IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
TYPE vc_tab_typ IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
TYPE dt_tab_typ IS TABLE OF DATE INDEX BY PLS_INTEGER;
TYPE ids_tab_typ IS TABLE OF INTERVAL DAY TO SECOND INDEX BY PLS_INTEGER;
--TYPE lob_tab_typ IS TABLE OF CLOB INDEX BY PLS_INTEGER;

FUNCTION in_int(p_int IN int_tab_typ) RETURN VARCHAR2;
FUNCTION in_num(p_num IN num_tab_typ) RETURN VARCHAR2;
FUNCTION in_vc(p_vc IN vc_tab_typ) RETURN VARCHAR2;
FUNCTION in_dt(p_dt IN dt_tab_typ) RETURN VARCHAR2;
FUNCTION in_ids(p_dur IN ids_tab_typ) RETURN VARCHAR2;
END;
`
	testDb.Exec("DROP PACKAGE " + pkg)
	t.Log("package", pkg)
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(err, qry)
	}
	defer testDb.Exec("DROP PACKAGE " + pkg)

	qry = `CREATE OR REPLACE PACKAGE BODY ` + pkg + ` AS
FUNCTION in_int(p_int IN int_tab_typ) RETURN VARCHAR2 IS
  v_idx PLS_INTEGER;
  v_res VARCHAR2(32767);
BEGIN
  v_idx := p_int.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_res := v_res||v_idx||':'||p_int(v_idx)||CHR(10);
    v_idx := p_int.NEXT(v_idx);
  END LOOP;
  RETURN(v_res);
END;

FUNCTION in_num(p_num IN num_tab_typ) RETURN VARCHAR2 IS
  v_idx PLS_INTEGER;
  v_res VARCHAR2(32767);
BEGIN
  v_idx := p_num.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_res := v_res||v_idx||':'||p_num(v_idx)||CHR(10);
    v_idx := p_num.NEXT(v_idx);
  END LOOP;
  RETURN(v_res);
END;

FUNCTION in_vc(p_vc IN vc_tab_typ) RETURN VARCHAR2 IS
  v_idx PLS_INTEGER;
  v_res VARCHAR2(32767);
BEGIN
  v_idx := p_vc.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_res := v_res||v_idx||':'||p_vc(v_idx)||CHR(10);
    v_idx := p_vc.NEXT(v_idx);
  END LOOP;
  RETURN(v_res);
END;
FUNCTION in_dt(p_dt IN dt_tab_typ) RETURN VARCHAR2 IS
  v_idx PLS_INTEGER;
  v_res VARCHAR2(32767);
BEGIN
  v_idx := p_dt.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_res := v_res||v_idx||':'||TO_CHAR(p_dt(v_idx), 'YYYY-MM-DD"T"HH24:MI:SS')||CHR(10);
    v_idx := p_dt.NEXT(v_idx);
  END LOOP;
  RETURN(v_res);
END;
FUNCTION in_ids(p_dur IN ids_tab_typ) RETURN VARCHAR2 IS
  V_idx PLS_INTEGER;
  v_res VARCHAR2(32767);
BEGIN
  v_idx := p_dur.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_res := v_res||v_idx||':'||TO_CHAR(p_dur(v_idx), 'YYYY-MM-DD"T"HH24:MI:SS')||CHR(10);
    v_idx := p_dur.NEXT(v_idx);
  END LOOP;
  RETURN(v_res);
END;
END;
`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(err, qry)
	}
	compileErrors, err := godror.GetCompileErrors(ctx, testDb, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(compileErrors) != 0 {
		t.Logf("compile errors: %v", compileErrors)
		for _, ce := range compileErrors {
			if strings.Contains(ce.Error(), pkg) {
				t.Fatal(ce)
			}
		}
	}
	serverTZ := time.Local
	if err = godror.Raw(ctx, testDb, func(conn godror.Conn) error {
		serverTZ = conn.Timezone()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	qry = "ALTER SESSION SET time_zone = 'UTC'"
	if _, err = tx.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	epoch := time.Date(2017, 11, 20, 12, 14, 21, 0, time.UTC)
	epochPlus := epoch.AddDate(0, -6, 0)
	const timeFmt = "2006-01-02T15:04:05"
	_ = epoch
	for name, tC := range map[string]struct {
		In   any
		Want string
	}{
		// "int_0":{In:[]int32{}, Want:""},
		"num_0": {In: []godror.Number{}, Want: ""},
		"vc_0":  {In: []string{}, Want: ""},
		"dt_0":  {In: []time.Time{}, Want: ""},
		"dt_00": {In: []godror.NullTime{}, Want: ""},

		"num_3": {
			In:   []godror.Number{"1", "2.72", "-3.14"},
			Want: "1:1\n2:2.72\n3:-3.14\n",
		},
		"vc_3": {
			In:   []string{"a", "", "cCc"},
			Want: "1:a\n2:\n3:cCc\n",
		},
		"dt_2": {
			In: []time.Time{epoch, epochPlus},
			Want: ("1:" + epoch.In(serverTZ).Format(timeFmt) + "\n" +
				"2:" + epochPlus.In(serverTZ).Format(timeFmt) + "\n"),
		},
		"dt_02": {
			In: []godror.NullTime{{Valid: true, Time: epoch},
				{Valid: true, Time: epochPlus}},
			Want: ("1:" + epoch.In(serverTZ).Format(timeFmt) + "\n" +
				"2:" + epochPlus.In(serverTZ).Format(timeFmt) + "\n"),
		},

		// "ids_1": { In:   []time.Duration{32 * time.Second}, Want: "1:32s\n", },
	} {
		typ := strings.SplitN(name, "_", 2)[0]
		qry := "BEGIN :1 := " + pkg + ".in_" + typ + "(:2); END;"
		var res string
		if _, err := tx.ExecContext(ctx, qry, godror.PlSQLArrays,
			sql.Out{Dest: &res}, tC.In,
		); err != nil {
			t.Error(fmt.Errorf("%q. %s %+v: %w", name, qry, tC.In, err))
		}
		t.Logf("%q. %q", name, res)
		if typ == "num" {
			res = strings.Replace(res, ",", ".", -1)
		}
		if res != tC.Want {
			t.Errorf("%q. got %q, wanted %q.", name, res, tC.Want)
		}
	}
}

func TestDbmsOutput(t *testing.T) {
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("DbmsOutput"), 10*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := godror.EnableDbmsOutput(ctx, conn); err != nil {
		t.Fatal(err)
	}

	txt := `árvíztűrő tükörfúrógép`
	qry := "BEGIN DBMS_OUTPUT.PUT_LINE('" + txt + "'); END;"
	if _, err := conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := godror.ReadDbmsOutput(ctx, &buf, conn); err != nil {
		t.Error(err)
	}
	t.Log(buf.String())
	if buf.String() != txt+"\n" {
		t.Errorf("got\n%q, wanted\n%q", buf.String(), txt+"\n")
	}
}

func TestInOutArray(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()

	ctx, cancel := context.WithTimeout(testContext("InOutArray"), 20*time.Second)
	defer cancel()

	pkg := strings.ToUpper("test_pkg" + tblSuffix)
	qry := `CREATE OR REPLACE PACKAGE ` + pkg + ` AS
TYPE int_tab_typ IS TABLE OF BINARY_INTEGER INDEX BY PLS_INTEGER;
TYPE num_tab_typ IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
TYPE vc_tab_typ IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
TYPE dt_tab_typ IS TABLE OF DATE INDEX BY PLS_INTEGER;
TYPE lob_tab_typ IS TABLE OF CLOB INDEX BY PLS_INTEGER;

PROCEDURE inout_int(p_int IN OUT int_tab_typ);
PROCEDURE inout_num(p_num IN OUT num_tab_typ);
PROCEDURE inout_vc(p_vc IN OUT vc_tab_typ);
PROCEDURE inout_dt(p_dt IN OUT dt_tab_typ);
PROCEDURE p2(
	--p_int IN OUT int_tab_typ,
	p_num IN OUT num_tab_typ, p_vc IN OUT vc_tab_typ, p_dt IN OUT dt_tab_typ);
END;
`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(err, qry)
	}
	defer testDb.Exec("DROP PACKAGE " + pkg)

	qry = `CREATE OR REPLACE PACKAGE BODY ` + pkg + ` AS
PROCEDURE inout_int(p_int IN OUT int_tab_typ) IS
  v_idx PLS_INTEGER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('p_int.COUNT='||p_int.COUNT||' FIRST='||p_int.FIRST||' LAST='||p_int.LAST);
  v_idx := p_int.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    p_int(v_idx) := NVL(p_int(v_idx) * 2, 1);
	v_idx := p_int.NEXT(v_idx);
  END LOOP;
  p_int(NVL(p_int.LAST, 0)+1) := p_int.COUNT;
END;

PROCEDURE inout_num(p_num IN OUT num_tab_typ) IS
  v_idx PLS_INTEGER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('p_num.COUNT='||p_num.COUNT||' FIRST='||p_num.FIRST||' LAST='||p_num.LAST);
  v_idx := p_num.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    p_num(v_idx) := NVL(p_num(v_idx) / 2, 0.5);
	v_idx := p_num.NEXT(v_idx);
  END LOOP;
  p_num(NVL(p_num.LAST, 0)+1) := p_num.COUNT;
END;

PROCEDURE inout_vc(p_vc IN OUT vc_tab_typ) IS
  v_idx PLS_INTEGER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('p_vc.COUNT='||p_vc.COUNT||' FIRST='||p_vc.FIRST||' LAST='||p_vc.LAST);
  v_idx := p_vc.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    p_vc(v_idx) := NVL(p_vc(v_idx) ||' +', '-');
	v_idx := p_vc.NEXT(v_idx);
  END LOOP;
  p_vc(NVL(p_vc.LAST, 0)+1) := p_vc.COUNT;
END;

PROCEDURE inout_dt(p_dt IN OUT dt_tab_typ) IS
  v_idx PLS_INTEGER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('p_dt.COUNT='||p_dt.COUNT||' FIRST='||p_dt.FIRST||' LAST='||p_dt.LAST);
  v_idx := p_dt.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    DBMS_OUTPUT.PUT_LINE(v_idx||'='||TO_CHAR(p_dt(v_idx), 'YYYY-MM-DD HH24:MI:SS'));
    p_dt(v_idx) := NVL(p_dt(v_idx) + 1, TRUNC(SYSDATE)-v_idx);
	v_idx := p_dt.NEXT(v_idx);
  END LOOP;
  p_dt(NVL(p_dt.LAST, 0)+1) := TRUNC(SYSDATE);
  DBMS_OUTPUT.PUT_LINE('p_dt.COUNT='||p_dt.COUNT||' FIRST='||p_dt.FIRST||' LAST='||p_dt.LAST);
END;

PROCEDURE p2(
	--p_int IN OUT int_tab_typ,
	p_num IN OUT num_tab_typ,
	p_vc IN OUT vc_tab_typ,
	p_dt IN OUT dt_tab_typ
--, p_lob IN OUT lob_tab_typ
) IS
BEGIN
  --inout_int(p_int);
  inout_num(p_num);
  inout_vc(p_vc);
  inout_dt(p_dt);
  --p_lob := NULL;
END p2;
END;
`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(err, qry)
	}
	compileErrors, err := godror.GetCompileErrors(ctx, testDb, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(compileErrors) != 0 {
		t.Logf("compile errors: %v", compileErrors)
		for _, ce := range compileErrors {
			if strings.Contains(ce.Error(), pkg) {
				t.Fatal(ce)
			}
		}
	}

	intgr := []int32{3, 1, 4, 0, 0}[:3]
	intgrWant := []int32{3 * 2, 1 * 2, 4 * 2, 3}
	_ = intgrWant
	num := []godror.Number{"3.14", "-2.48", ""}[:2]
	numWant := []godror.Number{"1.57", "-1.24", "2"}
	vc := []string{"string", "bring", ""}[:2]
	vcWant := []string{"string +", "bring +", "2"}
	var today time.Time
	qry = "SELECT TRUNC(SYSDATE) FROM DUAL"
	if testDb.QueryRowContext(ctx, qry).Scan(&today); err != nil {
		t.Fatal(err)
	}
	dt := []time.Time{
		time.Date(2017, 6, 18, 7, 5, 51, 0, time.Local),
		{},
		today.Add(-2 * 24 * time.Hour),
		today,
	}
	dt[1] = dt[0].Add(24 * time.Hour)
	dtWant := make([]time.Time, len(dt))
	for i, d := range dt {
		if i < len(dt)-1 {
			// p_dt(v_idx) := NVL(p_dt(v_idx) + 1, TRUNC(SYSDATE)-v_idx);
			dtWant[i] = d.AddDate(0, 0, 1)
		} else {
			// p_dt(NVL(p_dt.LAST, 0)+1) := TRUNC(SYSDATE);
			dtWant[i] = d
		}
	}
	dt = dt[:len(dt)-1]

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "ALTER SESSION SET time_zone = local"); err != nil {
		t.Fatal(err)
	}
	godror.EnableDbmsOutput(ctx, conn)

	opts := []cmp.Option{
		cmp.Comparer(func(x, y time.Time) bool { return x.Equal(y) }),
	}

	for _, tC := range []struct {
		In, Want any
		Name     string
	}{
		{Name: "vc", In: vc, Want: vcWant},
		{Name: "num", In: num, Want: numWant},
		{Name: "dt", In: dt, Want: dtWant},
		// {Name: "int", In: intgr, Want: intgrWant},
		{Name: "vc-1", In: vc[:1], Want: []string{"string +", "1"}},
		{Name: "vc-0", In: vc[:0], Want: []string{"0"}},
	} {
		t.Run("inout_"+tC.Name, func(t *testing.T) {
			t.Logf("%s=%s", tC.Name, tC.In)
			nm := strings.SplitN(tC.Name, "-", 2)[0]
			qry = "BEGIN " + pkg + ".inout_" + nm + "(:1); END;"
			dst := copySlice(tC.In)
			if _, err := conn.ExecContext(ctx, qry,
				godror.PlSQLArrays,
				sql.Out{Dest: dst, In: true},
			); err != nil {
				t.Fatalf("%s\n%#v\n%+v", qry, dst, err)
			}
			got := reflect.ValueOf(dst).Elem().Interface()
			if nm == "dt" {
				t.Logf("\nin =%v\ngot=%v\nwt= %v", tC.In, got, tC.Want)
			}

			if cmp.Equal(got, tC.Want, opts...) {
				return
			}
			t.Errorf("%s: %s", tC.Name, cmp.Diff(printSlice(tC.Want), printSlice(got)))
			var buf bytes.Buffer
			if err := godror.ReadDbmsOutput(ctx, &buf, conn); err != nil {
				t.Error(err)
			}
			t.Log("OUTPUT:", buf.String())
		})
	}

	// lob := []godror.Lob{godror.Lob{IsClob: true, Reader: strings.NewReader("abcdef")}}
	t.Run("p2", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx,
			"BEGIN "+pkg+".p2(:1, :2, :3); END;",
			godror.PlSQLArrays,
			// sql.Out{Dest: &intgr, In: true},
			sql.Out{Dest: &num, In: true},
			sql.Out{Dest: &vc, In: true},
			sql.Out{Dest: &dt, In: true},
			// sql.Out{Dest: &lob, In: true},
		); err != nil {
			t.Fatal(err)
		}
		t.Logf("int=%#v num=%#v vc=%#v dt=%#v", intgr, num, vc, dt)
		// if d := cmp.Diff(intgr, intgrWant); d != "" {
		//	t.Errorf("int: %s", d)
		// }
		if d := cmp.Diff(num, numWant); d != "" {
			t.Errorf("num: %s", d)
		}
		if d := cmp.Diff(vc, vcWant); d != "" {
			t.Errorf("vc: %s", d)
		}
		if !cmp.Equal(dt, dtWant, opts...) {
			if d := cmp.Diff(dt, dtWant); d != "" {
				t.Errorf("dt: %s", d)
			}
		}
	})
}

func TestOutParam(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("OutParam"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "ALTER SESSION SET time_zone = local"); err != nil {
		t.Fatal(err)
	}
	pkg := strings.ToUpper("test_p1" + tblSuffix)
	qry := `CREATE OR REPLACE PROCEDURE
` + pkg + `(p_int IN OUT INTEGER, p_num IN OUT NUMBER, p_vc IN OUT VARCHAR2, p_dt IN OUT DATE, p_lob IN OUT CLOB)
IS
BEGIN
  p_int := NVL(p_int * 2, 1);
  p_num := NVL(p_num / 2, 0.5);
  p_vc := NVL(p_vc ||' +', '-');
  p_dt := NVL(p_dt + 1, SYSDATE);
  p_lob := NULL;
END;`
	if _, err = conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(err, qry)
	}
	defer testDb.Exec("DROP PROCEDURE " + pkg)

	qry = "BEGIN " + pkg + "(:1, :2, :3, :4, :5); END;"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer stmt.Close()

	var intgr int = 3
	num := godror.Number("3.14")
	var vc string = "string"
	var dt time.Time = time.Date(2017, 6, 18, 7, 5, 51, 0, time.Local)
	var lob godror.Lob = godror.Lob{IsClob: true, Reader: strings.NewReader("abcdef")}
	if _, err := stmt.ExecContext(ctx,
		sql.Out{Dest: &intgr, In: true},
		sql.Out{Dest: &num, In: true},
		sql.Out{Dest: &vc, In: true},
		sql.Out{Dest: &dt, In: true},
		sql.Out{Dest: &lob, In: true},
	); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Logf("int=%#v num=%#v vc=%#v dt=%#v", intgr, num, vc, dt)
	if intgr != 6 {
		t.Errorf("int: got %d, wanted %d", intgr, 6)
	}
	if num != "1.57" {
		t.Errorf("num: got %q, wanted %q", num, "1.57")
	}
	if vc != "string +" {
		t.Errorf("vc: got %q, wanted %q", vc, "string +")
	}
}

func TestSelectRefCursor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectRefCursor"), 10*time.Second)
	defer cancel()
	rows, err := testDb.QueryContext(ctx, "SELECT CURSOR(SELECT object_name, object_type, object_id, created FROM all_objects WHERE ROWNUM <= 10) FROM DUAL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sub driver.Rows
		if err := rows.Scan(&sub); err != nil {
			sub.Close()
			t.Fatal(err)
		}
		defer sub.Close()
		t.Logf("%[1]T %[1]p", sub)
		cols := sub.(driver.RowsColumnTypeScanType).Columns()
		t.Log("Columns", cols)
		dests := make([]driver.Value, len(cols))
		for {
			if err := sub.Next(dests); err != nil {
				if err == io.EOF {
					break
				}
				sub.Close()
				t.Error(err)
				break
			}
			// fmt.Println(dests)
			t.Log(dests)
		}
		sub.Close()
	}
	// Test the Finalizers
	runtime.GC()
}

func TestSelectRefCursorWrap(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectRefCursorWrap"), 10*time.Second)
	defer cancel()
	rows, err := testDb.QueryContext(ctx, "SELECT CURSOR(SELECT object_name, object_type, object_id, created FROM all_objects WHERE ROWNUM <= 10) FROM DUAL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var intf driver.Rows
		if err := rows.Scan(&intf); err != nil {
			t.Error(err)
			continue
		}
		t.Logf("%[1]T %[1]p", intf)
		dr := intf
		sub, err := godror.WrapRows(ctx, testDb, dr)
		if err != nil {
			dr.Close()
			t.Fatal(err)
		}
		t.Log("Sub", sub)
		for sub.Next() {
			var oName, oType, oID string
			var created time.Time
			if err := sub.Scan(&oName, &oType, &oID, &created); err != nil {
				dr.Close()
				sub.Close()
				t.Error(err)
				break
			}
			t.Log(oName, oType, oID, created)
		}
		dr.Close()
		sub.Close()
	}
	// Test the Finalizers
	runtime.GC()
}

func TestExecRefCursor(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("ExecRefCursor"), 30*time.Second)
	defer cancel()
	funName := "test_rc" + tblSuffix
	funQry := "CREATE OR REPLACE FUNCTION " + funName + ` RETURN SYS_REFCURSOR IS
  v_cur SYS_REFCURSOR;
BEGIN
  OPEN v_cur FOR SELECT object_name FROM all_objects WHERE ROWNUM < 10;
  RETURN(v_cur);
END;`
	if _, err := testDb.ExecContext(ctx, funQry); err != nil {
		t.Fatalf("%s: %v", funQry, err)
	}
	defer testDb.ExecContext(ctx, `DROP FUNCTION `+funName)
	qry := "BEGIN :1 := " + funName + "; END;"
	var dr driver.Rows
	if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: &dr}); err != nil {
		t.Fatalf("%s: %v", qry, err)
	}
	defer dr.Close()
	sub, err := godror.WrapRows(ctx, testDb, dr)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	t.Log("Sub", sub)
	for sub.Next() {
		var s string
		if err := sub.Scan(&s); err != nil {
			t.Fatal(err)
		}
		t.Log(s)
	}
	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()
}

func TestExecuteMany(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()

	ctx, cancel := context.WithTimeout(testContext("ExecuteMany"), 30*time.Second)
	defer cancel()
	tbl := "test_em" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	testDb.ExecContext(ctx, "CREATE TABLE "+tbl+` (
		f_id INTEGER,
		f_int INTEGER,
		f_num NUMBER,
		f_num_6 NUMBER(6),
		F_num_5_2 NUMBER(5,2),
		f_vc VARCHAR2(30),
		F_dt DATE
	)`)
	defer testDb.Exec("DROP TABLE " + tbl)

	const num = 1000
	ints := make([]int, num)
	nums := make([]godror.Number, num)
	int32s := make([]int32, num)
	floats := make([]float64, num)
	strs := make([]string, num)
	dates := make([]time.Time, num)

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "ALTER SESSION SET time_zone = local"); err != nil {
		t.Fatal(err)
	}
	// This is instead of now: a nice moment in time right before the summer time shift
	now := time.Date(2017, 10, 29, 1, 27, 53, 0, time.Local).Truncate(time.Second)
	ids := make([]int, num)
	for i := range nums {
		ids[i] = i
		ints[i] = i << 1
		nums[i] = godror.Number(strconv.Itoa(i))
		int32s[i] = int32(i)
		floats[i] = float64(i) / float64(3.14)
		strs[i] = fmt.Sprintf("%x", i)
		dates[i] = now.Add(-time.Duration(i) * time.Hour)
	}

	t.Run("one-col", func(t *testing.T) {
		for i, tc := range []struct {
			Value any
			Name  string
		}{
			{Name: "f_int", Value: ints},
			{Name: "f_num", Value: nums},
			{Name: "f_num_6", Value: int32s},
			{Name: "f_num_5_2", Value: floats},
			{Name: "f_vc", Value: strs},
			{Name: "f_dt", Value: dates},
		} {
			res, execErr := tx.ExecContext(ctx,
				"INSERT INTO "+tbl+" ("+tc.Name+") VALUES (:1)", //nolint:gas
				tc.Value)
			if execErr != nil {
				t.Fatalf("%d. INSERT INTO "+tbl+" (%q) VALUES (%+v): %#v", //nolint:gas
					i, tc.Name, tc.Value, execErr)
			}
			ra, raErr := res.RowsAffected()
			if raErr != nil {
				t.Error(raErr)
			} else if ra != num {
				t.Errorf("%d. %q: wanted %d rows, got %d", i, tc.Name, num, ra)
			}
		}
		tx.Rollback()
	})

	testDb.ExecContext(ctx, "TRUNCATE TABLE "+tbl+"")

	t.Run("multi-col", func(t *testing.T) {
		if tx, err = testDb.BeginTx(ctx, nil); err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		res, err := tx.ExecContext(ctx,
			`INSERT INTO `+tbl+ //nolint:gas
				` (f_id, f_int, f_num, f_num_6, F_num_5_2, F_vc, F_dt)
			VALUES
			(:1, :2, :3, :4, :5, :6, :7)`,
			ids, ints, nums, int32s, floats, strs, dates)
		if err != nil {
			t.Fatalf("%#v", err)
		}
		ra, err := res.RowsAffected()
		if err != nil {
			t.Error(err)
		} else if ra != num {
			t.Errorf("wanted %d rows, got %d", num, ra)
		}

		rows, err := tx.QueryContext(ctx,
			"SELECT * FROM "+tbl+" ORDER BY F_id", //nolint:gas
		)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		i := 0
		for rows.Next() {
			var id, Int int
			var num string
			var vc string
			var num6 int32
			var num52 float64
			var dt time.Time
			if err := rows.Scan(&id, &Int, &num, &num6, &num52, &vc, &dt); err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("ID got %d, wanted %d.", id, i)
			}
			if Int != ints[i] {
				t.Errorf("%d. INT got %d, wanted %d.", i, Int, ints[i])
			}
			if num != string(nums[i]) {
				t.Errorf("%d. NUM got %q, wanted %q.", i, num, nums[i])
			}
			if num6 != int32s[i] {
				t.Errorf("%d. NUM_6 got %v, wanted %v.", i, num6, int32s[i])
			}
			rounded := float64(int64(floats[i]/0.005+0.5)) * 0.005
			if math.Abs(num52-rounded) > 0.05 {
				t.Errorf("%d. NUM_5_2 got %v, wanted %v.", i, num52, rounded)
			}
			if vc != strs[i] {
				t.Errorf("%d. VC got %q, wanted %q.", i, vc, strs[i])
			}
			t.Logf("%d. dt=%v", i, dt)
			if !dt.Equal(dates[i]) {
				if fmt.Sprintf("%v", dt) == "2017-10-29 02:27:53 +0100 CET" &&
					fmt.Sprintf("%v", dates[i]) == "2017-10-29 00:27:53 +0000 UTC" {
					t.Logf("%d. got DT %v, wanted %v (%v)", i, dt, dates[i], dt.Sub(dates[i]))
				} else {
					t.Errorf("%d. got DT %v, wanted %v (%v)", i, dt, dates[i], dt.Sub(dates[i]))
				}
			}
			i++
		}
	})

	testDb.ExecContext(ctx, "TRUNCATE TABLE "+tbl+"")

	t.Run("returning-one", func(t *testing.T) {
		if tx, err = testDb.BeginTx(ctx, nil); err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		qry := `DECLARE
  v_id PLS_INTEGER;
BEGIN
  INSERT INTO ` + tbl + `
			(f_id, f_int, f_num, f_num_6, F_num_5_2, F_vc, F_dt)
	VALUES
	(:1, :2, :3, :4, :5, :6, :7)
	RETURNING F_id INTO v_id;
	:8 := v_id;
END;`

		var id int
		res, err := tx.ExecContext(ctx, qry,
			ids[0], ints[0], nums[0], int32s[0], floats[0], strs[0], dates[0],
			sql.Out{Dest: &id},
		)
		if err != nil {
			t.Fatal(err)
		}
		ra, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("rowsAffected=%d", ra)
		t.Logf("id=%d", id)
		if id != ids[0] {
			t.Errorf("got id=%d, wanted %d", id, ids[0])
		}
	})

	testDb.ExecContext(ctx, "TRUNCATE TABLE "+tbl+"")

	t.Run("returning-many", func(t *testing.T) {
		if tx, err = testDb.BeginTx(ctx, nil); err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		qry := `DECLARE
	TYPE id_tt IS TABLE OF ` + tbl + `.F_id%TYPE INDEX BY PLS_INTEGER;
	l_ids id_tt := :ids;
	l_ints id_tt := :ints;
	TYPE num_tt IS TABLE OF ` + tbl + `.F_num%TYPE INDEX BY PLS_INTEGER;
	l_nums num_tt := :nums;
	TYPE num6_tt IS TABLE OF ` + tbl + `.F_num_6%TYPE INDEX BY PLS_INTEGER;
	l_num6s num6_tt := :int32s;
	TYPE num5_tt IS TABLE OF ` + tbl + `.F_num_5_2%TYPE INDEX BY PLS_INTEGER;
	l_num5s num5_tt := :floats;
	TYPE vc_tt IS TABLE OF ` + tbl + `.F_vc%TYPE INDEX BY PLS_INTEGER;
	l_strs vc_tt := :strs;
	TYPE dt_tt IS TABLE OF ` + tbl + `.F_dt%TYPE INDEX BY PLS_INTEGER;
	l_dts dt_tt := :dts;

  l_tab id_tt;
BEGIN
	FORALL i IN l_ids.FIRST .. l_ids.LAST
	  INSERT INTO ` + tbl + `
		(f_id, f_int, f_num, f_num_6, F_num_5_2, F_vc, F_dt)
	VALUES
	(l_ids(i), l_ints(i), l_nums(i), l_num6s(i), l_num5s(i), l_strs(i), l_dts(i))
	RETURNING F_id BULK COLLECT INTO l_tab;
	:rids := l_tab;
END;`

		rIDs := make([]godror.Number, len(ids))
		O := func(ss []int) []godror.Number {
			nn := make([]godror.Number, len(ss))
			for i, s := range ss {
				nn[i] = godror.Number(strconv.FormatInt(int64(s), 10))
			}
			return nn
		}
		O32 := func(ss []int32) []godror.Number {
			nn := make([]godror.Number, len(ss))
			for i, s := range ss {
				nn[i] = godror.Number(strconv.FormatInt(int64(s), 10))
			}
			return nn
		}
		OF := func(ss []float64) []godror.Number {
			nn := make([]godror.Number, len(ss))
			for i, s := range ss {
				nn[i] = godror.Number(strconv.FormatFloat(s, 'f', -1, 64))
			}
			return nn
		}

		res, err := tx.ExecContext(ctx, qry, godror.PlSQLArrays,
			sql.Named("ids", O(ids)), sql.Named("ints", O(ints)),
			sql.Named("nums", nums), sql.Named("int32s", O32(int32s)),
			sql.Named("floats", OF(floats)), sql.Named("strs", strs),
			sql.Named("dts", dates),
			sql.Named("rids", sql.Out{Dest: &rIDs}),
		)
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		ra, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("rowsAffected=%d", ra)
		t.Logf("ids=%v", rIDs)
		for i := range rIDs {
			if string(rIDs[i]) != strconv.Itoa(ids[i]) {
				t.Errorf("%d. got id=%s, wanted %d", i, rIDs[i], ids[i])
			}
		}
	})
}

func TestReadWriteLOB(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteLob"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_lob" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (f_id NUMBER(6), f_blob BLOB, f_clob CLOB)", //nolint:gas
	)
	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (F_id, f_blob, F_clob) VALUES (:1, :2, :3)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for tN, tC := range []struct {
		String string
		Bytes  []byte
	}{
		{Bytes: []byte{0, 1, 2, 3, 4, 5}, String: "12345"},
	} {

		if _, err = stmt.ExecContext(ctx, tN*2, tC.Bytes, tC.String); err != nil {
			t.Errorf("%d/1. (%v, %q): %v", tN, tC.Bytes, tC.String, err)
			continue
		}
		if _, err = stmt.ExecContext(ctx, tN*3+1,
			godror.Lob{Reader: bytes.NewReader(tC.Bytes)},
			godror.Lob{Reader: strings.NewReader(tC.String), IsClob: true},
		); err != nil {
			t.Errorf("%d/2. (%v, %q): %v", tN, tC.Bytes, tC.String, err)
		}

		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx,
			"SELECT F_id, F_blob, F_clob FROM "+tbl+" WHERE F_id IN (:1, :2)", //nolint:gas
			godror.LobAsReader(),
			2*tN, 2*tN+1)
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		for rows.Next() {
			var id, blob, clob any
			if err = rows.Scan(&id, &blob, &clob); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. blob=%+v clob=%+v", id, blob, clob)
			if blob, ok := blob.(*godror.Lob); !ok {
				t.Errorf("%d. %T is not LOB", id, blob)
			} else {
				got := make([]byte, len(tC.Bytes))
				n, err := io.ReadFull(blob, got)
				t.Logf("%d. BLOB read %d (%q =%t= %q): %+v", id, n, got, bytes.Equal(got, tC.Bytes), tC.Bytes, err)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				} else if !bytes.Equal(got, tC.Bytes) {
					t.Errorf("%d. got %q for BLOB, wanted %q", id, got, tC.Bytes)
				}
			}
			if clob, ok := clob.(*godror.Lob); !ok {
				t.Errorf("%d. %T is not LOB", id, clob)
			} else {
				var got []byte
				if got, err = io.ReadAll(clob); err != nil {
					t.Errorf("%d. %v", id, err)
				} else if got := string(got); got != tC.String {
					t.Errorf("%d. got %q for CLOB, wanted %q", id, got, tC.String)
				}
			}
		}
		rows.Close()
	}

	rows, err := conn.QueryContext(ctx,
		"SELECT F_blob, F_clob FROM "+tbl+"", //nolint:gas
		godror.ClobAsString())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		var s string
		if err = rows.Scan(&b, &s); err != nil {
			t.Error(err)
		}
		t.Logf("clobAsString: %q", s)
	}

	qry := "SELECT CURSOR(SELECT f_id, F_blob, f_clob FROM " + tbl + " WHERE ROWNUM <= 10) FROM DUAL"
	rows, err = testDb.QueryContext(ctx, qry, godror.ClobAsString())
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	for rows.Next() {
		var intf any
		if err := rows.Scan(&intf); err != nil {
			t.Error(err)
			continue
		}
		t.Logf("%T", intf)
		sub := intf.(driver.RowsColumnTypeScanType)
		cols := sub.Columns()
		t.Log("Columns", cols)
		dests := make([]driver.Value, len(cols))
		for {
			if err := sub.Next(dests); err != nil {
				if err == io.EOF {
					break
				}
				t.Error(err)
				break
			}
			// fmt.Println(dests)
			t.Log(dests)
		}
		sub.Close()
	}

}

func TestReadWriteBfile(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWritBfile"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_Bfile" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (f_id NUMBER(6), f_bf BFILE)", //nolint:gas
	)
	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (F_id, f_bf) VALUES (:1, BFILENAME(:2, :3))", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for tN, tC := range []struct {
		Dir  string
		File string
	}{
		{"TEST", "1.txt"},
	} {

		if _, err = stmt.ExecContext(ctx, tN*2, tC.Dir, tC.File); err != nil {
			t.Errorf("%d/1. (%s, %s): %v", tN, tC.Dir, tC.File, err)
			continue
		}

		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx,
			"SELECT F_id, F_bf FROM "+tbl+" WHERE F_id = :1", //nolint:gas
			2*tN)
		if err != nil {
			t.Errorf("%d/2. %v", tN, err)
			continue
		}
		for rows.Next() {
			var id, bfile any
			if err = rows.Scan(&id, &bfile); err != nil {
				rows.Close()
				t.Errorf("%d/2. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. bfile=%+v", id, bfile)
			if b, ok := bfile.(*godror.Lob); !ok {
				t.Errorf("%d. %T is not LOB", id, b)
			} else {
				lobD, err := b.Hijack()
				if err != nil {
					t.Error(err)
				}
				dir, file, err := lobD.GetFileName()
				if err != nil {
					t.Error(err)
				}
				if dir != tC.Dir {
					t.Errorf("the got dir %v not equal want %v", dir, tC.Dir)
				}
				if file != tC.File {
					t.Errorf("the got file %v not equal want %v", file, tC.File)
				}
			}
		}
		rows.Close()
	}
}

func printSlice(orig any) any {
	ro := reflect.ValueOf(orig)
	if ro.Kind() == reflect.Pointer {
		ro = ro.Elem()
	}
	ret := make([]string, 0, ro.Len())
	for i := 0; i < ro.Len(); i++ {
		ret = append(ret, fmt.Sprintf("%v", ro.Index(i).Interface()))
	}
	return ret
}
func copySlice(orig any) any {
	ro := reflect.ValueOf(orig)
	rc := reflect.New(reflect.TypeOf(orig)).Elem() // *[]s
	rc.Set(reflect.MakeSlice(ro.Type(), ro.Len(), ro.Cap()+1))
	for i := 0; i < ro.Len(); i++ {
		rc.Index(i).Set(ro.Index(i))
	}
	return rc.Addr().Interface()
}

func TestOpenCloseDB(t *testing.T) {
	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.MinSessions, cs.MaxSessions = 4, 4
	cs.StandaloneConnection = godror.Bool(true)
	const countQry = "SELECT COUNT(0) FROM user_objects"
	ctx, cancel := context.WithCancel(testContext("OpenCloseDB"))
	defer cancel()
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(cs.MaxSessions)
	for i := range 32 {
		cs := cs
		cs.WaitTimeout += time.Duration(i)
		grp.Go(func() error {
			// To make the connections differ
			t.Log(cs.String())
			db := sql.OpenDB(godror.NewConnector(cs))
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				return fmt.Errorf("Open: %w", err)
			}
			defer conn.Close()
			var n int32
			err = conn.QueryRowContext(ctx, countQry).Scan(&n)
			t.Logf("object count: %d (%+v)", n, err)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCloseConn(t *testing.T) {
	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.MinSessions, cs.MaxSessions = 4, 4
	t.Log(cs.String())
	db := sql.OpenDB(godror.NewConnector(cs))
	defer db.Close()
	const countQry = "SELECT COUNT(0) FROM user_objects"
	ctx, cancel := context.WithCancel(testContext("OpenCloseConn"))
	defer cancel()
	limitCh := make(chan struct{}, cs.MaxSessions)
	grp, ctx := errgroup.WithContext(ctx)
	for range 100 {
		limitCh <- struct{}{}
		grp.Go(func() error {
			defer func() { <-limitCh }()
			conn, err := db.Conn(ctx)
			if err != nil {
				return fmt.Errorf("Open: %w", err)
			}
			defer conn.Close()
			var n int32
			err = conn.QueryRowContext(ctx, countQry).Scan(&n)
			t.Logf("object count: %d (%+v)", n, err)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCloseTx(t *testing.T) {
	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.MinSessions, cs.MaxSessions = 6, 6
	t.Log(cs.String())
	db, err := sql.Open("godror", cs.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cErr := db.Close(); cErr != nil {
			t.Error("CLOSE:", cErr)
		}
	}()
	db.SetMaxIdleConns(cs.MinSessions)
	db.SetMaxOpenConns(cs.MaxSessions)
	ctx, cancel := context.WithCancel(testContext("OpenCloseTx"))
	defer cancel()
	const module = "godror.v2.test-OpenCloseTx "
	const countQry = "SELECT COUNT(0) FROM v$session WHERE module LIKE '" + module + "%'"
	stmt, err := db.PrepareContext(ctx, countQry)
	if err != nil {
		if strings.Contains(err.Error(), "ORA-12516:") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer stmt.Close()

	sessCount := func() (n int, stats godror.PoolStats, err error) {
		var sErr error
		sErr = godror.Raw(ctx, db, func(cx godror.Conn) error {
			var gErr error
			stats, gErr = cx.GetPoolStats()
			return gErr
		})
		if qErr := stmt.QueryRowContext(ctx).Scan(&n); qErr != nil && sErr == nil {
			sErr = qErr
		}
		return n, stats, sErr
	}
	n, ps, err := sessCount()
	if err != nil {
		t.Skip(err)
	}
	if n > 0 {
		t.Logf("sessCount=%d, stats=%s at start!", n, ps)
	}
	var tt godror.TraceTag
	for i := 0; i < cs.MaxSessions*2; i++ {
		t.Logf("%d. PREPARE", i+1)
		stmt, err := db.PrepareContext(ctx, "SELECT 1 FROM DUAL")
		if err != nil {
			t.Fatal(err)
		}
		if n, ps, err = sessCount(); err != nil {
			t.Error(err)
		} else {
			t.Logf("sessCount=%d stats=%s", n, ps)
		}
		tt.Module = fmt.Sprintf("%s%d", module, 2*i)
		ctx = godror.ContextWithTraceTag(ctx, tt)
		tx1, err1 := db.BeginTx(ctx, nil)
		if err1 != nil {
			t.Fatal(err1)
		}
		tt.Module = fmt.Sprintf("%s%d", module, 2*i+1)
		ctx = godror.ContextWithTraceTag(ctx, tt)
		tx2, err2 := db.BeginTx(ctx, nil)
		if err2 != nil {
			if strings.Contains(err2.Error(), "ORA-12516:") {
				tx1.Rollback()
				break
			}
			t.Fatal(err2)
		}
		if n, ps, err = sessCount(); err != nil {
			t.Log(err)
		} else if n == 0 {
			t.Errorf("sessCount=0, stats=%s want at least 2", ps)
		} else {
			t.Logf("sessCount=%d stats=%s", n, ps)
		}
		tx1.Rollback()
		tx2.Rollback()
		stmt.Close()
	}
	if n, ps, err = sessCount(); err != nil {
		t.Log(err)
	} else if n > 7 {
		t.Errorf("sessCount=%d stats=%s", n, ps)
	}
}

func TestOpenBadMemory(t *testing.T) {
	var mem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mem)
	t.Log("Allocated 0:", mem.Alloc)
	zero := mem.Alloc
	for i := range 100 {
		badConStr := strings.Replace(testConStr, "@", fmt.Sprintf("BAD%dBAD@", i), 1)
		db, err := sql.Open("godror", badConStr)
		if err != nil {
			t.Fatalf("bad connection string %q didn't produce error!", badConStr)
		}
		db.Close()
		runtime.GC()
		runtime.ReadMemStats(&mem)
		t.Logf("Allocated %d: %d", i+1, mem.Alloc)
	}
	d := mem.Alloc - zero
	if mem.Alloc < zero {
		d = 0
	}
	t.Logf("atlast: %d", d)
	if d > 64<<10 {
		t.Errorf("Consumed more than 64KiB of memory: %d", d)
	}
}

func TestSelectFloat(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectFloat"), 10*time.Second)
	defer cancel()
	tbl := "test_numbers" + tblSuffix
	qry := `CREATE TABLE ` + tbl + ` (
  INT_COL     NUMBER,
  FLOAT_COL  NUMBER,
  EMPTY_INT_COL NUMBER
)`
	testDb.Exec("DROP TABLE " + tbl)
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	const INT, FLOAT = 1234567, 4.5
	qry = `INSERT INTO ` + tbl + //nolint:gas
		` (INT_COL, FLOAT_COL, EMPTY_INT_COL)
     VALUES (1234567, 45/10, NULL)`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	qry = "SELECT int_col, float_col, empty_int_col FROM " + tbl //nolint:gas
	type numbers struct {
		Number  godror.Number
		String  string
		NString sql.NullString
		NInt    sql.NullInt64
		Int64   int64
		Float   float64
		Int     int
	}
	var n numbers
	var i1, i2, i3 any
	for tName, tC := range map[string]struct {
		Dest [3]any
		Want numbers
	}{
		"int,float,nstring": {
			Dest: [3]any{&n.Int, &n.Float, &n.NString},
			Want: numbers{Int: INT, Float: FLOAT},
		},
		"inf,float,Number": {
			Dest: [3]any{&n.Int, &n.Float, &n.Number},
			Want: numbers{Int: INT, Float: FLOAT},
		},
		"int64,float,nullInt": {
			Dest: [3]any{&n.Int64, &n.Float, &n.NInt},
			Want: numbers{Int64: INT, Float: FLOAT},
		},
		"intf,intf,intf": {
			Dest: [3]any{&i1, &i2, &i3},
			Want: numbers{Int64: INT, Float: FLOAT},
		},
	} {
		i1, i2, i3 = nil, nil, nil
		n = numbers{}
		F := func() error {
			err := testDb.QueryRowContext(ctx, qry).Scan(tC.Dest[0], tC.Dest[1], tC.Dest[2])
			if err != nil {
				err = fmt.Errorf("%s: %w", qry, err)
			}
			return err
		}
		if err := F(); err != nil {
			if strings.HasSuffix(err.Error(), "unsupported Scan, storing driver.Value type <nil> into type *string") {
				t.Log("WARNING:", err)
				continue
			}
			noLogging := tl.enableLogging(t)
			err = F()
			t.Errorf("%q: %v", tName, fmt.Errorf("%s: %w", qry, err))
			noLogging()
			continue
		}
		if tName == "intf,intf,intf" {
			t.Logf("%q: %#v, %#v, %#v", tName, i1, i2, i3)
			continue
		}
		t.Logf("%q: %+v", tName, n)
		if n != tC.Want {
			t.Errorf("%q:\ngot\t%+v,\nwanted\t%+v.", tName, n, tC.Want)
		}
	}
}

func TestNumInputs(t *testing.T) {
	t.Parallel()
	var a, b string
	if err := testDb.QueryRow("SELECT :1, :2 FROM DUAL", 'a', 'b').Scan(&a, &b); err != nil {
		t.Errorf("two inputs: %+v", err)
	}
	if err := testDb.QueryRow("SELECT :a, :b FROM DUAL", 'a', 'b').Scan(&a, &b); err != nil {
		t.Errorf("two named inputs: %+v", err)
	}
	if err := testDb.QueryRow("SELECT :a, :a FROM DUAL", sql.Named("a", a)).Scan(&a, &b); err != nil {
		t.Errorf("named inputs: %+v", err)
	}
}

func TestPtrArg(t *testing.T) {
	t.Parallel()
	s := "dog"
	rows, err := testDb.Query("SELECT * FROM user_objects WHERE object_name=:1", &s)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

func TestRanaOraIssue244(t *testing.T) {
	tableName := "test_ora_issue_244" + tblSuffix
	qry := "CREATE TABLE " + tableName + " (FUND_ACCOUNT VARCHAR2(18) NOT NULL, FUND_CODE VARCHAR2(6) NOT NULL, BUSINESS_FLAG NUMBER(10) NOT NULL, MONEY_TYPE VARCHAR2(3) NOT NULL)"
	testDb.Exec("DROP TABLE " + tableName)
	if _, err := testDb.Exec(qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	var max int
	ctx, cancel := context.WithCancel(testContext("RanaOraIssue244-1"))
	txs := make([]*sql.Tx, 0, maxSessions/2)
	for max = 0; max < maxSessions/2; max++ {
		tx, err := testDb.BeginTx(ctx, nil)
		if err != nil {
			max--
			break
		}
		txs = append(txs, tx)
	}
	for _, tx := range txs {
		tx.Rollback()
	}
	cancel()
	t.Logf("maxSessions=%d max=%d", maxSessions, max)

	dur := time.Minute / 2
	if testing.Short() {
		dur = 10 * time.Second
	}
	ctx, cancel = context.WithTimeout(testContext("RanaOraIssue244-2"), dur)
	defer cancel()
	defer testDb.Exec("DROP TABLE " + tableName)
	const bf = "143"
	const sc = "270004"
	qry = "INSERT INTO " + tableName + " (fund_account, fund_code, business_flag, money_type) VALUES (:1, :2, :3, :4)" //nolint:gas
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	fas := []string{"14900666", "1868091", "1898964", "14900397"}
	for _, v := range fas {
		if _, err := stmt.ExecContext(ctx, v, sc, bf, "0"); err != nil {
			stmt.Close()
			t.Fatal(err)
		}
	}
	stmt.Close()
	tx.Commit()

	qry = `SELECT fund_account, money_type FROM ` + tableName + ` WHERE business_flag = :1 AND fund_code = :2 AND fund_account = :3` //nolint:gas
	grp, grpCtx := errgroup.WithContext(ctx)
	for i := 0; i < max; i++ {
		index := rand.Intn(len(fas))
		i, qry := i, qry
		grp.Go(func() error {
			tx, err := testDb.BeginTx(grpCtx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			defer tx.Rollback()

			stmt, err := tx.PrepareContext(grpCtx, qry)
			if err != nil {
				return fmt.Errorf("%d.Prepare %q: %w", i, qry, err)
			}
			defer stmt.Close()

			for j := range 3 {
				select {
				case <-grpCtx.Done():
					return grpCtx.Err()
				default:
				}
				index = (index + 1) % len(fas)
				rows, err := stmt.QueryContext(grpCtx, bf, sc, fas[index])
				if err != nil {
					return fmt.Errorf("%d.tx=%p stmt=%p %d. %q: %w", i, tx, stmt, j, qry, err)
				}

				for rows.Next() {
					var acc, mt string
					if err = rows.Scan(&acc, &mt); err != nil {
						err = fmt.Errorf("Scan: %w", err)
						break
					}

					if acc != fas[index] {
						err = fmt.Errorf("got acc %q, wanted %q", acc, fas[index])
						break
					}
					if mt != "0" {
						err = fmt.Errorf("got mt %q, wanted 0", mt)
						break
					}
				}
				rows.Close()
				if err == nil {
					err = rows.Err()
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		if errors.Is(err, driver.ErrBadConn) {
			return
		}
		errS := errors.Unwrap(err).Error()
		switch errS {
		case "sql: statement is closed",
			"sql: transaction has already been committed or rolled back":
			return
		}
		if strings.Contains(errS, "ORA-12516:") || strings.Contains(errS, "ORA-24496:") {
			t.Log(err)
		} else {
			t.Error(err)
		}
	}
}

func TestNumberMarshal(t *testing.T) {
	t.Parallel()
	var n godror.Number
	if err := testDb.QueryRow("SELECT 6000370006565900000073 FROM DUAL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	t.Log(n.String())
	b, err := n.MarshalJSON()
	t.Logf("%s", b)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte{'e'}) {
		t.Errorf("got %q, wanted without scientific notation", b)
	}
	if b, err = json.Marshal(struct {
		N godror.Number
	}{N: n},
	); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", b)
}

func TestExecHang(t *testing.T) {
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("ExecHang"), 1*time.Second)
	defer cancel()
	done := make(chan error, 1)
	errMismatch := errors.New("mismatch")
	var wg sync.WaitGroup
	for i := 0; i < cap(done); i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				done <- err
				return
			}
			_, err := testDb.ExecContext(ctx, "BEGIN DBMS_SESSION.sleep(3); END;")
			t.Logf("%d. %v", i, err)
			if err == nil {
				done <- fmt.Errorf("%w: %d. wanted timeout got %+v", errMismatch, i, err)
			}
		}()
	}
	wg.Wait()
	close(done)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

}

func TestNumberNull(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("NumberNull"), time.Minute)
	defer cancel()
	testDb.Exec("DROP TABLE number_test")
	qry := `CREATE TABLE number_test (
		caseNum NUMBER(3),
		precisionNum NUMBER(5),
      precScaleNum NUMBER(5, 0),
		normalNum NUMBER
		)`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP TABLE number_test")

	qry = `
		INSERT ALL
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (1, 4, 65, 123)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (2, NULL, NULL, NULL)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (3, NULL, NULL, NULL)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (4, NULL, 42, NULL)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (5, NULL, NULL, 31)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (6, 3, 3, 4)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (7, NULL, NULL, NULL)
		INTO number_test (caseNum, precisionNum, precScaleNum, normalNum) VALUES (8, 6, 9, 7)
		SELECT 1 FROM DUAL`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	qry = "SELECT precisionNum, precScaleNum, normalNum FROM number_test ORDER BY caseNum"
	rows, err := testDb.Query(qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()

	for rows.Next() {
		var precisionNum, recScaleNum, normalNum sql.NullInt64
		if err = rows.Scan(&precisionNum, &recScaleNum, &normalNum); err != nil {
			t.Fatal(err)
		}

		t.Log(precisionNum, recScaleNum, normalNum)

		if precisionNum.Int64 == 0 && precisionNum.Valid {
			t.Errorf("precisionNum=%v, wanted {0 false}", precisionNum)
		}
		if recScaleNum.Int64 == 0 && recScaleNum.Valid {
			t.Errorf("recScaleNum=%v, wanted {0 false}", recScaleNum)
		}
	}

	rows, err = testDb.Query(qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()

	for rows.Next() {
		var precisionNumStr, recScaleNumStr, normalNumStr sql.NullString
		if err = rows.Scan(&precisionNumStr, &recScaleNumStr, &normalNumStr); err != nil {
			t.Fatal(err)
		}
		t.Log(precisionNumStr, recScaleNumStr, normalNumStr)
	}
}

func TestNullFloat(t *testing.T) {
	t.Parallel()
	testDb.Exec("DROP TABLE test_char")
	if _, err := testDb.Exec(`CREATE TABLE test_char (
			CHARS VARCHAR2(10 BYTE),
			FLOATS NUMBER(10, 2)
		)`); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_char")

	tx, err := testDb.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO test_char VALUES(:CHARS, :FLOATS)",
		[]string{"dog", "", "cat"},
		/*[]sql.NullString{sql.NullString{"dog", true},
		sql.NullString{"", false},
		sql.NullString{"cat", true}},*/
		[]sql.NullFloat64{
			{Float64: 3.14, Valid: true},
			{Float64: 12.36, Valid: true},
			{Float64: 0.0, Valid: false},
		},
	)
	if err != nil {
		t.Error(err)
	}
}

func TestColumnSize(t *testing.T) {
	t.Parallel()
	testDb.Exec("DROP TABLE test_column_size")
	if _, err := testDb.Exec(`CREATE TABLE test_column_size (
		vc20b VARCHAR2(20 BYTE),
		vc1b VARCHAR2(1 BYTE),
		nvc20 NVARCHAR2(20),
		nvc1 NVARCHAR2(1),
		vc20c VARCHAR2(20 CHAR),
		vc1c VARCHAR2(1 CHAR)
	)`); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_column_size")

	r, err := testDb.Query("SELECT * FROM test_column_size")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rts, err := r.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range rts {
		l, _ := col.Length()

		t.Logf("Column %q has length %v", col.Name(), l)
	}
}

func TestColumnPrecision(t *testing.T) {
	t.Parallel()
	testDb.Exec("DROP TABLE test_column_precision")
	if _, err := testDb.Exec(`CREATE TABLE test_column_precision (
            timestamp_s TIMESTAMP(0),
            timestamp_ms TIMESTAMP(3),
	    timestamp_us TIMESTAMP(6),
	    timestamp_ns TIMESTAMP(9),
            num NUMBER(10)
	)`); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_column_precision")

	r, err := testDb.Query("SELECT * FROM test_column_precision")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rts, err := r.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	if len(rts) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(rts))
	}

	expected := []int64{0, 3, 6, 9, 10}
	for i, col := range rts {
		prec, _, ok := col.DecimalSize()

		if !ok {
			t.Fatalf("column %q: expected precision, got none", col.Name())
		} else if prec != expected[i] {
			t.Fatalf("column %q: expected precision %d, got %d", col.Name(), expected[i], prec)
		}
	}
}

func TestColumnPrecisionDescribeQuery(t *testing.T) {
	t.Parallel()
	testDb.Exec("DROP TABLE test_column_precision_describe_query")
	if _, err := testDb.Exec(`CREATE TABLE test_column_precision_describe_query (
            timestamp_s TIMESTAMP(0),
            timestamp_ms TIMESTAMP(3),
	    timestamp_us TIMESTAMP(6),
	    timestamp_ns TIMESTAMP(9),
            num NUMBER(10)
	)`); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_column_precision")

	cols, err := godror.DescribeQuery(context.Background(), testDb, "SELECT * FROM test_column_precision_describe_query")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}
	expected := []int{0, 3, 6, 9, 10}
	for i, col := range cols {
		if col.Precision != expected[i] {
			t.Fatalf("column %q: expected precision %d, got %d", col.Name, expected[i], col.Precision)
		}
	}
}

func TestReturning(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	testDb.Exec("DROP TABLE test_returning")
	if _, err := testDb.Exec("CREATE TABLE test_returning (a VARCHAR2(20))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_returning")

	want := "abraca dabra"
	var got string
	if _, err := testDb.Exec(
		`INSERT INTO test_returning (a) VALUES (UPPER(:1)) RETURNING a INTO :2`,
		want, sql.Out{Dest: &got},
	); err != nil {
		t.Fatal(err)
	}
	want = strings.ToUpper(want)
	if want != got {
		t.Errorf("got %q, wanted %q", got, want)
	}

	if _, err := testDb.Exec(
		`UPDATE test_returning SET a = '1' WHERE 1=0 RETURNING a /*LASTINSERTID*/ INTO :1`,
		sql.Out{Dest: &got},
	); err != nil {
		t.Fatal(err)
	}
	t.Logf("RETURNING (zero set): %v", got)
}

func TestMaxOpenCursorsORA1000(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext("ORA1000"))
	defer cancel()
	rows, err := testDb.QueryContext(ctx, "SELECT * FROM user_objects WHERE ROWNUM < 100")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var openCursors sql.NullInt64
	const qry1 = "SELECT p.value FROM v$parameter p WHERE p.name = 'open_cursors'"
	if err := testDb.QueryRowContext(ctx, qry1).Scan(&openCursors); err == nil {
		t.Logf("open_cursors=%v", openCursors)
	} else {
		if err := testDb.QueryRow(qry1).Scan(&openCursors); err != nil {
			var cErr interface{ Code() int }
			if errors.As(err, &cErr) && cErr.Code() == 942 {
				t.Logf("%s: %+v", qry1, err)
			} else {
				t.Error(fmt.Errorf("%s: %w", qry1, err))
			}
		} else {
			t.Log(fmt.Errorf("%s: %w", qry1, err))
		}
	}
	n := int(openCursors.Int64)
	if 0 <= n || n >= 100 {
		n = 100
	}
	n *= 2
	for i := 0; i < n; i++ {
		var cnt int64
		qry2 := "SELECT /* " + strconv.Itoa(i) + " */ 1 FROM DUAL"
		if err = testDb.QueryRowContext(ctx, qry2).Scan(&cnt); err != nil {
			t.Fatal(fmt.Errorf("%d. %s: %w", i, qry2, err))
		}
	}
}

func TestRO(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(testContext("RO"))
	defer cancel()
	tx, err := testDb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.QueryContext(ctx, "SELECT 1 FROM DUAL"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE test_table (i INTEGER)"); err == nil {
		t.Log("RO allows CREATE TABLE ?")
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestNullIntoNum(t *testing.T) {
	t.Parallel()
	testDb.Exec("DROP TABLE test_null_num")
	qry := "CREATE TABLE test_null_num (i NUMBER(3))"
	if _, err := testDb.Exec(qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP TABLE test_null_num")

	qry = "INSERT INTO test_null_num (i) VALUES (:1)"
	var i *int
	if _, err := testDb.Exec(qry, i); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
}

func TestPing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(testContext("Ping"), 1*time.Second)
	defer cancel()
	err := testDb.PingContext(ctx)
	cancel()
	if err != nil {
		t.Error(err)
	}

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.ConnectString = "--BAD---" + P.ConnectString
	badDB, err := sql.Open("godror", P.String())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(testContext("Ping"), 1*time.Second)
	defer cancel()

	dl, _ := ctx.Deadline()
	err = badDB.PingContext(ctx)
	ok := dl.After(time.Now())
	if err != nil {
		t.Log(err)
	} else {
		t.Log("ping succeeded")
		if !ok {
			t.Error("ping succeeded after deadline!")
		}
	}
}

func TestNoConnectionPooling(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("godror",
		strings.Replace(
			strings.Replace(testConStr, "TestClassName", godror.NoConnectionPoolingConnectionClass, 1),
			"standaloneConnection=0", "standaloneConnection=1", 1,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestExecTimeout(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("ExecTimeout"), 100*time.Millisecond)
	defer cancel()
	if _, err := testDb.ExecContext(ctx, "DECLARE cnt PLS_INTEGER; BEGIN SELECT COUNT(0) INTO cnt FROM (SELECT 1 FROM all_objects WHERE ROWNUM < 1000), (SELECT 1 FROM all_objects WHERE rownum < 1000); END;"); err != nil {
		t.Log(err)
	}
}

func TestQueryTimeout(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("QueryTimeout"), 100*time.Millisecond)
	defer cancel()
	if rows, err := testDb.QueryContext(ctx, "SELECT COUNT(0) FROM (SELECT 1 FROM all_objects WHERE rownum < 1000), (SELECT 1 FROM all_objects WHERE rownum < 1000)"); err != nil {
		t.Log(err)
	} else {
		rows.Close()
	}
}
func TestQueryLOBTimeout(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("QueryLOBTimeout"), 10*time.Second)
	defer cancel()
	tbl := "test_query_lob_" + tblSuffix
	drop := func() { testDb.ExecContext(context.Background(), "DROP TABLE "+tbl) }
	drop()
	qry := "CREATE TABLE " + tbl + " (i NUMBER(3), lob BLOB)"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer drop()

	// Generate some lob, 1MiB size
	length := int(1 << 20)
	lobGen := godror.Lob{
		Reader: ReaderFunc(func(p []byte) (int, error) {
			if length <= 0 {
				return 0, io.EOF
			}
			var n int
			for i := 0; i < len(p) && length > 0; i++ {
				p[i] = byte(length & 0xff)
				n++
				length--
			}
			if length <= 0 {
				return n, io.EOF
			}
			return n, nil
		}),
	}
	qry = "INSERT INTO " + tbl + " (i, lob) VALUES (0, :1)"
	if _, err := testDb.ExecContext(ctx, qry, lobGen); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}

	ctx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if rows, err := testDb.QueryContext(ctx, "SELECT A.i, A.lob FROM all_objects B, "+tbl+" A FETCH FIRST 1 ROW ONLY"); err != nil {
		t.Log(err)
	} else {
		defer rows.Close()
		var i int
		var s []byte
		for rows.Next() {
			if err := rows.Scan(&i, &s); err != nil {
				t.Errorf("scan: %+v", err)
				break
			}
			fmt.Println(i, len(s))
			t.Log(i, len(s))
		}
	}
}

func TestSDO(t *testing.T) {
	// t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SDO"), 30*time.Second)
	defer cancel()
	innerQry := `SELECT MDSYS.SDO_GEOMETRY(
	3001,
	NULL,
	NULL,
	MDSYS.SDO_ELEM_INFO_ARRAY(
		1,1,1,4,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
	),
	MDSYS.SDO_ORDINATE_ARRAY(
		480736.567,10853969.692,0,0.998807402795312,-0.0488238888381834,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
		) SHAPE FROM DUAL`
	selectQry := `SELECT shape, DUMP(shape), CASE WHEN shape IS NULL THEN 'I' ELSE 'N' END FROM (` + innerQry + ")"
	rows, err := testDb.QueryContext(ctx, selectQry)
	if err != nil {
		if !strings.Contains(err.Error(), `ORA-00904: "MDSYS"."SDO_GEOMETRY"`) {
			t.Fatal(fmt.Errorf("%s: %w", selectQry, err))
		}
		for _, qry := range []string{
			`CREATE TYPE test_sdo_point_type AS OBJECT (
			   X NUMBER,
			   Y NUMBER,
			   Z NUMBER)`,
			"CREATE TYPE test_sdo_elem_info_array AS VARRAY (1048576) of NUMBER",
			"CREATE TYPE test_sdo_ordinate_array AS VARRAY (1048576) of NUMBER",
			`CREATE TYPE test_sdo_geometry AS OBJECT (
			 SDO_GTYPE NUMBER,
			 SDO_SRID NUMBER,
			 SDO_POINT test_SDO_POINT_TYPE,
			 SDO_ELEM_INFO test_SDO_ELEM_INFO_ARRAY,
			 SDO_ORDINATES test_SDO_ORDINATE_ARRAY)`,

			`CREATE TABLE test_sdo(
					id INTEGER not null,
					shape test_SDO_GEOMETRY not null
				)`,
		} {
			var drop string
			if strings.HasPrefix(qry, "CREATE TYPE") {
				drop = "DROP TYPE " + qry[12:strings.Index(qry, " AS")] + " FORCE"
			} else {
				drop = "DROP TABLE " + qry[13:strings.Index(qry, "(")]
			}
			testDb.ExecContext(ctx, drop)
			t.Log(drop)
			if _, err = testDb.ExecContext(ctx, qry); err != nil {
				err = fmt.Errorf("%s: %w", qry, err)
				t.Log(err)
				if !strings.Contains(err.Error(), "ORA-01031:") {
					t.Fatal(err)
				}
				t.Skip(err)
			}
			defer testDb.ExecContext(ctx, drop)
		}

		selectQry = strings.Replace(selectQry, "MDSYS.SDO_", "test_SDO_", -1)
		if rows, err = testDb.QueryContext(ctx, selectQry); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", selectQry, err))
		}

	}
	defer rows.Close()
	for rows.Next() {
		var dmp, isNull string
		var intf any
		if err = rows.Scan(&intf, &dmp, &isNull); err != nil {
			t.Error(fmt.Errorf("%s: %w", "scan", err))
		}
		t.Log(dmp, isNull)
		obj := intf.(*godror.Object)
		// t.Log("obj:", obj)
		printObj(t, "", obj)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func printObj(t *testing.T, name string, obj *godror.Object) {
	if obj == nil {
		return
	}
	for key := range obj.Attributes {
		sub, err := obj.Get(key)
		t.Logf("%s.%s. %+v (err=%+v)\n", name, key, sub, err)
		if err != nil {
			t.Errorf("ERROR: %+v", err)
		}
		if ss, ok := sub.(*godror.Object); ok {
			printObj(t, name+"."+key, ss)
		} else if coll, ok := sub.(*godror.ObjectCollection); ok {
			slice, err := coll.AsSlice(nil)
			t.Logf("%s.%s. %+v", name, key, slice)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

var _ = driver.Valuer((*Custom)(nil))
var _ = sql.Scanner((*Custom)(nil))

type Custom struct {
	Num int64
}

func (t *Custom) Value() (driver.Value, error) {
	return t.Num, nil
}

func (t *Custom) Scan(v any) error {
	var err error
	switch v := v.(type) {
	case int64:
		t.Num = v
	case string:
		t.Num, err = strconv.ParseInt(v, 10, 64)
	case float64:
		t.Num = int64(v)
	default:
		err = fmt.Errorf("unknown type %T", v)
	}
	return err
}

func TestSelectCustomType(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectCustomType"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_custom_type" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (nm VARCHAR2(30), typ VARCHAR2(30), id NUMBER(6), created DATE)"
	if _, err = conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	n := 1000
	nms, typs, ids, createds := make([]string, n), make([]string, n), make([]int, n), make([]time.Time, n)
	now := time.Now()
	for i := range nms {
		nms[i], typs[i], ids[i], createds[i] = fmt.Sprintf("obj-%d", i), "OBJECT", i, now.Add(-time.Duration(i)*time.Second)
	}
	qry = "INSERT INTO " + tbl + " (nm, typ, id, created) VALUES (:1, :2, :3, :4)"
	if _, err = conn.ExecContext(ctx, qry, nms, typs, ids, createds); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	const num = 10
	nums := &Custom{Num: num}
	type underlying int64
	numbers := underlying(num)
	rows, err := conn.QueryContext(ctx,
		"SELECT nm, typ, id, created FROM "+tbl+" WHERE ROWNUM < COALESCE(:alpha, :beta, 2) ORDER BY id",
		sql.Named("alpha", nums),
		sql.Named("beta", int64(numbers)),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	n = 0
	oldOid := int64(0)
	for rows.Next() {
		var tbl, typ string
		var oid int64
		var created time.Time
		if err := rows.Scan(&tbl, &typ, &oid, &created); err != nil {
			t.Fatal(err)
		}
		t.Log(tbl, typ, oid, created)
		if tbl == "" {
			t.Fatal("empty tbl")
		}
		n++
		if oldOid > oid {
			t.Errorf("got oid=%d, wanted sth < %d.", oid, oldOid)
		}
		oldOid = oid
	}
	if n == 0 || n > num {
		t.Errorf("got %d rows, wanted %d", n, num)
	}
}

func TestExecInt64(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ExecInt64"), 10*time.Second)
	defer cancel()
	qry := `CREATE OR REPLACE PROCEDURE test_i64_out(p_int NUMBER, p_out1 OUT NUMBER, p_out2 OUT NUMBER) IS
	BEGIN p_out1 := p_int; p_out2 := p_int; END;`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(ctx, "DROP PROCEDURE test_i64_out")

	qry = "BEGIN test_i64_out(:1, :2, :3); END;"
	var num sql.NullInt64
	var str string
	defer tl.enableLogging(t)()
	if _, err := testDb.ExecContext(ctx, qry, 3.14, sql.Out{Dest: &num}, sql.Out{Dest: &str}); err != nil {
		t.Fatal(err)
	}
	t.Log("num:", num, "str:", str)
}

func TestImplicitResults(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ImplicitResults"), 10*time.Second)
	defer cancel()
	const qry = `declare
			c0 sys_refcursor;
            c1 sys_refcursor;
            c2 sys_refcursor;
        begin
            OPEN c0 FOR SELECT 0 FROM DUAL;
			:1 := c0;
            open c1 for select 1 from DUAL;
            dbms_sql.return_result(c1);
            open c2 for select 'A' from DUAL;
            dbms_sql.return_result(c2);
        end;`
	var rows driver.Rows
	if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: &rows}); err != nil {
		if strings.Contains(err.Error(), "PLS-00302:") {
			t.Skip()
		}
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	defer rows.Close()
	r := rows.(driver.RowsNextResultSet)
	for r.HasNextResultSet() {
		if err := r.NextResultSet(); err != nil {
			t.Error(err)
		}
	}
}

func TestStartupShutdown(t *testing.T) {
	ensureSystemDB(t)
	if os.Getenv("GODROR_DB_SHUTDOWN") != "1" {
		t.Skip("GODROR_DB_SHUTDOWN != 1, skipping shutdown/startup test")
	}
	p, err := godror.ParseDSN(testSystemConStr)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testSystemConStr, err))
	}
	if !(p.AdminRole == godror.SysDBA || p.AdminRole == godror.SysOPER) {
		p.AdminRole = godror.SysDBA
	}
	if !p.IsPrelim {
		p.IsPrelim = true
	}
	db := sql.OpenDB(godror.NewConnector(p))
	defer db.Close()
	ctx, cancel := context.WithTimeout(testContext("StartupShutdown"), time.Minute)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err, p.StringWithPassword())
	}

	if err = godror.Raw(ctx, db, func(conn godror.Conn) error {
		if err = conn.Shutdown(godror.ShutdownTransactionalLocal); err != nil {
			return err
		}
		return conn.Shutdown(godror.ShutdownFinal)
	}); err != nil {
		t.Errorf("SHUTDOWN: %+v", err)
	}
	db.Close()

	db = sql.OpenDB(godror.NewConnector(p))
	defer db.Close()
	if err = godror.Raw(ctx, db, func(conn godror.Conn) error {
		return conn.Startup(godror.StartupDefault)
	}); err != nil {
		t.Log("Couldn't start up database. run 'echo startup | sqlplus / as sysdba'")
		t.Errorf("STARTUP: %+v", err)
	}
}

func TestIssue134(t *testing.T) {
	cleanup := func() {
		for _, qry := range []string{
			`DROP TYPE test_prj_task_tt`,
			`DROP TYPE test_prj_task_ot`,
			`DROP PROCEDURE test_create_task_activity`,
		} {
			testDb.Exec(qry)
		}
	}
	cleanup()
	ctx, cancel := context.WithTimeout(testContext("Issue134"), 10*time.Second)
	defer cancel()
	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	for _, qry := range []string{
		`CREATE OR REPLACE TYPE test_PRJ_TASK_ot AS OBJECT (
	PROJECT_NUMBER VARCHAR2(100)
	,SOURCE_ID VARCHAR2(100)
	,TASK_NAME VARCHAR2(300)
	,TASK_DESCRIPTION VARCHAR2(2000)
	,TASK_START_DATE DATE
	,TASK_END_DATE DATE
	,TASK_COST NUMBER
	,SOURCE_PARENT_ID NUMBER
	,TASK_TYPE VARCHAR2(100)
	,QUANTITY NUMBER
	,data BLOB
)`,
		`CREATE OR REPLACE TYPE test_PRJ_TASK_tt IS TABLE OF test_PRJ_TASK_ot`,
		`CREATE OR REPLACE FUNCTION test_CREATE_TASK_ACTIVITY (
    p_create_task_i IN test_PRJ_TASK_tt,
	p_project_id_i IN NUMBER) RETURN test_prj_task_ot
IS
  v_obj test_prj_task_ot;
BEGIN
  IF p_create_task_i.COUNT = 0 THEN
    v_obj := test_prj_task_ot(
	PROJECT_NUMBER=>NULL
	,SOURCE_ID=>NULL
	,TASK_NAME=>NULL
	,TASK_DESCRIPTION=>NULL
	,TASK_START_DATE=>NULL
	,TASK_END_DATE=>NULL
	,TASK_COST=>NULL
	,SOURCE_PARENT_ID=>NULL
	,TASK_TYPE=>NULL
	,QUANTITY=>NULL
	,data=>NULL);
  ELSE
    v_obj := p_create_task_i(p_create_task_i.FIRST);
  END IF;
  DBMS_LOB.createtemporary(v_obj.data, TRUE, 31);
  DBMS_LOB.writeAppend(v_obj.data, 10,
    UTL_RAW.cast_to_raw(TO_CHAR(SYSDATE, 'YYYY-MM-DD')));
  RETURN(v_obj);
END;`,
	} {
		if _, err := cx.ExecContext(ctx, qry); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer cleanup()
	compileErrors, err := godror.GetCompileErrors(ctx, cx, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, ce := range compileErrors {
		t.Error(ce)
	}
	if len(compileErrors) != 0 {
		t.Fatal("compile failed")
	}

	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ot, err := conn.GetObjectType("TEST_PRJ_TASK_tt")
	if err != nil {
		t.Fatal(err)
	}
	obj, err := ot.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	t.Logf("obj=%#v", obj)
	rt, err := conn.GetObjectType("TEST_PRJ_TASK_ot")
	if err != nil {
		t.Fatal(err)
	}
	ret, err := rt.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer ret.Close()

	qry := "BEGIN :1 := test_create_task_activity(:2, :3); END;"
	if err := prepExec(ctx, conn, qry,
		driver.NamedValue{Value: &ret, Ordinal: 1},
		driver.NamedValue{Value: &obj, Ordinal: 2},
		driver.NamedValue{Value: 1, Ordinal: 3},
	); err != nil {
		t.Fatalf("%s [%#v, 1]: %+v", qry, obj, err)
	}
	t.Logf("ret: %#v", ret)
	var data godror.Data
	if err := ret.GetAttribute(&data, "DATA"); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(data.GetLob()); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("DATA: %q", b)
	}
}

func TestDateRset(t *testing.T) {
	defer tl.enableLogging(t)()
	ctx := testContext("DateRset")

	const qry = `DECLARE
  v_cur SYS_REFCURSOR;
BEGIN
  OPEN v_cur FOR
    SELECT TO_DATE('2015/05/15 8:30:25', 'YYYY/MM/DD HH:MI:SS') as DD FROM DUAL
    UNION ALL SELECT TO_DATE('2015/05/15 8:30:25', 'YYYY/MM/DD HH:MI:SS') as DD FROM DUAL;
  :1 := v_cur;
END;`

	for i := 0; i < maxSessions/2; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			stmt, err := testDb.PrepareContext(ctx, qry)
			if err != nil {
				t.Fatalf("%s: %+v", qry, err)
			}
			defer stmt.Close()

			var rows1 driver.Rows
			if _, err := stmt.ExecContext(ctx, sql.Out{Dest: &rows1}); err != nil {
				t.Fatalf("%s: %+v", qry, err)

			}
			defer rows1.Close()
			cols1 := rows1.Columns()
			values := make([]driver.Value, len(cols1))

			var rowNum int
			for {
				rowNum++
				if err := rows1.Next(values); err != nil {
					if err == io.EOF {
						break
					}
				}
				t.Logf("%[1]d. %[2]T %[2]v", rowNum, values[0])
			}
		})
	}
}

func TestTsRset(t *testing.T) {
	defer tl.enableLogging(t)()
	ctx := testContext("TsRset")

	const qry = `DECLARE
  v_cur SYS_REFCURSOR;
BEGIN
  OPEN v_cur FOR
	SELECT TO_TIMESTAMP_TZ('2019-05-01 09:39:12 01:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') FROM DUAL
	UNION ALL SELECT FROM_TZ(TO_TIMESTAMP('2019-05-01 09:39:12', 'YYYY-MM-DD HH24:MI:SS'), '01:00') FROM DUAL;
  :1 := v_cur;
END;`

	for i := 0; i < maxSessions/2; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			var rows1 driver.Rows
			if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: &rows1}); err != nil {
				t.Fatalf("%s: %+v", qry, err)

			}
			defer rows1.Close()
			cols1 := rows1.Columns()
			values := make([]driver.Value, len(cols1))

			var rowNum int
			for {
				rowNum++
				if err := rows1.Next(values); err != nil {
					if err == io.EOF {
						break
					}
				}
				t.Logf("%[1]d. %[2]T %[2]v", rowNum, values[0])
			}
		})
	}
}

func TestTsTZ(t *testing.T) {
	t.Parallel()
	fields := []string{
		"FROM_TZ(TO_TIMESTAMP('2019-05-01 09:39:12', 'YYYY-MM-DD HH24:MI:SS'), '{{.TZ}}')",
		"TO_TIMESTAMP_TZ('2019-05-01 09:39:12 {{.TZ}}', 'YYYY-MM-DD HH24:MI:SS {{.TZDec}}')",
		"CAST(TO_TIMESTAMP_TZ('2019-05-01 09:39:12 {{.TZ}}', 'YYYY-MM-DD HH24:MI:SS {{.TZDec}}') AS DATE)",
	}
	ctx, cancel := context.WithTimeout(testContext("TsTZ"), 10*time.Second)
	defer cancel()

	defer tl.enableLogging(t)()

	for _, Case := range []struct {
		TZ, TZDec string
	}{
		{"00:00", "TZH:TZM"},
		{"01:00", "TZH:TZM"},
		{"-01:00", "TZH:TZM"},
		{"Europe/Berlin", "TZR"},
	} {
		repl := strings.NewReplacer("{{.TZ}}", Case.TZ, "{{.TZDec}}", Case.TZDec)
		for i, f := range fields {
			f = repl.Replace(f)
			qry := "SELECT DUMP(" + f + ") FROM DUAL"
			var s string
			if err := testDb.QueryRowContext(ctx, qry).Scan(&s); err != nil {
				if Case.TZDec != "TZR" {
					t.Fatalf("%s: %s: %+v", Case.TZ, qry, err)
				}
				t.Logf("%s: %s: %+v", Case.TZ, qry, err)
			}
			t.Logf("%s: DUMP[%d]: %q", Case.TZ, i, s)

			qry = "SELECT " + f + " FROM DUAL"
			var ts time.Time
			if err := testDb.QueryRowContext(ctx, qry).Scan(&ts); err != nil {
				var oerr *godror.OraErr
				if errors.As(err, &oerr) && oerr.Code() == 1805 {
					t.Skipf("%s: %s: %+v", Case.TZ, qry, err)
					continue
				}
				t.Fatalf("%s: %s: %+v", Case.TZ, qry, err)
			}
			t.Logf("%s: %d: %s", Case.TZ, i, ts)
		}
	}

	qry := "SELECT filename, version FROM v$timezone_file"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Log(qry, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var fn, ver string
		if err := rows.Scan(&fn, &ver); err != nil {
			t.Log(qry, err)
			continue
		}
		t.Log(fn, ver)
	}
	t.Skip("wanted non-zero time")
}

func TestGetDBTimezone(t *testing.T) {
	t.Parallel()
	defer tl.enableLogging(t)()

	ctx, cancel := context.WithTimeout(testContext("GetDBTimeZone"), 10*time.Second)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	qry := `SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD"T"HH24:MI:SS'), TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD"T"hH24:MI:SS'), DBTIMEZONE, SESSIONTIMEZONE, TO_CHAR(SYSTIMESTAMP, 'TZR') AS dbOSTZ, SYSTIMESTAMP FROM DUAL`
	var sysdate, currentDate, dbTz, tzr, sts, dbOSTZ, ts string
	if err := tx.QueryRowContext(ctx, qry).Scan(&sysdate, &currentDate, &dbTz, &tzr, &dbOSTZ, &ts); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Logf("sysdate=%q currentDate=%q dbTZ=%q TZR=%q dbOSTZ=%q ts=%q", sysdate, currentDate, dbTz, tzr, dbOSTZ, ts)

	qry = "ALTER SESSION SET time_zone = 'UTC'"
	if _, err := tx.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	qry = `SELECT DBTIMEZONE, SESSIONTIMEZONE, TO_CHAR(SYSTIMESTAMP, '"TZR="TZR "TZD="TZD "TZH:TZM="TZH:TZM'), LOCALTIMESTAMP||'' FROM DUAL`
	var tz, lts string
	if err := tx.QueryRowContext(ctx, qry).Scan(&dbTz, &tz, &sts, &lts); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Log("db timezone:", dbTz, "session timezone:", tz, "systimestamp:", sts, "localtimestamp:", lts)

	today := time.Now().Truncate(time.Second)
	for i, tim := range []time.Time{today, today.AddDate(0, 6, 0)} {
		t.Log("local:", tim.Format(time.RFC3339))

		qry = "SELECT :1 FROM DUAL"
		var dbTime time.Time
		if err := tx.QueryRowContext(ctx, qry, tim).Scan(&dbTime); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		t.Log("db:", dbTime.Format(time.RFC3339))
		if !dbTime.Equal(tim) {
			msg := fmt.Sprintf("db says %s, local is %s", dbTime.Format(time.RFC3339), tim.Format(time.RFC3339))
			if i == 0 {
				t.Error("ERROR:", msg)
			} else {
				t.Log(msg)
			}
		}
	}
}

func TestConnParamsTimezone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ConnParamsTZ"), 30*time.Second)
	defer cancel()

	getServerTZ := func(db *sql.DB) *time.Location {
		var serverTZ *time.Location
		if err := godror.Raw(ctx, db, func(cx godror.Conn) error {
			serverTZ = cx.Timezone()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return serverTZ
	}

	orig := getServerTZ(testDb)
	_, off := time.Now().In(orig).Zone()
	diff := 7200
	if off > 12*3600-diff {
		diff = -diff
	}
	off += diff

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	want := time.FixedZone(fmt.Sprintf("%d=%s%+d", off, orig, diff), off)
	P.Timezone = want
	t.Logf("Set Timezone from %s to %s", orig, want)
	db := sql.OpenDB(godror.NewConnector(P))
	got := getServerTZ(db)
	if got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}
}

func TestNumberAsStringBool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("NumberBool"), 3*time.Second)
	defer cancel()
	const qry = "SELECT 181 id, 1 status FROM DUAL"
	rows, err := testDb.QueryContext(ctx, qry, godror.NumberAsString())
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	for rows.Next() {
		var id int
		var status bool
		if err := rows.Scan(&id, &status); err != nil {
			t.Errorf("failed to scan data: %s\n", err)
		}
		t.Logf("Source id=%d, status=%t\n", id, status)
		if !status {
			t.Errorf("wanted true, got %t", status)
		}
	}
}

func TestDST(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("DST"), 10*time.Minute)
	defer cancel()

	defer tl.enableLogging(t)()
	t.Parallel()

	start := time.Date(1950, 1, 1, 0, 0, 0, 0, time.Local)
	now := time.Now()
	maxNum := (365*4 + 1) * (now.Year() - start.Year() + 1) / 4
	qry := `SELECT dt, TO_CHAR(dt, 'YYYY-MM-DD') as tx FROM (
		SELECT TO_DATE('` + start.Format("2006-01-02") + `', 'YYYY-MM-DD')+rn AS dt
			FROM (SELECT ROWNUM AS rn FROM DUAL CONNECT BY LEVEL <= ` + strconv.Itoa(maxNum) + "))"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	var stmt *sql.Stmt
	{
		const qry = `SELECT TO_CHAR(:1, 'YYYY-MM-DD HH24:MI:SS') FROM DUAL`
		if stmt, err = testDb.PrepareContext(ctx, qry); err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
	}

	var want string
	first := true
	for rows.Next() {
		var tim time.Time
		if err := rows.Scan(&tim, &want); err != nil {
			t.Fatalf("scan %s: %+v", qry, err)
		}
		if tim.Location() == time.UTC {
			t.Skip("db is UTC")
		}
		// t.Log("tim:", tim, "want:", want)
		if got := tim.Format("2006-01-02 15:04:05"); !strings.HasSuffix(got, " 00:00:00") {
			t.Logf("got %s, wanted %s", tim.Format(time.RFC3339), want)
			var s string
			if err := stmt.QueryRowContext(ctx, tim).Scan(&s); err != nil {
				t.Fatalf("scan [%v]: %+v", tim, err)
			}
			if s != want+" 00:00:00" {
				t.Errorf("wanted %q, got %q", want, s)
			}
		}
		if first {
			t.Log("first:", want)
			first = false
		}
	}
	t.Log("last:", want)
}

func TestNumberBool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("NumberBool"), 3*time.Second)
	defer cancel()
	const qry = "SELECT 181 id, '1' status FROM DUAL"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	for rows.Next() {
		var id int
		var status bool
		if err := rows.Scan(&id, &status); err != nil {
			t.Errorf("failed to scan data: %s\n", err)
		}
		t.Logf("Source id=%d, status=%t\n", id, status)
		if !status {
			t.Errorf("wanted true, got %t", status)
		}
	}
}

func TestCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Cancel"), 15*time.Second)
	defer cancel()
	subCtx, subCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		const qry = `BEGIN DBMS_SESSION.sleep(10); END;`
		if _, err := testDb.ExecContext(subCtx, qry); err != nil {
			done <- err
		}
	}()
	time.Sleep(time.Second)
	subCancel()
	time.Sleep(time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	default:
		t.Error("hasn't finished yet")
	}
}

func TestTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skip cancel test")
	}
	db, err := sql.Open("godror", testConStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pid := os.Getpid()
	maxConc := maxSessions / 2
	db.SetMaxOpenConns(maxConc - 1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithCancel(testContext("Timeout"))
	defer cancel()
	const qryCount = "SELECT COUNT(0) FROM v$session WHERE username = USER AND process = TO_CHAR(:1)"
	cntStmt, err := testDb.PrepareContext(ctx, qryCount)
	if err != nil {
		t.Fatalf("%q: %v", qryCount, err)
	}
	// Just a test, as Prepared statements does not work
	cntStmt.Close()
	Cnt := func() int {
		var cnt int
		if err := db.QueryRowContext(ctx, qryCount, pid).Scan(&cnt); err != nil {
			if !strings.Contains(err.Error(), "ORA-00942:") {
				t.Fatal(fmt.Errorf("%s: %w", qryCount, err))
			}
		}
		return cnt
	}

	t.Log("Pid:", pid)
	goal := Cnt() + 1
	t.Logf("Before: %d", goal)
	const qry = "BEGIN FOR rows IN (SELECT 1 FROM DUAL) LOOP DBMS_SESSION.SLEEP(10); END LOOP; END;"
	subCtx, subCancel := context.WithTimeout(ctx, time.Duration(2*maxConc+1)*time.Second)
	grp, grpCtx := errgroup.WithContext(subCtx)
	for range maxConc {
		grp.Go(func() error {
			if _, err := db.ExecContext(grpCtx, qry); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("%s: %w", qry, err)
			}
			return nil
		})
	}
	time.Sleep(time.Second)
	t.Logf("After exec, before cancel: %d", Cnt())
	subCancel()
	t.Logf("After cancel: %d", Cnt())
	if err = grp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	t.Logf("After finish: %d; goal: %d", Cnt(), goal)

	for i := 0; i < 2*maxConc; i++ {
		cnt := Cnt()
		t.Logf("After %ds: %d", i+1, cnt)
		if cnt <= goal {
			return
		}
		time.Sleep(time.Second)
	}
	t.Skip("cancelation timed out")
}

func TestObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Object"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer testCon.Close()
	if err = testCon.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("dbstats: %#v", testDb.Stats())

	cleanup := func() {
		testDb.Exec("DROP PROCEDURE test_obj_modify")
		testDb.Exec("DROP TYPE test_obj_tab_t")
		testDb.Exec("DROP TYPE test_obj_rec_t")
	}
	cleanup()
	const crea = `
CREATE OR REPLACE TYPE test_obj_rec_t AS OBJECT (num NUMBER, vc VARCHAR2(1000), dt DATE);
CREATE OR REPLACE TYPE test_obj_tab_t AS TABLE OF test_obj_rec_t;
CREATE OR REPLACE PROCEDURE test_obj_modify(p_obj IN OUT NOCOPY test_obj_tab_t) IS
BEGIN
  p_obj.EXTEND;
  p_obj(p_obj.LAST) := test_obj_rec_t(
    num => 314/100 + p_obj.COUNT,
    vc  => 'abraka dabra',
    dt  => SYSDATE);
END;`
	for _, qry := range strings.Split(crea, "\nCREATE OR") {
		if qry == "" {
			continue
		}
		qry = "CREATE OR" + qry
		if _, err = testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}

	defer cleanup()

	cOt, err := testCon.GetObjectType(strings.ToUpper("test_obj_tab_t"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cOt)

	// create object from the type
	coll, err := cOt.NewCollection()
	if err != nil {
		t.Fatal("NewCollection:", err)
	}
	defer coll.Close()

	// create an element object
	elt, err := cOt.CollectionOf.NewObject()
	if err != nil {
		t.Fatal("collection.NewObject:", err)
	}
	defer elt.Close()

	// append to the collection
	t.Logf("append an empty %s", elt)
	coll.AppendObject(elt)

	const mod = "BEGIN test_obj_modify(:1); END;"
	if err = prepExec(ctx, testCon, mod, driver.NamedValue{Ordinal: 1, Value: coll}); err != nil {
		t.Error(err)
	}
	t.Logf("coll: %s", coll)
	var data godror.Data
	for i, err := coll.First(); err == nil; i, err = coll.Next(i) {
		if err = coll.GetItem(&data, i); err != nil {
			t.Fatal(err)
		}
		elt = data.GetObject()

		t.Logf("elt[%d] : %s", i, elt)
		for attr := range elt.Attributes {
			val, err := elt.Get(attr)
			if err != nil {
				t.Error(err, attr)
			}
			t.Logf("elt[%d].%s=%s", i, attr, val)
		}
	}
}

func TestNewPassword(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(testContext("NewPassword"), 10*time.Second)
	defer cancel()
	const user, oldPassword, newPassword = "test_expired", "oldPassw0rd_long", "newPassw0rd_longer"

	testDb.Exec("DROP USER " + user)
	// GRANT CREATE USER, DROP USER TO test
	// GRANT CREATE SESSION TO test WITH ADMIN OPTION
	qry := "CREATE USER " + user + " IDENTIFIED BY " + oldPassword + " PASSWORD EXPIRE"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		if strings.Contains(err.Error(), "ORA-01031:") {
			t.Log("Please issue this:\nGRANT CREATE USER, DROP USER TO " + P.Username + ";\n" +
				"GRANT CREATE SESSION TO " + P.Username + " WITH ADMIN OPTION;\n")
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer testDb.Exec("DROP USER " + user)

	qry = "GRANT CREATE SESSION TO " + user
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		if strings.Contains(err.Error(), "ORA-01031:") {
			t.Log("Please issue this:\nGRANT CREATE SESSION TO " + P.Username + " WITH ADMIN OPTION;\n")
		}
		t.Fatal(err)
	}

	P.Username, P.Password = user, godror.NewPassword(oldPassword)
	P.StandaloneConnection = godror.Bool(true)
	P.NewPassword = godror.NewPassword(newPassword)
	{
		db, err := sql.Open("godror", P.StringWithPassword())
		if err != nil {
			t.Fatal(err)
		}
		err = db.Ping()
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	P.Password = P.NewPassword
	P.NewPassword.Reset()
	{
		db, err := sql.Open("godror", P.StringWithPassword())
		if err != nil {
			t.Fatal(err)
		}
		err = db.Ping()
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnClass(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(testContext("ConnClass"), 10*time.Second)
	defer cancel()

	const connClass = "fc8153b840"
	P.ConnClass = connClass

	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Skip(err)
	}

	const qry = "SELECT username,program,cclass_name FROM v$cpool_conn_info"
	rows, err := db.QueryContext(ctx, qry)
	if err != nil {
		var oerr *godror.OraErr
		if errors.As(err, &oerr) && oerr.Code() == 942 {
			t.Skip(err)
		}
		t.Fatalf("%s: %+v", qry, err)
	}
	defer rows.Close()

	for rows.Next() {
		var usr, prg, class string
		if err = rows.Scan(&usr, &prg, &class); err != nil {
			t.Fatal(err)
		}
		t.Log(usr, prg, class)
	}
}

func TestOnInit(t *testing.T) {
	t.Parallel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	const numChars = "#@"
	P.SetSessionParamOnInit("nls_numeric_characters", numChars)
	t.Log(P.String())
	ctx, cancel := context.WithTimeout(testContext("OnInit"), 10*time.Second)
	defer cancel()

	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conns := make([]*sql.Conn, 3)
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()
	for i := range conns {
		if conns[i], err = db.Conn(ctx); err != nil {
			t.Fatal(err)
		}

		const qry = "SELECT value, TO_CHAR(123/10) AS num FROM v$nls_parameters WHERE parameter = 'NLS_NUMERIC_CHARACTERS'"
		var v, n string
		if err = conns[i].QueryRowContext(ctx, qry).Scan(&v, &n); err != nil {
			t.Errorf("%d. %+v", i, err)
		}
		t.Logf("%d. v=%q n=%q", i, v, n)
		if v != numChars {
			t.Errorf("%d. got %q wanted %q", i, v, numChars)
		}
		if n != "12#3" {
			t.Errorf("%d. got %q wanted 12#3", i, n)
		}
	}
}

func TestOnInitParallel(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	const numChars = "#@"
	P.SetSessionParamOnInit("nls_numeric_characters", numChars)
	t.Log(P.String())
	ctx, cancel := context.WithTimeout(testContext("OnInit"), 1*time.Minute)
	defer cancel()

	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const want = "12#3"
	const qry = "SELECT value, TO_CHAR(123/10) AS num FROM v$nls_parameters WHERE parameter = 'NLS_NUMERIC_CHARACTERS'"
	for i := range 4 {
		var wg sync.WaitGroup
		for j := 0; j < maxSessions*2; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				var v, n string
				if err := db.QueryRowContext(ctx, qry).Scan(&v, &n); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					var ec interface{ Code() int }
					if errors.As(err, &ec) && (ec.Code() == 28547 || ec.Code() == 18) {
						return
					}
					t.Errorf("%d*%d. %+v", i, j, err)
					return
				}
				//t.Logf("%d. v=%q n=%q", i, v, n)
				if v != numChars || n != want {
					t.Errorf("%d*%d. got %q(%q) wanted %q(%q)", i, j, v, n, numChars, want)
					cancel()
				}
			}(i, j)
		}
		wg.Wait()
	}
}

func TestSelectTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("SelectTypes"), time.Minute)
	defer cancel()
	const createQry = `CREATE TABLE TEST_TYPES (
			A_LOB BFILE,
			B BINARY_DOUBLE,
			C BINARY_FLOAT,
			D_LOB BLOB,
			E CHAR(10),
			F_LOB CLOB,
			G DATE,
			GN DATE,
			H NUMBER(18 , 2),
			I FLOAT(126),
			J FLOAT(10),
			K NUMBER(38 , 0),
			L INTERVAL DAY TO SECOND(6),
			M INTERVAL YEAR TO MONTH,
			--N LONG,
			P NCHAR(100),
			Q_LOB NCLOB,
			R NUMBER(18 , 2),
			S NUMBER(18 , 2),
			T NVARCHAR2(100),
			U RAW(100),
			V FLOAT(63),
			W NUMBER(38 , 0),
			X TIMESTAMP,
			Y TIMESTAMP WITH LOCAL TIME ZONE,
			Z TIMESTAMP WITH TIME ZONE,
			AA VARCHAR2(100),
			AB XMLTYPE
		)`
	testDb.ExecContext(ctx, "DROP TABLE test_types")
	if _, err := testDb.ExecContext(ctx, createQry); err != nil {
		t.Fatalf("%s: %+v", createQry, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(testContext("SelectTypes-drop"), 10*time.Second)
		defer cancel()
		testDb.ExecContext(ctx, "DROP TABLE test_types")
	}()

	const insertQry = `INSERT INTO test_types
  (b, c, e, g, gn,
   h, i, j,
   k, l,
   r, s, u,
   v, w, x, y,
   z,
   aa)
  VALUES (3.14, 4.15, 'char(10)', TO_DATE('2020-01-21 09:16:36', 'YYYY-MM-DD HH24:MI:SS'), NULL,
          1/3, 5.16, 6.17,
          123456789012345678901234567890, INTERVAL '8' HOUR,
		  7.18, 8.19, HEXTORAW('deadbeef'),
		  0.01, -3, SYSTIMESTAMP, SYSTIMESTAMP,
		  TIMESTAMP '2018-02-15 14:00:00.00 CET',
          'varchar2(100)')`

	if _, err := testDb.ExecContext(ctx, insertQry); err != nil {
		t.Fatalf("%s: %+v", insertQry, err)
	}
	const insertQry2 = `INSERT INTO test_types (z) VALUES (cast(TO_TIMESTAMP_TZ('2018-02-15T14:00:00 01:00','yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') as date))`
	if _, err := testDb.ExecContext(ctx, insertQry2); err != nil {
		t.Fatalf("%s: %+v", insertQry2, err)
	}
	var n int32
	if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM test_types").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("%d rows in the table, wanted 2", n)
	}

	var dbTZ string
	if err := testDb.QueryRowContext(ctx, "SELECT dbtimezone FROM DUAL").Scan(&dbTZ); err != nil {
		t.Fatal(err)
	}
	rows, err := testDb.QueryContext(ctx, "SELECT filename, version FROM v$timezone_file")
	if err != nil {
		t.Log(err)
	} else {
		for rows.Next() {
			var fn, ver string
			if err = rows.Scan(&fn, &ver); err != nil {
				t.Fatal(err)
			}
			t.Logf("v$timezone file=%q version=%q", fn, ver)
		}
		rows.Close()
	}

	const qry = "SELECT * FROM test_types"

	// get rows
	rows, err = testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer rows.Close()

	// get columns name
	colsName, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("columns:", colsName)

	// get types of query columns
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	// total columns
	totalColumns := len(colsName)

	oracleFieldParse := func(datatype string, field any) any {
		// DEBUG: print the type of field and datatype returned for the drivers
		t.Logf("%T\t%s\n", field, datatype)

		if field == nil {
			return nil
		}
		switch x := field.(type) {
		case string:
			return x
		case godror.Number:
			return string(x)
		case []uint8:
			switch datatype {
			case "RAW", "LONG RAW":
				return fmt.Sprintf("%x", x)
			default:
				return fmt.Sprintf("unsupported datatype %s", datatype)
			}
		case godror.NullTime:
			if !x.Valid {
				return "NULL"
			}
			if x.Time.IsZero() {
				t.Errorf("zero NullTime.Time, and Valid!?")
			}
			return x.Time.Format(time.RFC3339)
		case time.Time:
			return x.Format(time.RFC3339)
		default:
			return fmt.Sprintf("%v", field)
		}
	}

	// create a slice of any's to represent each column,
	// and a second slice to contain pointers to each item in the columns slice
	columns := make([]any, totalColumns)
	recordPointers := make([]any, totalColumns)
	for i := range columns {
		if types[i].DatabaseTypeName() == "DATE" {
			var t godror.NullTime
			recordPointers[i] = &t
			columns[i] = t
		} else {
			recordPointers[i] = &columns[i]
		}
	}

	dumpRows := func() {
		dests := make([]string, 0, len(colsName))
		params := make([]any, 0, cap(dests))
		var dumpQry strings.Builder
		dumpQry.WriteString("SELECT ")
		for _, col := range colsName {
			if strings.HasSuffix(col, "_LOB") {
				continue
			}
			if len(dests) != 0 {
				dumpQry.WriteString(", ")
			}
			fmt.Fprintf(&dumpQry, "'%[1]s='||DUMP(%[1]s, 1017,1,10) AS %[1]s", col)
			dests = append(dests, "")
			params = append(params, &dests[len(dests)-1])
		}
		dumpQry.WriteString(" FROM test_types")
		qry := dumpQry.String()
		rows, err := testDb.QueryContext(ctx, qry)
		if err != nil {
			t.Errorf("%s: %+v", qry, err)
			return
		}
		defer rows.Close()
		var i int
		for rows.Next() {
			if err = rows.Scan(params...); err != nil {
				t.Fatal(err)
			}
			i++
			t.Logf("%d. %q", i, dests)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
	}

	// record destination
	record := make([]any, totalColumns)
	var rowCount int
	for rows.Next() {
		// Scan the result into the record pointers...
		if err = rows.Scan(recordPointers...); err != nil {
			dumpRows()
			t.Fatal(err)
		}
		rowCount++

		// Parse each field of recordPointers for get a custom field depending the type
		for idxCol := range recordPointers {
			record[idxCol] = oracleFieldParse(types[idxCol].DatabaseTypeName(), reflect.ValueOf(recordPointers[idxCol]).Elem().Interface())
		}

		t.Log(record)
	}
	if err = rows.Err(); err != nil {
		var cErr interface{ Code() int }
		if errors.As(err, &cErr) && cErr.Code() == 1805 {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	dumpRows()
	if rowCount != 2 {
		t.Errorf("read %d rows, wanted 2", rowCount)
	}
}

func TestInsertIntervalDS(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("InsertIntervalDS"), 10*time.Second)
	defer cancel()
	const tbl = "test_interval_ds"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (F_interval_ds INTERVAL DAY TO SECOND(3))"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer func() { testDb.ExecContext(testContext("InsertIntervalDS-drop"), "DROP TABLE "+tbl) }()

	qry = "INSERT INTO " + tbl + " (F_interval_ds) VALUES (INTERVAL '32' SECOND)"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	qry = "INSERT INTO " + tbl + " (F_interval_ds) VALUES (:1)"
	if _, err := testDb.ExecContext(ctx, qry, 33*time.Second); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	qry = "SELECT F_interval_ds FROM " + tbl + " ORDER BY 1"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	var got []time.Duration
	for rows.Next() {
		var dur time.Duration
		if err = rows.Scan(&dur); err != nil {
			t.Fatal(err)
		}
		got = append(got, dur)
	}
	t.Log("got:", got)
	if !(len(got) == 2 && got[0] == 32*time.Second && got[1] == 33*time.Second) {
		t.Errorf("wanted [32s, 33s], got %v", got)
	}
}
func TestBool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("Bool"), 10*time.Second)
	defer cancel()
	const tbl = "test_bool_t"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (F_bool VARCHAR2(1))"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer func() { testDb.ExecContext(testContext("Bool-drop"), "DROP TABLE "+tbl) }()

	qry = "INSERT INTO " + tbl + " (F_bool) VALUES ('Y')"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	qry = "INSERT INTO " + tbl + " (F_bool) VALUES (:1)"
	if _, err := testDb.ExecContext(ctx, qry, booler(true)); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	b2s := godror.BoolToString("t", "f")
	if _, err := testDb.ExecContext(ctx, qry, true, b2s); err != nil {
		t.Fatal(err)
	}
	if _, err := testDb.ExecContext(ctx, qry, false, b2s); err != nil {
		t.Fatal(err)
	}

	qry = "SELECT F_bool, F_bool FROM " + tbl + " A ORDER BY ASCII(A.F_bool)"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	var got []bool
	for rows.Next() {
		var b booler
		var s string
		if err = rows.Scan(&s, &b); err != nil {
			t.Fatal(err)
		}
		t.Logf("%q: %v", s, b)
		got = append(got, bool(b))
	}
	t.Log("got:", got)
	want := []bool{true, true, false, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wanted %v got %v", want, got)
	}
}

type booler bool

func (b *booler) Scan(src any) error {
	switch x := src.(type) {
	case int:
		*b = x == 1
	case string:
		*b = x == "Y" || x == "t"
	default:
		return fmt.Errorf("unknown scanner source %T", src)
	}
	return nil
}
func (b booler) Value() (driver.Value, error) {
	if b {
		return "Y", nil
	}
	return "N", nil
}

func TestBoolValueTypes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("NullBool"), 10*time.Second)
	defer cancel()
	const tbl = "test_bool_value_types_t"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (F_bool NUMBER(1))"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer func() { testDb.ExecContext(testContext("Bool-value-types-drop"), "DROP TABLE "+tbl) }()

	tests := []struct {
		name   string
		obj    interface{}
		expect interface{}
	}{
		{
			name:   "null with nil bool ptr",
			obj:    (*bool)(nil),
			expect: nil,
		},
		{
			name:   "null with nil",
			obj:    nil,
			expect: nil,
		},
		{
			name:   "true",
			obj:    true,
			expect: int64(1),
		},
		{
			name:   "true with bool ptr",
			obj:    func() *bool { b := true; return &b }(),
			expect: int64(1),
		},
		{
			name:   "false",
			obj:    false,
			expect: int64(0),
		},
		{
			name:   "false with bool ptr",
			obj:    func() *bool { b := false; return &b }(),
			expect: int64(0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qry = "INSERT INTO " + tbl + " (F_bool) VALUES (:1)"
			if _, err := testDb.ExecContext(ctx, qry, test.obj); err != nil {
				t.Fatal(fmt.Errorf("%s: %w", qry, err))
			}
			qry = "SELECT F_bool FROM " + tbl
			var result interface{}
			if err := testDb.QueryRowContext(ctx, qry).Scan(&result); err != nil {
				t.Fatal(fmt.Errorf("%s: %w", qry, err))
			}
			if !reflect.DeepEqual(test.expect, result) {
				t.Errorf("wanted %+v, got %+v", test.expect, result)
			}
			// Clean up
			if _, err := testDb.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResetSession(t *testing.T) {
	const poolSize = 4
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.MinSessions, P.SessionIncrement, P.MaxSessions = poolSize, 1, poolSize
	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatalf("%s: %+v", P, err)
	}
	defer db.Close()
	db.SetMaxIdleConns(poolSize)

	ctx, cancel := context.WithTimeout(testContext("ResetSession"), time.Minute)
	defer cancel()
	for i := range 2 * poolSize {
		shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := db.Conn(shortCtx)
		if err != nil {
			shortCancel()
			t.Fatalf("%d. Conn: %+v", i, err)
		}
		err = conn.PingContext(shortCtx)
		shortCancel()
		if err != nil {
			t.Fatalf("%d. Ping: %+v", i, err)
		}
		conn.Close()
	}
}

func TestSelectNullTime(t *testing.T) {
	t.Parallel()
	const qry = "SELECT SYSDATE, SYSDATE+NULL, SYSDATE+NULL FROM DUAL"
	var t0, t1 time.Time
	var nt sql.NullTime
	ctx, cancel := context.WithTimeout(testContext("SelectNullTime"), 30*time.Second)
	err := testDb.QueryRowContext(ctx, qry, godror.NullDateAsZeroTime()).Scan(&t0, &t1, &nt)
	cancel()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	t.Logf("t0=%s t1=%s nt=%v", t0, t1, nt)
}
func TestSelectROWID(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ROWID"), 10*time.Second)
	defer cancel()
	const tbl = "test_rowid_t"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (F_seq NUMBER(6))"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer func() { testDb.ExecContext(testContext("ROWID-drop"), "DROP TABLE "+tbl) }()

	qry = "INSERT INTO " + tbl + " (F_seq) VALUES (:1)"
	for i := range 10 {
		if _, err := testDb.ExecContext(ctx, qry, i); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}
	qry = "SELECT F_seq, ROWID FROM " + tbl + " ORDER BY F_seq"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	for rows.Next() {
		var i int
		var rowid string
		if err = rows.Scan(&i, &rowid); err != nil {
			t.Fatalf("scan: %+v", err)
		}
		t.Logf("%d. %v", i, rowid)
		if len(rowid) != 18 {
			t.Errorf("%d. got %v (%d bytes), wanted sth 18 bytes", i, rowid, len(rowid))
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCloseLOB(t *testing.T) {
	t.Parallel()
	const poolSize = 2
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.WaitTimeout = 5 * time.Second
	P.MinSessions, P.SessionIncrement, P.MaxSessions = poolSize, 1, poolSize
	t.Log(P.String())
	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatalf("%s: %+v", P, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(poolSize)
	if P.StandaloneConnection.Valid && P.StandaloneConnection.Bool {
		db.SetMaxIdleConns(poolSize)
	} else {
		db.SetMaxIdleConns(0)
	}

	ctx, cancel := context.WithTimeout(testContext("OpenCloseLob"), time.Minute)
	defer cancel()

	const qry = "DECLARE v_lob BLOB; BEGIN DBMS_LOB.CREATETEMPORARY(v_lob, TRUE); DBMS_LOB.WRITEAPPEND(v_lob, 4, HEXTORAW('DEADBEEF')); :1 := v_lob; END;"
	const shortTimeout = 5 * time.Second
	var breakLoop int32
	for i := 0; i < 10*poolSize && atomic.LoadInt32(&breakLoop) != 0; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			shortCtx, shortCancel := context.WithTimeout(ctx, shortTimeout)
			defer shortCancel()
			tx, err := db.BeginTx(shortCtx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			stmt, err := tx.PrepareContext(shortCtx, qry)
			if err != nil {
				t.Fatalf("%s: %v", qry, err)
			}
			defer stmt.Close()
			var lob godror.Lob
			if _, err = stmt.ExecContext(shortCtx, sql.Out{Dest: &lob}); err != nil {
				var ec interface{ Code() int }
				if errors.As(err, &ec) && (ec.Code() == 3146) {
					t.Log(err)
					return
				}
				t.Error(err)
				return
			}
			b, err := io.ReadAll(lob)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("0x%x", b)
		})
	}
}

var ensureSystemDBOnce sync.Once

func ensureSystemDB(t *testing.T) {
	ensureSystemDBOnce.Do(func() {
		if testSystemDb != nil {
			return
		}
		if testSystemConStr == "" {
			return
		}
		var err error
		t.Log("SYSTEM:", testSystemConStr)
		if testSystemDb, err = sql.Open("godror", testSystemConStr); err != nil {
			panic(fmt.Errorf("%s: %+v", testConStr, err))
		}
	})
	if testSystemDb == nil {
		t.Skip("Please define GODROR_TEST_SYSTEM_USERNAME and GODROR_TEST_SYSTEM_PASSWORD env variables")
	}
}

func TestSystem(t *testing.T) {
	ensureSystemDB(t)

	ctx, cancel := context.WithTimeout(testContext("TestPreFetchQuery"), 30*time.Second)
	defer cancel()

	// Create a table used for Prefetch, ArrayFetch queries

	tbl := "t_employees" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (employee_id NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	const num = 250 // 250 rows to be created
	nums := make([]godror.Number, num)
	for i := range nums {
		nums[i] = godror.Number(strconv.Itoa(i))
	}

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i, tc := range []struct {
		Value any
		Name  string
	}{
		{Name: "employee_id", Value: nums},
	} {
		res, execErr := tx.ExecContext(ctx,
			"INSERT INTO "+tbl+" ("+tc.Name+") VALUES (:1)", //nolint:gas
			tc.Value)
		if execErr != nil {
			t.Fatal("%d. INSERT INTO "+tbl+" (%q) VALUES (%+v): %#v", //nolint:gas
				i, tc.Name, tc.Value, execErr)
		}
		ra, raErr := res.RowsAffected()
		if raErr != nil {
			t.Error(raErr)
		} else if ra != num {
			t.Errorf("%d. %q: wanted %d rows, got %d", i, tc.Name, num, ra)
		}
	}
	tx.Commit()
	sid := func() uint {
		var sid uint
		sql := "SELECT sys_context('userenv','sid') FROM dual"
		err := testDb.QueryRow(sql).Scan(&sid)
		if err != nil {
			t.Fatal(err)
		}
		return sid
	}

	// verify round trips for SingleRowFetch and MultiRowFetch
	// and return failure on unexpected roundtrips

	for _, tCase := range []struct {
		pf, as   int
		srt, mrt uint
	}{
		{useDefaultFetchValue, useDefaultFetchValue, 1, 4},
		{0, useDefaultFetchValue, 2, 4},
		{1, useDefaultFetchValue, 2, 4},
		{2, useDefaultFetchValue, 1, 4},
		{100, useDefaultFetchValue, 1, 3},
		{-1, 100, 2, 4},
		{0, 100, 2, 4},
		{1, 100, 2, 4},
		{2, 100, 1, 4},
		{100, 100, 1, 3},
		{useDefaultFetchValue, 40, 1, 7},
		{2, 40, 1, 7},
		{-1, 40, 2, 7},
		{120, useDefaultFetchValue, 1, 3},
		{120, 100, 1, 3},
		{120, 0, 1, 3},
		{120, -1, 1, 3},
		{120, 250, 1, 2},
		{0, 10, 2, 23},
		{10, 10, 1, 22},
		{214, 214, 1, 2},
		{215, 214, 1, 1},
		{215, useDefaultFetchValue, 1, 1},
		{215, 10, 1, 1},
	} {
		srt, mrt := runPreFetchTests(t, sid(), tCase.pf, tCase.as)
		if !(srt == tCase.srt && mrt == tCase.mrt) {
			t.Fatalf("wanted %d/%d SingleFetchRoundTrip / MultiFetchRoundTrip, got %d/%d", tCase.srt, tCase.mrt, srt, mrt)
		}
	}
}

func runPreFetchTests(t *testing.T, sid uint, pf int, as int) (uint, uint) {
	rt1 := getRoundTrips(t, sid)

	var r uint
	// Do some work
	r = singleRowFetch(t, pf, as)

	rt2 := getRoundTrips(t, sid)

	t.Log("SingleRowFetch: ", "Prefetch:", pf, ", Arraysize:", as, ", Rows: ", r, ", Round-trips:", rt2-rt1)
	srt := rt2 - rt1
	rt1 = rt2
	// Do some work
	r = multiRowFetch(t, pf, as)
	rt2 = getRoundTrips(t, sid)
	t.Log("MultiRowFetch: ", "Prefetch:", pf, ", Arraysize:", as, ", Rows: ", r, ", Round-trips:", rt2-rt1)
	mrt := rt2 - rt1
	return srt, mrt
}
func getRoundTrips(t *testing.T, sid uint) uint {

	sql := `SELECT ss.value
        FROM v$sesstat ss, v$statname sn
        WHERE ss.sid = :sid
        AND ss.statistic# = sn.statistic#
        AND sn.name LIKE '%roundtrip%client%'`
	var rt uint
	err := testSystemDb.QueryRow(sql, sid).Scan(&rt)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func singleRowFetch(t *testing.T, pf int, as int) uint {
	ctx, cancel := context.WithTimeout(testContext("Singlerowfetch"), 10*time.Second)
	defer cancel()
	var employeeid int
	var err error
	tbl := "t_employees" + tblSuffix
	query := "select employee_id from " + tbl + " where employee_id = :id"

	if pf == useDefaultFetchValue && as == useDefaultFetchValue {
		err = testDb.QueryRowContext(ctx, query, 100).Scan(&employeeid)
	} else if pf == useDefaultFetchValue && as != useDefaultFetchValue {
		err = testDb.QueryRowContext(ctx, query, 100, godror.FetchArraySize(as)).Scan(&employeeid)
	} else if pf != useDefaultFetchValue && as == useDefaultFetchValue {
		err = testDb.QueryRowContext(ctx, query, 100, godror.PrefetchCount(pf)).Scan(&employeeid)
	} else {
		err = testDb.QueryRowContext(ctx, query, 100, godror.PrefetchCount(pf), godror.FetchArraySize(as)).Scan(&employeeid)
	}
	if err != nil {
		t.Fatal(err)
	}
	return 1
}

func multiRowFetch(t *testing.T, pf int, as int) uint {

	ctx, cancel := context.WithTimeout(testContext("Singlerowfetch"), 10*time.Second)
	defer cancel()
	tbl := "t_employees" + tblSuffix
	query := "select employee_id from " + tbl + " where rownum < 215"
	var rows *sql.Rows
	var err error

	if pf == useDefaultFetchValue && as == useDefaultFetchValue {
		rows, err = testDb.QueryContext(ctx, query)
	} else if pf == useDefaultFetchValue && as != useDefaultFetchValue {
		rows, err = testDb.QueryContext(ctx, query, godror.FetchArraySize(as))
	} else if pf != useDefaultFetchValue && as == useDefaultFetchValue {
		rows, err = testDb.QueryContext(ctx, query, godror.PrefetchCount(pf))
	} else {
		rows, err = testDb.QueryContext(ctx, query, godror.PrefetchCount(pf), godror.FetchArraySize(as))
	}
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var /* employee_id,*/ c uint
	for rows.Next() {
		c++
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestShortTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ShortTimeout"), 1*time.Minute)
	defer cancel()
	const qry = `SELECT * FROM all_objects ORDER BY DBMS_RANDOM.value`
	shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	rows, err := testDb.QueryContext(shortCtx, qry)
	t.Log("rowsNil:", rows == nil, "error:", err)
	shortCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Log(err)
			return
		}
		t.Fatal(err)
	}
	rows.Close()
}

func TestIssue100(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Issue100"), 1*time.Minute)
	defer cancel()
	const baseName = "test_issue100"
	{
		const qry = `create or replace type ` + baseName + `_t as table of TIMESTAMP`
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer func() { testDb.ExecContext(context.Background(), "DROP TYPE "+baseName+"_t") }()
	}
	{
		const qry = `create or replace FUNCTION ` + baseName + `_f (
    ITERS IN VARCHAR2 DEFAULT 10,
    PAUSE IN VARCHAR2 DEFAULT 1
) RETURN ` + baseName + `_t AUTHID CURRENT_USER PIPELINED AS
BEGIN
    FOR i IN 1..ITERS LOOP
        DBMS_SESSION.SLEEP(PAUSE);
        PIPE ROW(SYSTIMESTAMP);
    END LOOP;
END;`
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer func() { testDb.ExecContext(context.Background(), "DROP FUNCTION "+baseName+"_f") }()
	}

	const qry = `SELECT * FROM TABLE(` + baseName + `_f(iters => 15, pause => 1))`
	ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rows, err := testDb.QueryContext(ctx, qry)
	t.Logf("error: %+v", err)
	if err == nil {
		defer rows.Close()
	}

	if ctx.Err() == context.DeadlineExceeded {
		t.Logf("Error: Timeout")
	}
	if rows == nil {
		return
	}

	var res string
	err = rows.Scan(&res)
	if err != nil {
		t.Logf("Row Fetch Error: %+v", err)
	}

	t.Logf("Result: %s", res)
}

func TestStmtFetchDeadlineForLOB(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(testContext("TestStmtFetchDeadline"), 60*time.Second)
	defer cancel()

	// Create a table used for fetching clob, blob data

	tbl := "t_lob_fetch" + tblSuffix
	const basename = "test_stmtfetchdeadline"
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.ExecContext(ctx, "DROP TABLE "+tbl)

	conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (k number, c clob, b blob)", //nolint:gas
	)
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (k, c, b) VALUES (:1, :2, :3)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for tN, tC := range []struct {
		String string
		Bytes  []byte
	}{
		{Bytes: []byte{0, 1, 2, 3, 4}, String: "abcdef"},
		{Bytes: []byte{5, 6, 7, 8, 9}, String: "ghijkl"},
	} {

		// Straight bind
		if _, err = stmt.ExecContext(ctx, tN*2, tC.String, tC.Bytes); err != nil {
			t.Errorf("%d. %v", tN, err)
		}

		if _, err = stmt.ExecContext(ctx, tN*2+1, tC.String, tC.Bytes); err != nil {
			t.Errorf("%d. %v", tN, err)
		}
	}

	const qryf = `create or replace FUNCTION ` + basename + `_f (
    ITERS IN VARCHAR2 DEFAULT 10,
    PAUSE IN VARCHAR2 DEFAULT 1
) RETURN number
IS
cnt number;

BEGIN
    FOR i IN 1..ITERS LOOP
        DBMS_SESSION.SLEEP(PAUSE);
    END LOOP;
    cnt :=2;
    RETURN cnt;
END;`
	if _, err = testDb.ExecContext(context.Background(), qryf); err != nil {
		t.Fatal(fmt.Errorf("%s: %+v", qryf, err))
	}
	defer func() { testDb.ExecContext(context.Background(), "DROP FUNCTION "+tbl+"_f") }()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	qry := `select k, c, b from ` + tbl + ` where k <= ` + basename + `_f(iters => 2, pause => 1)`
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %v", qry, err))
	}
	defer rows.Close()

	var k int
	var c string
	var b []byte

	for rows.Next() { // stmtFetch wont complete and cause deadline error
		if err = ctx.Err(); err != nil {
			break
		}
		if err = rows.Scan(&k, &c, &b); err != nil {
			t.Fatal(err)
		}
		t.Logf("key=%v, CLOB=%q BLOB=%q\n", k, c, b)
	}

	err = rows.Err()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Logf("Info: %+v", err)
		}
	} else if err = ctx.Err(); !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("Error:Deadline Not Exceeded, but %+v", err)
	}
}

// This covers following externalAuthentication test cases:
// - standalone=0
//   - user as [sessonusername], during creation or first Query causes DPI-1032
//   - empty user/passwd ,taking credentials from external source.
//   - Proxy Authentication by giving sessionuser , [sessionuser]
//   - error DPI-1069, if sessionuser is passed without brackets
//
// - standalone=1
//   - user as [sessonusername], during creation return success
//   - empty user/passwd ,taking credentials from external source.
//
// connectstring alias for example is defined in env GODROR_TEST_DB
// example: export GODROR_TEST_DB=db10g
// db_alias,db10g should match in tnsnames.ora and credential entry in wallet
// this test case will skip if external wallet is not defined but wont fail
func TestExternalAuthIntegration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(testContext("TestExternalAuth"), 1*time.Minute)
	defer cancel()

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	username := cs.Username

	// make username/passwd empty
	cs.Username = ""
	cs.Password.Reset()

	// setting ExternalAuth is optional as
	// with empty username/passwd , homeogeneous mode
	// ExternalAuth and heterogeneous are internally set
	//cs.ExternalAuth = true

	sessionUserPassword := getRandomString()
	const sessionUser = "test_sessionUser"

	testExternalAuthConStr := cs.StringWithPassword()
	t.Log("testExternalAuthConStr", testExternalAuthConStr)

	var testExternalAuthDB *sql.DB
	if testExternalAuthDB, err = sql.Open("godror", testExternalAuthConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testExternalAuthConStr, err))
	}
	defer testExternalAuthDB.Close()
	testExternalAuthDB.SetMaxIdleConns(0)

	// It also causes heterogeneous pool to be created
	// for external authentication with username, password as empty parameters
	testExternalAuthDB.ExecContext(ctx, fmt.Sprintf("DROP USER %s", sessionUser))
	// set up connectstring with sessionuser in pool creation
	cs.Username = "[" + sessionUser + "]"
	cs.Password.Reset()
	cs.ExternalAuth = godror.Bool(true)
	if !cs.IsStandalone() {
		cs.Heterogeneous = godror.Bool(true)
	}
	testExternalAuthProxyConStr := cs.StringWithPassword()
	t.Log("testExternalAuthProxyConStr", testExternalAuthProxyConStr)

	var testExternalAuthProxyDB *sql.DB
	if testExternalAuthProxyDB, err = sql.Open("godror", testExternalAuthProxyConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testExternalAuthProxyConStr, err))
	}
	defer testExternalAuthProxyDB.Close()
	testExternalAuthProxyDB.SetMaxIdleConns(0)

	for _, qry := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY "+sessionUserPassword, sessionUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", sessionUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", sessionUser, username),
	} {
		if _, err = testExternalAuthDB.ExecContext(ctx, qry); err != nil {
			if strings.Contains(err.Error(), "ORA-01031:") {
				t.Log("Please issue this:\nGRANT CREATE USER, DROP USER, ALTER USER TO " + username + ";\n" +
					"GRANT CREATE SESSION TO " + username + " WITH ADMIN OPTION;\n")
			}
			t.Skip(fmt.Errorf(" %s: %w", qry, err))
		}
	}
	defer func() {
		testExternalAuthDB.ExecContext(testContext("ExternalAuthentication-drop"), "DROP USER "+sessionUser)
	}()

	// test Proxyuser getsession with pool created with external credentials
	var result string
	if err = testExternalAuthProxyDB.QueryRowContext(ctx, "SELECT user FROM dual").Scan(&result); err != nil {

		if !cs.IsStandalone() {
			if !strings.Contains(err.Error(), "DPI-1032:") {
				t.Errorf("testExternalAuthProxyDB: unexpected Error %s", err.Error())
			}
		} else {
			t.Fatal(err)
		}
	}
	if cs.IsStandalone() && !strings.EqualFold(sessionUser, result) {
		t.Errorf("testExternalAuthProxyDB: currentUser got %s, wanted %s", result, sessionUser)
	}

	for tName, tCase := range map[string]struct {
		In   context.Context
		Want string
	}{
		"noContext":                  {In: ctx, Want: username},
		"sessionUser":                {In: godror.ContextWithUserPassw(ctx, "["+sessionUser+"]", "", ""), Want: sessionUser},
		"sessionUserwithOutbrackets": {In: godror.ContextWithUserPassw(ctx, sessionUser, "", ""), Want: sessionUser},
	} {
		if cs.StandaloneConnection.Valid && cs.StandaloneConnection.Bool && tName != "noContext" {
			// for standalone changing connection params not allowed
			continue
		}
		t.Run(tName, func(t *testing.T) {
			var result string
			if err = testExternalAuthDB.QueryRowContext(tCase.In, "SELECT user FROM dual").Scan(&result); err != nil {
				if tName == "sessionUserwithOutbrackets" {
					if !strings.Contains(err.Error(), "DPI-1069:") {
						t.Errorf("%s: unexpected Error %s", tName, err.Error())
					}
				} else {
					t.Fatal(err)
				}
			}
			if tName != "sessionUserwithOutbrackets" {
				if !strings.EqualFold(tCase.Want, result) {
					t.Errorf("%s: currentUser got %s, wanted %s", tName, result, tCase.Want)
				}
			}
		})
	}
}

func getRandomString() string {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")
	length := 8
	var b strings.Builder
	for range length {
		b.WriteRune(chars[rnd.Intn(len(chars))])
	}
	// fix starting letter as alphabet
	return "A" + b.String() // E.g. "ExcbsVQs"
}

func TestNullIssue143(t *testing.T) {
	t.Parallel()
	const funQry = `CREATE OR REPLACE FUNCTION test_getXml_143(
		    pInDate IN DATE,
		    pOutDate IN DATE,
		    pAmount IN NUMBER DEFAULT NULL) RETURN CLOB IS BEGIN RETURN(NULL); END;`

	ctx, cancel := context.WithTimeout(testContext("NullIssue143"), 10*time.Second)
	defer cancel()

	if _, err := testDb.ExecContext(ctx, funQry); err != nil {
		t.Fatalf("%s: %v", funQry, err)
	}
	defer testDb.ExecContext(context.Background(), "DROP FUNCTION test_getXml_143")

	const qry = `begin :xmlResponse := test_getXml_143(pindate => :startDate,
                                              poutdate => :endDate,
											  pamount => :amount);end;`
	stmt, err := testDb.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("prepare %s: %+v", qry, err)
	}
	defer stmt.Close()

	var xmlResponse string
	params := []any{
		sql.Named("xmlResponse", sql.Out{Dest: &xmlResponse}),
		sql.Named("startDate", time.Now()),
		sql.Named("endDate", time.Now()),
		sql.Named("amount", nil),
	}
	_, err = stmt.ExecContext(ctx, params...)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}

	// Logic for xmlResponse
	t.Log(xmlResponse)
}

func TestForError8192(t *testing.T) {
	t.Parallel()
	params, err := godror.ParseConnString(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	params.StandaloneConnection = godror.Bool(true)
	params.Timezone = time.UTC
	t.Log("UTC params:", params)
	dbUTC, err := sql.Open("godror", params.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer dbUTC.Close()
	params.Timezone = time.Local
	t.Log("local params:", params)
	dbLocal, err := sql.Open("godror", params.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer dbLocal.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	connUTC, err := dbUTC.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connUTC.Close()
	connLocal, err := dbLocal.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connLocal.Close()

	// prepare table
	_, _ = connUTC.ExecContext(ctx, "DROP TABLE test_date_error")
	if _, err := connUTC.ExecContext(ctx, "CREATE TABLE test_date_error (problem_ts DATE)"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = connUTC.ExecContext(context.Background(), "DROP TABLE test_date_error") }()

	// fetch a date from the database
	selectStmt, err := connUTC.PrepareContext(ctx, "select to_date('01-01-0001','dd-mm-yyyy') problem_ts from dual")
	if err != nil {
		t.Fatal(err)
	}
	defer selectStmt.Close()
	rows, err := selectStmt.Query()
	if err != nil {
		t.Fatalf("Failed to query: %s\n", err)
	}
	defer rows.Close()

	var problemT sql.NullTime
	for rows.Next() {
		if err = rows.Scan(&problemT); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("problemT=%v, valid? %t, zero? %t", problemT.Time, problemT.Valid, problemT.Time.IsZero())
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	// insert the date in the database
	tx, err := connLocal.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to beginTx: %s\n", err)
	}
	defer tx.Rollback()

	const qry = "insert into test_date_error(problem_ts) values(:problem_ts)"
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer func() { stmt.Close() }()

	tim := time.Time{}.UTC().Add(-time.Minute) //In(time.FixedZone("LMT", (+50 * 60)))
	for i := range 6 * 30 {
		tim := tim.Add(time.Duration(-1*(i%2)) * time.Duration(i) * time.Minute)

		param := sql.NullTime{Time: tim, Valid: true}
		_, err = stmt.ExecContext(ctx, param)
		// msg=setTimestamp time=0001-01-01T01:15:20+01:16 utc=0000-12-31T23:59:00Z tz=Local Y=1 M=January D=1 h=1 m=15 s=20 t=0 tzHour=2 tzMin=0

		if err != nil {
			if errors.Is(err, godror.ErrBadDate) {
				t.Logf("exec failure for Local %v: %v\n", tim, err)
			} else {
				t.Errorf("exec failure for Local %v: %+v\n", tim, err)
				if _, err = dbUTC.ExecContext(ctx, qry, param); err != nil {
					t.Fatalf("exec failure for UTC %v: %+v", tim, err)
				}
				return
			}
			// Close the corrupted stmt and prepare a new one
			stmt.Close()
			if stmt, err = tx.PrepareContext(ctx, qry); err != nil {
				t.Fatalf("%s: %+v", qry, err)
			}
		}
	}
}

func TestObjType_Issue198(t *testing.T) {
	if testing.Short() {
		t.Skip("skip")
	}
	ctx, cancel := context.WithTimeout(testContext("ObjType_Issue198"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer testCon.Close()
	if err = testCon.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("dbstats: %#v", testDb.Stats())

	cleanup := func() {
		testDb.Exec("DROP TYPE test_obj198_t")
	}
	cleanup()
	const crea = `CREATE OR REPLACE TYPE test_obj198_t AS OBJECT (L VARCHAR2(10), T NUMBER);`
	if _, err = testDb.ExecContext(ctx, crea); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", crea, err))
	}

	defer cleanup()

	oT, err := testCon.GetObjectType(strings.ToUpper("test_obj198_t"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(oT)

	pid := os.Getpid()
	var start uint64
	const N = 1_000_000
	for i := range N {
		obj, err := oT.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		obj.Close()
		if i%1000 == 0 {
			s, err := readSmaps(pid)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(s)
			if start == 0 {
				start = s
			}
		}
	}
	s, err := readSmaps(pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(s)
	if start < s {
		per100Iteration := (s - start) * 100 / N
		t.Logf("Memory: start=%d end=%d (%.03f%% gain), %d bytes per 100 iteration", start, s, float64((s-start)*100)/float64(s), per100Iteration)
		if per100Iteration >= 100 {
			t.Errorf("memory leak - try with libjemalloc (LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1)")
		}
	}
}

func TestLoopInLoop_199(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	ctx, cancel := context.WithTimeout(testContext("LoopInLoop_199"), 1*time.Minute)
	defer cancel()
	var first, last, avg time.Duration
	const cnt = 20
	for i := range cnt {
		start := time.Now()
		rows, err := testDb.QueryContext(ctx, "SELECT 1 FROM DUAL")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			for range cnt {
				rows2, err := testDb.QueryContext(ctx, "SELECT 1 FROM DUAL")
				if err != nil {
					t.Fatal(err)
				}
				for rows2.Next() {
				}
				//rows2.Close()
			}
			//rows.Close()
			if i == 0 { // The first is very cold (slow)
				continue
			}
			last = time.Since(start)
			if first == 0 {
				first = last
			}
			avg += last
			t.Log(last)
		}
	}
	avg /= cnt
	t.Log("first:", first, "last:", last, "avg:", avg)
	if first*11 < last*10 {
		t.Error("last > 110% of first")
	}
	if !(avg*8/10 < last && last < avg*12/10) || first*11 < last*10 {
		t.Error("last does differ from the avg more than 20%")
	}
}
func TestLobAsStringTypeName(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("LobAsStringTypeName"), 10*time.Second)
	defer cancel()

	tbl := "test_lobasstring" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	qry := "CREATE TABLE " + tbl + " (f_id NUMBER, f_blob BLOB)"
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer func() { _, _ = testDb.Exec("DROP TABLE " + tbl) }()

	qry = "SELECT F_id, F_blob FROM " + tbl
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	cols, err := rows.ColumnTypes()
	rows.Close()
	want := map[string]string{"F_ID": "NUMBER", "F_BLOB": "BLOB"}
	got := make(map[string]string, len(cols))
	for _, c := range cols {
		got[c.Name()] = c.DatabaseTypeName()
	}
	t.Log("got:", got)
	if err != nil {
		t.Error(err)
	}
	if d := cmp.Diff(want, got); d != "" {
		t.Error(d)
	}
}

func TestTTCIssue215(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("TestBreakingOracle12"), 10*time.Second)
	defer cancel()
	cleanup := func() {
		for _, qry := range []string{
			"DROP PROCEDURE test_get_menu_bec" + tblSuffix,
			"DROP TYPE test_MENU_REC" + tblSuffix,
			"DROP TYPE test_MENU" + tblSuffix,
		} {
			testDb.Exec(qry)
		}
	}

	cleanup()
	defer cleanup()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Creating Types and Procedure
	for _, qry := range []string{
		`CREATE OR REPLACE TYPE test_MENU_REC` + tblSuffix + ` IS OBJECT(
        menudesc   VARCHAR2(200),
        menufather NUMBER,
        menulevel  NUMBER,
        menuurl    VARCHAR2(200),
        menuid     NUMBER,
        menuorder  NUMBER,
        menueventlist char(1))`,
		`CREATE OR REPLACE TYPE test_MENU` + tblSuffix + ` IS table OF test_menu_rec` + tblSuffix,
		`CREATE OR REPLACE PROCEDURE test_get_menu_bec` + tblSuffix + `(
  p_userid in varchar2,
  p_language in integer,
  p_message out varchar2,
  p_menu out test_menu` + tblSuffix + `,
  p_imagefile out varchar2,
  p_lastname out varchar2,
  p_firstname out varchar2,
  p_profile out varchar2,
  p_company out varchar2) IS
BEGIN
  p_message := 'Oracle is breaking version 12';
  p_imagefile := 'amazing_summer.jpg';
  p_lastname := 'Rossi';
  p_firstname := 'Mario';
  p_profile := 'Photo';
  p_company := 'GitHub';
  p_menu := test_menu` + tblSuffix + `();
  p_menu.extend();
  p_menu(1) := test_menu_rec` + tblSuffix + `(
    'Awesome',
    0,
    0,
    '/github',
    1,
    1,
    'Y');
END;`,
	} {
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	if errs, err := godror.GetCompileErrors(ctx, conn, false); err != nil {
		t.Fatal(err)
	} else if len(errs) != 0 {
		t.Fatal(errs)
	}

	var message, imageFile, lastName, firstName, profile, company string

	stmt, err := conn.PrepareContext(ctx, "call test_GET_MENU_BEC"+tblSuffix+"(:P_USERID,:P_LANGUAGE,:P_MESSAGE,:P_MENU,:P_IMAGEFILE,:P_LASTNAME,:P_FIRSTNAME,:P_PROFILE,:P_COMPANY)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	resType, err := godror.GetObjectType(ctx, conn, "test_MENU"+tblSuffix)
	if err != nil {
		t.Fatal(err)
	}
	defer resType.Close()
	res, err := resType.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if _, err := stmt.ExecContext(ctx,
		sql.Named("P_USERID", "n.one"),
		sql.Named("P_LANGUAGE", 0),

		sql.Named("P_MESSAGE", sql.Out{Dest: &message}),
		sql.Named("P_MENU", sql.Out{Dest: &res}),
		sql.Named("P_IMAGEFILE", sql.Out{Dest: &imageFile}),
		sql.Named("P_LASTNAME", sql.Out{Dest: &lastName}),
		sql.Named("P_FIRSTNAME", sql.Out{Dest: &firstName}),
		sql.Named("P_PROFILE", sql.Out{Dest: &profile}),
		sql.Named("P_COMPANY", sql.Out{Dest: &company}),
	); err != nil {
		t.Fatal(err)
	}
	t.Log(res.AsMapSlice(true))
}
func TestSelectText(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectText"), 10*time.Second)
	defer cancel()
	qry := "SELECT UPPER(:1) FROM DUAL"
	tx, err := testDb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := testDb.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	for _, in := range []string{"a", "abc", "abcd", "árvíztűrő tükörfúrógép"} {
		var got string
		if err := stmt.QueryRowContext(ctx, in).Scan(&got); err != nil {
			t.Errorf("%s [%q]: %+v", qry, in, err)
		}
		t.Logf("%q -> %q", in, got)
		if want := strings.ToUpper(in); got != want {
			t.Errorf("%q. got %q, wanted %q", in, got, want)
		}
	}
}

func TestReplaceQuestionPlaceholders(t *testing.T) {
	for tN, tC := range []struct {
		Qry, Want string
	}{
		{" ", " "},
		{"?", ":1"},
		{
			"SELECT * FROM a WHERE id=? AND b=?",
			"SELECT * FROM a WHERE id=:1 AND b=:2",
		},
		{
			"BEGIN ? := fun(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); END;",
			"BEGIN :1 := fun(:2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12); END;",
		},
	} {
		if got := godror.ReplaceQuestionPlacholders(tC.Qry); got != tC.Want {
			t.Errorf("%d. got\n%q wanted\n%q", tN, got, tC.Want)
		}
	}
}

func TestPipelinedSelectIssue289(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectText"), 10*time.Second)
	defer cancel()
	funcName := "test_pipelined_" + tblSuffix
	defer testDb.ExecContext(context.Background(), "DROP FUNCTION "+funcName)
	qry := `CREATE OR REPLACE FUNCTION ` + funcName + `(v_string IN VARCHAR2, v_delimiter IN VARCHAR2) RETURN SYS.ODCIVARCHAR2LIST PIPELINED IS
  v_start_pos       PLS_INTEGER := 1;
  v_next_pos        PLS_INTEGER;
BEGIN
  LOOP
    v_next_pos := INSTR(v_string, v_delimiter, v_start_pos);
    IF v_next_pos = 0 THEN
      PIPE ROW (SUBSTR(v_string, v_start_pos));
      EXIT;
    END IF;
    PIPE ROW (SUBSTR(v_string, v_start_pos, v_next_pos - v_start_pos));
    v_start_pos := v_next_pos + LENGTH(v_delimiter);
  END LOOP;
  RETURN;
END;`
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	fun := funcName + "('sdfasdf,asdfasdf',',')"

	t.Run("TABLE", func(t *testing.T) {
		qry := "select * FROM TABLE(" + fun + ")"
		rows, err := testDb.QueryContext(ctx, qry)
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer rows.Close()
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatalf("scan: %+v", err)
			}
			t.Log(text)
		}
	})

	t.Run("CALL", func(t *testing.T) {
		qry := "CALL :1 := " + fun + ";"
		var obj godror.Object
		if _, err := testDb.ExecContext(ctx, qry, sql.Out{Dest: &obj}); err != nil {
			t.Skipf("%s: %+v", qry, err)
		}
		t.Log("obj:", obj)
	})
}

func TestLevelSerializable(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("LevelSerializable"), 10*time.Second)
	defer cancel()

	tbl := "test_serializable_" + tblSuffix
	dropQry := `DROP TABLE ` + tbl
	testDb.Exec(dropQry)
	{
		qry := "CREATE TABLE " + tbl + " (id NUMBER(3))"
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(err)
		}
	}
	defer testDb.Exec(dropQry)
	txRO, err := testDb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer txRO.Rollback()
	txRW, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer txRW.Rollback()

	C := func(tx interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}) int {
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		t.Logf("%p: %d", tx, n)
		return n
	}

	if !(C(txRO) == 0 && C(txRW) == 0) {
		t.Fatal("starting with non-zero rows?!")
	}
	if _, err := txRW.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (:1)", 3); err != nil {
		t.Fatal("insert1:", err)
	}
	if got := C(txRW); got != 1 {
		t.Errorf("txRW has %d rows (instead of 1)", got)
	}
	if got := C(txRO); got != 0 {
		t.Errorf("txRO has %d rows (instead of 0)", got)
	}

	if err := txRW.Commit(); err != nil {
		t.Errorf("commit txRW: %+v", err)
	}
	if got := C(txRO); got != 0 {
		t.Errorf("txRO sees %d rows (instead of 0)", got)
	}
}

func TestSelectAlterSessionIssue297(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext(t.Name()), 10*time.Second)
	defer cancel()

	db := testSystemDb
	qry := "ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT=128"
	if db == nil {
		db = testDb
		qry = "ALTER SESSION SET ISOLATION_LEVEL = READ COMMIT" + "TED"
	}
	grp, grpCtx := errgroup.WithContext(ctx)
	for range 10 {
		grp.Go(func() error {
			conn, err := db.Conn(grpCtx)
			if err != nil {
				return err
			}
			defer conn.Close()

			_, err = conn.ExecContext(ctx, qry)
			if err != nil {
				return fmt.Errorf("%s: %w", qry, err)
			}
			qry = "select sysdate from dual"
			rows, err := conn.QueryContext(ctx, qry)
			if err != nil {
				return fmt.Errorf("%s: %w", qry, err)
			}
			defer rows.Close()
			for rows.Next() {
				var a1 string
				if err := rows.Scan(&a1); err != nil {
					return fmt.Errorf("scan %s: %w", qry, err)
				}
				t.Log(a1)
			}
			return rows.Err()
		})
	}

	if err := grp.Wait(); err != nil {
		t.Fatal(err)
	}
}

type ReaderFunc func([]byte) (int, error)

func (rf ReaderFunc) Read(p []byte) (int, error) { return rf(p) }

func TestBindNumber(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext(t.Name()), 10*time.Second)
	defer cancel()

	tx, err := testDb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	const qry = "SELECT DUMP(:1) FROM DUAL"
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("prepare %s: %+v", qry, err)
	}
	defer stmt.Close()

	type intLike int
	type floatLike float64

	for _, tC := range []struct {
		In   any
		Want string
	}{
		{int(1), "Typ=2 Len=2: 193,2"},
		{float32(3.14), "Typ=100 Len=4: 192,72,245,195"},
		{intLike(2), "Typ=2 Len=2: 193,3"},
		{floatLike(2.78), "Typ=101 Len=8: 192,6,61,112,163,215,10,61"},
	} {
		var got string
		if err := stmt.QueryRowContext(ctx, tC.In).Scan(&got); err != nil {
			t.Fatalf("scan %q [%v]: %+v", qry, tC.In, err)
		}
		t.Log(tC.In, "got", got)
		if got != tC.Want {
			t.Errorf("%v: got %q, wanted %q", tC.In, got, tC.Want)
		}
	}
}

func TestPartialBatch(t *testing.T) {
	defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext(t.Name()), 10*time.Second)
	defer cancel()

	tbl := "test_savexc_" + tblSuffix
	dropQry := `DROP TABLE ` + tbl
	testDb.ExecContext(ctx, dropQry)
	{
		for _, qry := range []string{
			"CREATE TABLE " + tbl + " (id NUMBER(3) NOT NULL)",
			"ALTER TABLE " + tbl + " ADD CONSTRAINT c_" + tbl + " check (MOD(id, 2) = 1 AND id > 0)",
		} {
			if _, err := testDb.ExecContext(ctx, qry); err != nil {
				t.Fatal(err)
			}
		}
	}
	defer testDb.Exec(dropQry)

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	values := []int32{-1, 0, 1, 2, 3, 4, 5}
	wantAffected := []int{2, 4, 6}
	wantUnaffected := []int{0, 1, 3, 5}
	qry := "INSERT INTO " + tbl + " (id) VALUES (:1)"

	checkRowCount := func(t *testing.T, want int) {
		var got int
		cntQry := "SELECT COUNT(0) FROM " + tbl
		if err := tx.QueryRowContext(ctx, cntQry).Scan(&got); err != nil {
			t.Fatalf("%s: %+v", cntQry, err)
		}
		t.Logf("have %d rows", got)
		if got != want {
			t.Errorf("got %d rows, wanted %d", got, want)
		}
	}

	var be *godror.BatchErrors
	t.Run("no-option", func(t *testing.T) {
		res, err := tx.ExecContext(ctx, qry, values)
		if res == nil {
			t.Log("res is nil!")
		} else {
			if ra, err := res.RowsAffected(); err != nil {
				t.Errorf("get RowsAffected: %+v", err)
			} else if ra != 0 {
				t.Errorf("rowsAffected=%d, wanted 0", ra)
			}
		}
		if err == nil {
			t.Errorf("wanted error, got nil")
		} else if errors.As(err, &be) {
			t.Errorf("wanted normal error, got %#v", be)
		}
		checkRowCount(t, 0)
	})

	t.Run("PartialBatch", func(t *testing.T) {
		res, err := tx.ExecContext(ctx, qry, values, godror.PartialBatch())
		if res == nil {
			t.Log("res is nil!")
		} else {
			ra, err := res.RowsAffected()
			if err != nil {
				t.Errorf("get RowsAffected: %+v", err)
			}
			t.Logf("rowsAffected=%d", ra)
			if ra != int64(len(wantAffected)) {
				t.Errorf("rowsAffected=%d, wanted %d", ra, len(wantAffected))
			}
		}
		if err == nil {
			t.Error("wanted error, got <nil>")
		} else if !errors.As(err, &be) {
			t.Errorf("%s %v: NOT BatchError %+v", qry, values, err)
		} else {
			for _, oe := range be.Errs {
				t.Logf("erroneous offset: %d", oe.Offset())
			}
			t.Logf("affected=%#v unaffected=%#v", be.Affected, be.Unaffected)
			if wanted := wantAffected; !reflect.DeepEqual(be.Affected, wanted) {
				t.Errorf("affected is %#v, wanted %#v", be.Affected, wanted)
			}
			if wanted := wantUnaffected; !reflect.DeepEqual(be.Unaffected, wanted) {
				t.Errorf("affected is %#v, wanted %#v", be.Unaffected, wanted)
			}
		}

		checkRowCount(t, len(wantAffected))
	})
}

func TestStandaloneVsPool(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	Ps, Pp := P, P
	if Ps.StandaloneConnection.Valid && Ps.StandaloneConnection.Bool {
		Pp.StandaloneConnection = godror.Bool(false)
	} else {
		Ps.StandaloneConnection = godror.Bool(true)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, P := range []godror.ConnectionParams{Ps, Pp} {
		db := sql.OpenDB(godror.NewConnector(P))
		t.Logf("Standalone=%t %s", P.StandaloneConnection, P)
		const qry = "SELECT SYS_CONTEXT('USERENV', 'INSTANCE_NAME')||'/'||SYS_CONTEXT('USERENV','SERVICE_NAME')||'/'||SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL"
		var s string
		if err := db.QueryRowContext(ctx, qry).Scan(&s); err != nil {
			t.Errorf("%s %s: %+v", P, qry, err)
		}
		t.Logf("Standalone=%t: %s", P.StandaloneConnection, s)
		db.Close()
	}
}

func TestWarningAsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const query = `
		DECLARE
			ora_24344 EXCEPTION;
			PRAGMA EXCEPTION_INIT(ora_24344, -24344);
		BEGIN
			RAISE ora_24344;
		END;`
	args := []any{godror.WarningAsError()}
	for _, tC := range []struct {
		Qry     string
		Code    int
		Warning bool
	}{
		// Some syntax error
		{Qry: "SELECT 1 FROM not-exist", Code: 903},
		// No error - warning should not return the previous error
		{Qry: "SELECT 1 FROM DUAL", Warning: true},
		// Not a syntax error, but a compile error - just a warning
		{Qry: query},
		// Proper error
		{Qry: `BEGIN raise_application_error(-20001, 'test'); END;`, Code: 20001},
		// Warning as error
		{Qry: query, Code: 24344, Warning: true},
	} {
		args = args[:0]
		if tC.Warning {
			args = args[:1]
		}
		var ec interface{ Code() int }
		_, err := testDb.ExecContext(ctx, tC.Qry, args...)
		if tC.Code == 0 && err != nil {
			t.Errorf("got %+v, wanted no err", err)
		} else if tC.Code != 0 {
			if err == nil {
				t.Errorf("wanted error for %q", tC.Qry)
			} else if !errors.As(err, &ec) {
				t.Errorf("wanted OraErr, got %#v", err)
			} else if got := ec.Code(); got != tC.Code {
				t.Errorf("wanted ORA-%d, got %d", tC.Code, got)
			}
		}
	}
}

type nilPointerValuer string

func (n nilPointerValuer) Value() (driver.Value, error) {
	return nil, nil
}

func TestNilPointeValuer(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("NilPointerValuer"), 10*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_nilPointerValuer" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), string varchar2(16), CONSTRAINT ID_PK PRIMARY KEY (id))", //nolint:gas
	)
	if err != nil {
		t.Error(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)
	t.Logf(" NilPointerValuer table  %q: ", tbl)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (id, string) VALUES (:1, :2)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	var valuer *nilPointerValuer
	if _, err = stmt.ExecContext(ctx, 1, valuer); err != nil {
		t.Errorf("%d/1. (%v): %v", 1, valuer, err)
	}

	if _, err = stmt.ExecContext(ctx, 2, nil); err != nil {
		t.Fatal(err)
	}

}
