// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// execCacheDB returns a DB with a driver of its own, with the given change
// of the connection parameters.
func execCacheDB(t *testing.T, tweak func(P *godror.ConnectionParams)) *sql.DB {
	t.Helper()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if tweak != nil {
		tweak(&P)
	}
	return sql.OpenDB(godror.NewDriver().NewConnector(P))
}

// A failed Exec must not leave its closed statement in the cache.
func TestExecCacheAfterFailedExec(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const qry = "DECLARE v NUMBER; BEGIN v := 1/:1; END;"
	if _, err := conn.ExecContext(ctx, qry, 1); err != nil {
		t.Fatal(err)
	}
	var oraErr *godror.OraErr
	if _, err := conn.ExecContext(ctx, qry, 0); !errors.As(err, &oraErr) || oraErr.Code() != 1476 {
		t.Fatalf("got %+v, wanted ORA-01476", err)
	}
	for i := range 3 {
		if _, err := conn.ExecContext(ctx, qry, 1); err != nil {
			t.Fatalf("%d. Exec after the failed Exec of the same text: %+v", i, err)
		}
	}
}

// The statements evicted from the cache must be closed: an open statement
// holds its connection, which then does not go back to the pool.
func TestExecCacheEvictedIsClosed(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	db := execCacheDB(t, func(P *godror.ConnectionParams) {
		P.StandaloneConnection = sql.NullBool{Valid: true, Bool: false}
		P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 1, 1
		P.WaitTimeout = 3 * time.Second
		P.StmtCacheSize = 2
	})
	defer db.Close()
	db.SetMaxIdleConns(0) // closing the *sql.Conn closes the driver's connection

	for round := range 3 {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("round %d: the only session of the pool was not released: %+v", round, err)
		}
		for i := range 5 {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("BEGIN /* %d */ NULL; END;", i)); err != nil {
				t.Fatalf("%d/%d. %+v", round, i, err)
			}
		}
		conn.Close()
	}
}

// When the connection is lost, the Exec of a cached statement must return, and
// not wait for itself: a bad connection is closed with its cached statements.
func TestExecCacheLostConn(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	db := execCacheDB(t, nil)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const qry = "DECLARE v VARCHAR2(100); BEGIN v := :1; END;"
	if _, err := conn.ExecContext(ctx, qry, 1); err != nil {
		t.Fatal(err)
	}
	// Lose the connection: a timed out call closes it.
	shortCtx, shortCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = conn.ExecContext(shortCtx, "BEGIN DBMS_SESSION.SLEEP(5); END;")
	shortCancel()
	if err == nil {
		t.Fatal("wanted a timeout")
	}

	done := make(chan error, 1)
	go func() {
		// A string needs a new bind variable, which fails on a lost connection.
		_, err := conn.ExecContext(ctx, qry, "a")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Exec succeeded on a lost connection")
		}
		conn.Close()
		db.Close()
	case <-time.After(10 * time.Second):
		// db is not closed: Close would wait for the hanging Exec.
		t.Fatal("Exec of a cached statement hangs on the lost connection")
	}
}

// StmtCacheSize=-1 disables the statement cache; 1 is the smallest cache.
func TestExecCacheSize(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	for _, size := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("StmtCacheSize=%d: panic: %v", size, r)
				}
			}()
			db := execCacheDB(t, func(P *godror.ConnectionParams) { P.StmtCacheSize = size })
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for i := range 6 {
				qry := fmt.Sprintf("BEGIN /* %d */ :1 := :2 * 2; END;", i%2)
				var got int
				if _, err := conn.ExecContext(ctx, qry, sql.Out{Dest: &got}, i); err != nil {
					t.Fatalf("%d. %+v", i, err)
				}
				if got != 2*i {
					t.Errorf("%d. got %d, wanted %d", i, got, 2*i)
				}
			}
		})
	}
}

// A DDL must run every time, even when its text is the same as before.
func TestExecCacheRepeatedDDL(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_exec_cache_ddl" + tblSuffix
	proc := "test_exec_cache_ddl_p" + tblSuffix
	cleanup := func() {
		_, _ = testDb.Exec("DROP TABLE " + tbl + " PURGE")
		_, _ = testDb.Exec("DROP PROCEDURE " + proc)
	}
	cleanup()
	defer cleanup()
	count := func() (n int, err error) {
		err = conn.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n)
		return n, err
	}
	exec := func(qry string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	for i := range 3 {
		// Leading white space and comment, and lower case.
		exec(" /* " + t.Name() + " */ create table " + tbl + " (id NUMBER(6))")
		for j := range 2 {
			exec("INSERT INTO " + tbl + " (id) VALUES (1)")
			exec("TRUNCATE TABLE " + tbl)
			if n, err := count(); err != nil {
				t.Fatal(err)
			} else if n != 0 {
				t.Errorf("%d/%d. %d rows after TRUNCATE", i, j, n)
			}
		}
		for j := range 2 {
			exec("ALTER TABLE " + tbl + " ADD (c VARCHAR2(10))")
			exec("COMMENT ON COLUMN " + tbl + ".c IS 'c'")
			exec("INSERT INTO " + tbl + " (id, c) VALUES (1, 'a')")
			exec("ALTER TABLE " + tbl + " DROP COLUMN c")
			var oraErr *godror.OraErr
			if _, err := conn.ExecContext(ctx, "COMMENT ON COLUMN "+tbl+".c IS 'c'"); !errors.As(err, &oraErr) || oraErr.Code() != 904 {
				t.Errorf("%d/%d. COMMENT on the dropped column: got %+v, wanted ORA-00904", i, j, err)
			}
		}
		exec("DROP TABLE " + tbl + " PURGE")
		var oraErr *godror.OraErr
		if _, err := count(); !errors.As(err, &oraErr) || oraErr.Code() != 942 {
			t.Errorf("%d. the table is still there after DROP TABLE (%+v)", i, err)
		}
	}

	// A CREATE with PL/SQL body, repeated.
	for i := range 3 {
		exec("CREATE OR REPLACE PROCEDURE " + proc + "(p OUT NUMBER) IS BEGIN p := " + fmt.Sprint(i) + "; END;")
		for range 2 {
			var got int
			if _, err := conn.ExecContext(ctx, "BEGIN "+proc+"(:1); END;", sql.Out{Dest: &got}); err != nil {
				t.Fatal(err)
			} else if got != i {
				t.Errorf("procedure version %d returned %d", i, got)
			}
		}
	}
	for i := range 2 {
		exec("CREATE OR REPLACE PROCEDURE " + proc + " IS BEGIN NULL; END;")
		exec("DROP PROCEDURE " + proc)
		var oraErr *godror.OraErr
		if _, err := conn.ExecContext(ctx, "DROP PROCEDURE "+proc); !errors.As(err, &oraErr) || oraErr.Code() != 4043 {
			t.Errorf("%d. second DROP PROCEDURE: got %+v, wanted ORA-04043", i, err)
		}
	}
}

// The Options given as arguments of Exec apply to that execution, and only to
// that, though the next Exec of the same text reuses the statement.
func TestExecCacheOptions(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	t.Run("PlSQLArrays", func(t *testing.T) {
		const qry = `DECLARE
  TYPE num_tt IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  v num_tt := :1;
BEGIN
  :2 := v.COUNT;
END;`
		for i := range 2 {
			var n int
			if _, err := conn.ExecContext(ctx, qry, godror.PlSQLArrays, []int64{1, 2, 3}, sql.Out{Dest: &n}); err != nil {
				t.Fatalf("%d. %+v", i, err)
			}
			if n != 3 {
				t.Errorf("%d. got %d elements, wanted 3", i, n)
			}
		}
	})

	t.Run("BindTimeAsDate", func(t *testing.T) {
		const qry = "BEGIN SELECT DUMP(:1) INTO :2 FROM DUAL; END;"
		now := time.Now()
		for i, asDate := range []bool{false, true, true, false, false, true} {
			args := []any{now, sql.Out{Dest: new(string)}}
			if asDate {
				args = append(args, godror.BindTimeAsDate())
			}
			if _, err := conn.ExecContext(ctx, qry, args...); err != nil {
				t.Fatalf("%d. %+v", i, err)
			}
			dump := *args[1].(sql.Out).Dest.(*string)
			// DATE is type 12.
			if isDate := strings.HasPrefix(dump, "Typ=12 "); isDate != asDate {
				t.Errorf("%d. BindTimeAsDate=%t, the time was bound as %q", i, asDate, dump)
			}
		}
	})

	tbl := "test_exec_cache_opt" + tblSuffix
	cleanup := func() { _, _ = testDb.Exec("DROP TABLE " + tbl + " PURGE") }
	cleanup()
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(3) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	const ins = "INSERT INTO %s (id) VALUES (:1)"
	count := func(t *testing.T) (n int) {
		t.Helper()
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("PartialBatch", func(t *testing.T) {
		qry := fmt.Sprintf(ins, tbl)
		var oraErr *godror.OraErr
		// Without the option the batch fails as a whole,
		if _, err := conn.ExecContext(ctx, qry, []int{1, 2, 2, 3}); !errors.As(err, &oraErr) || oraErr.Code() != 1 {
			t.Fatalf("got %+v, wanted ORA-00001", err)
		}
		if n := count(t); n != 0 {
			t.Errorf("%d rows after the failed batch, wanted 0", n)
		}
		// with it the good rows are inserted,
		var batchErrs *godror.BatchErrors
		if _, err := conn.ExecContext(ctx, qry, godror.PartialBatch(), []int{1, 2, 2, 3}); !errors.As(err, &batchErrs) {
			t.Fatalf("got %T (%+v), wanted *BatchErrors", err, err)
		}
		if n := count(t); n != 3 {
			t.Errorf("%d rows after the partial batch, wanted 3", n)
		}
		// and the option does not stick.
		if _, err := conn.ExecContext(ctx, qry, []int{4, 5}); err != nil {
			t.Fatal(err)
		}
		batchErrs = nil
		if _, err := conn.ExecContext(ctx, qry, []int{6, 6, 7}); errors.As(err, &batchErrs) || !errors.As(err, &oraErr) || oraErr.Code() != 1 {
			t.Errorf("got %T (%+v), wanted ORA-00001", err, err)
		}
		if n := count(t); n != 5 {
			t.Errorf("%d rows, wanted 5", n)
		}
	})

	t.Run("ParseOnly", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatal(err)
		}
		qry := fmt.Sprintf(ins, tbl)
		want := 0
		for i, parseOnly := range []bool{true, false, true, true, false} {
			args := []any{10 + i}
			if parseOnly {
				args = append(args, godror.ParseOnly())
			} else {
				want++
			}
			if _, err := conn.ExecContext(ctx, qry, args...); err != nil {
				t.Fatalf("%d. %+v", i, err)
			}
			if n := count(t); n != want {
				t.Errorf("%d. ParseOnly=%t: %d rows, wanted %d", i, parseOnly, n, want)
			}
		}
	})
}
