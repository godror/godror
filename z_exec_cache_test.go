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
