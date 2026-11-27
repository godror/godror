// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A late BREAK must not leak into the next operation in an open transaction.
func TestBreakOnIdleSession(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// The savepoint also checks that recovery preserves the transaction.
	if _, err := tx.ExecContext(ctx, "SAVEPOINT before_break"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(dc any) error {
		godror.BreakIdleSessionForTest(dc.(godror.Conn))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO before_break"); err != nil {
		t.Fatalf("first call after late BREAK: %v", err)
	}
}

// TestBreakAfterCancelDoesNotHitNextCall checks that the BREAK sent on
// context cancelation does not cancel the next call on the same connection,
// when the cancelation races with the end of the execution.
//
// The race is narrow: run it a few times, with -race to see the data races too.
func TestBreakAfterCancelDoesNotHitNextCall(t *testing.T) {
	ctx, cancel := testContext(t, 10*time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	const qry = "BEGIN DBMS_SESSION.SLEEP(0.01); END;"
	// Measure how long the execution takes without a deadline.
	var base time.Duration
	for range 5 {
		start := time.Now()
		if _, err := conn.ExecContext(ctx, qry); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); base == 0 || d < base {
			base = d
		}
	}
	t.Logf("execution takes %v", base)

	// Each canceled execution leaves a cursor open on the server
	// (a separate issue), so stay well below OPEN_CURSORS.
	const rounds = 600
	var canceled, spurious, badConn int
	for i := range rounds {
		// Aim the deadline at the end of the execution, to hit the window
		// where the execution has finished but the BREAK is still sent.
		d := base - time.Millisecond + time.Duration(i%30)*50*time.Microsecond
		sctx, scancel := context.WithTimeout(ctx, d)
		_, err := conn.ExecContext(sctx, qry)
		scancel()
		if err != nil {
			canceled++
		}
		var n int
		if err = conn.QueryRowContext(context.Background(), "SELECT 1 FROM DUAL").Scan(&n); err == nil {
			continue
		}
		if oe, ok := godror.AsOraErr(err); ok && oe.Code() == 1013 {
			spurious++
			t.Logf("round %d (timeout %v): follow-up got %v", i, d, err)
			continue
		}
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("round %d: follow-up: %+v", i, err)
		}
		// Sometimes the BREAK leaves the session unusable; that is not what
		// this test is about, so continue on a new connection.
		badConn++
		t.Logf("round %d (timeout %v): follow-up got %v", i, d, err)
		conn.Close()
		if conn, err = testDb.Conn(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%d rounds, %d canceled executions, %d bad connections, %d spurious ORA-01013",
		rounds, canceled, badConn, spurious)
	if spurious != 0 {
		t.Errorf("%d follow-up calls got ORA-01013 from a late BREAK", spurious)
	}
}
