// Copyright 2026 The Godror Authors.
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/godror/godror"
)

// TestExecContextPanic tries to force a panic in stmt.bindVars
// by canceling the context during the execution of stmt.Exec.
//
// See https://github.com/godror/godror/issues/404 for details.
//
// You have to call it a lot! (go test -run=ExecContextPanic -count=100)
func TestExecContextPanic(t *testing.T) {
	ctx := t.Context()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	wait := 1 * time.Second
	for range 100 {
		dur, err := testExecContextPanicOne(t, conn, wait)
		if err != nil && godror.IsBadConn(err) {
			return
		}
		wait = dur - (dur >> 5)
	}
}

func testExecContextPanicOne(t *testing.T, conn *sql.Conn, wait time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	var syncWg, doneWg sync.WaitGroup
	var dur time.Duration
	var err error

	syncWg.Add(1)
	doneWg.Add(1)
	go func() {
		defer doneWg.Done()
		syncWg.Wait()

		start := time.Now()
		_, err = conn.ExecContext(ctx, "SELECT :param FROM dual", sql.Named("param", "value"))
		end := time.Now()
		dur = end.Sub(start)
		fmt.Fprintln(os.Stderr, end.Format(time.RFC3339Nano), "select done", dur.String())
		if err != nil && !errors.Is(err, context.Canceled) && !godror.IsBadConn(err) {
			if oerr, ok := godror.AsOraErr(err); !(ok && oerr.Code() == 1013) {
				t.Error(err)
			}
		}
	}()

	syncWg.Done()

	wait = (wait >> 5) + time.Duration(rand.Int63n(int64(wait-wait>>5)))
	time.Sleep(wait) // Might need to adjust offset and jitter to reproduce
	fmt.Fprintln(os.Stderr, "time", time.Now().Format(time.RFC3339Nano), "cancelling context, waited", wait.String())
	// Pull the rug
	cancel()
	doneWg.Wait()
	return dur, err
}
