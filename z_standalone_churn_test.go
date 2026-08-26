// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

// TestStandaloneChurn creates and releases standalone connections from many
// goroutines at once (database/sql with MaxIdleConns(0)), the pattern behind
// the glibc allocator aborts inside dpiConn_create reported in
// https://github.com/godror/godror/issues/361 and /issues/400.
//
// A native SIGABRT/SIGSEGV would kill the whole test binary, so the churn runs
// in a child process and its exit status is the verdict.
//
// Opt-in: GODROR_TEST_STANDALONE_CHURN=1; duration via
// GODROR_TEST_STANDALONE_CHURN_DURATION (default 1m); an unreachable
// connectString in GODROR_TEST_BAD_CONNECT_STRING adds a ping loop that models
// error-driven reconnects.
func TestStandaloneChurn(t *testing.T) {
	if os.Getenv("GODROR_TEST_STANDALONE_CHURN") == "" {
		t.Skip("set GODROR_TEST_STANDALONE_CHURN=1 to run (needs a database, ~1 minute)")
	}
	if os.Getenv("DO_TEST_STANDALONE_CHURN") == "1" {
		testStandaloneChurn(t)
		return
	}
	ex, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(ex, "-test.run=^TestStandaloneChurn$", "-test.v", "-test.timeout=10m")
	cmd.Env = append(os.Environ(), "DO_TEST_STANDALONE_CHURN=1", "GOTRACEBACK=crash")
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = os.Stdout, io.MultiWriter(os.Stderr, &stderr)
	if err = cmd.Run(); err != nil {
		tail := stderr.Bytes()
		if len(tail) > 4096 {
			tail = tail[len(tail)-4096:]
		}
		t.Fatalf("child process: %v\n%s", err, tail)
	}
}

func testStandaloneChurn(t *testing.T) {
	duration := time.Minute
	if s := os.Getenv("GODROR_TEST_STANDALONE_CHURN_DURATION"); s != "" {
		var err error
		if duration, err = time.ParseDuration(s); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(testContext("StandaloneChurn"), duration)
	defer cancel()

	P, err := dsn.Parse(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = sql.NullBool{Bool: true, Valid: true}
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	const workers = 32
	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(0) // every Conn.Close releases the native connection
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if u, err := user.LookupId(strconv.Itoa(os.Getuid())); err == nil {
		t.Logf("uid %d resolves to %q; the failed-user-lookup path of #361/#400 is not exercised, only generic churn", os.Getuid(), u.Username)
	}

	var wg sync.WaitGroup
	if bad := os.Getenv("GODROR_TEST_BAD_CONNECT_STRING"); bad != "" {
		bP := P
		bP.ConnectString = bad
		bdb := sql.OpenDB(godror.NewConnector(bP))
		defer bdb.Close()
		bdb.SetMaxOpenConns(1)
		bdb.SetMaxIdleConns(0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
				_ = bdb.PingContext(pctx) // expected to fail
				pcancel()
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}

	var ok, failed, rounds atomic.Int64
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		for ctx.Err() == nil {
			rounds.Add(1)
			start := make(chan struct{})
			var rwg sync.WaitGroup
			for i := 0; i < workers; i++ {
				rwg.Add(1)
				go func() {
					defer rwg.Done()
					<-start // release all workers at once: overlapping dpiConn_create
					qctx, qcancel := context.WithTimeout(ctx, 5*time.Second)
					defer qcancel()
					conn, err := db.Conn(qctx)
					if err != nil {
						if ctx.Err() == nil {
							failed.Add(1)
						}
						return
					}
					// column-count agnostic: some test targets answer with extra columns
					var rows *sql.Rows
					if rows, err = conn.QueryContext(qctx, "SELECT 1 FROM DUAL"); err == nil {
						rows.Next()
						err = rows.Err()
						_ = rows.Close()
					}
					if err != nil {
						if ctx.Err() == nil {
							failed.Add(1)
						}
					} else {
						ok.Add(1)
					}
					_ = conn.Close() // dpiConn_release
				}()
			}
			close(start)
			rwg.Wait()
		}
	}()
	// A server that stops answering the session release would park a worker in
	// cgo forever; bound the shutdown wait so the test reports instead of hanging.
	select {
	case <-churnDone:
	case <-time.After(duration + 90*time.Second):
		t.Logf("workers did not drain within 90s after the deadline (connection release stuck on the server?); continuing")
	}
	cancel()
	wg.Wait()
	t.Logf("rounds=%d ok=%d failed=%d", rounds.Load(), ok.Load(), failed.Load())
	if ok.Load() == 0 {
		t.Fatal("no successful round trip")
	}
}
