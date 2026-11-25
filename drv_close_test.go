// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

var testConStr = sync.OnceValue(func() string {
	if dsn := os.Getenv("GODROR_TEST_DSN"); dsn != "" {
		return dsn
	}
	return dsn.DefaultTestDSN
})

func closeTestParams(t *testing.T) commonAndPoolParams {
	cp, err := dsn.Parse(testConStr())
	if err != nil {
		t.Fatal(err)
	}
	return commonAndPoolParams{CommonParams: cp.CommonParams, PoolParams: cp.PoolParams}
}

// TestDriverCloseSlowPurge checks that Close survives purging taking longer
// than its timeout: the purge goroutine must not send on a closed channel.
func TestDriverCloseSlowPurge(t *testing.T) {
	P := closeTestParams(t)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 2, 2, 1

	old := driverCloseTimeout
	driverCloseTimeout = time.Nanosecond
	defer func() { driverCloseTimeout = old }()

	d := NewDriver()
	if _, err := d.getPool(P); err != nil {
		t.Skipf("getPool: %+v", err)
	}
	t.Log("Close:", d.Close())
	// Give the purge goroutine time to finish (and panic, if it would).
	time.Sleep(2 * time.Second)
}

// TestGetPoolDuringDriverClose checks that getPool doesn't write to the pools
// map after Close has set it to nil.
func TestGetPoolDuringDriverClose(t *testing.T) {
	P := closeTestParams(t)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 0, 1, 1
	for i := range 200 {
		d := NewDriver()
		if err := d.init(P.ConfigDir, P.LibDir); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("round %d: getPool panicked: %v", i, r)
				}
			}()
			_, _ = d.getPool(P)
		}()
		time.Sleep(time.Duration(i%20) * 100 * time.Microsecond)
		_ = d.Close()
		wg.Wait()
		if t.Failed() {
			return
		}
	}
}

// TestDriverCloseUninitialized checks that closing a Driver that
// has no dpiContext (never used, or closed already) is not an error.
func TestDriverCloseUninitialized(t *testing.T) {
	cp, err := dsn.Parse(testConStr())
	if err != nil {
		t.Skip(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := NewDriver().Close(); err != nil {
		t.Errorf("Close of a never used Driver: %v", err)
	}

	d := NewDriver()
	if err := sql.OpenDB(d.NewConnector(cp)).Close(); err != nil {
		t.Errorf("Close of a never used sql.DB: %v", err)
	}

	db := sql.OpenDB(d.NewConnector(cp))
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %+v", cp, err)
	}
	if err := db.Close(); err != nil {
		t.Error(err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("Driver.Close after DB.Close: %v", err)
	}
}
