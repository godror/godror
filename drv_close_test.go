// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

func closeTestParams(t *testing.T) commonAndPoolParams {
	s := os.Getenv("GODROR_TEST_DSN")
	if s == "" {
		t.Skip("GODROR_TEST_DSN is not set")
	}
	cp, err := dsn.Parse(s)
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
