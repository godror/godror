// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"os"
	"testing"

	"github.com/godror/godror/dsn"
)

// TestPoolPingInterval checks that pingInterval (also under its documented
// name, poolPingInterval) and poolMaxSessionsPerShard reach the pool.
func TestPoolPingInterval(t *testing.T) {
	s := os.Getenv("GODROR_TEST_DSN")
	if s == "" {
		t.Skip("GODROR_TEST_DSN is not set")
	}
	base, err := dsn.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDriver()
	defer d.Close()
	for _, tC := range []struct {
		param string
		want  int
	}{
		{"", 60}, // ODPI-C's default
		{"pingInterval=7s", 7},
		{"poolPingInterval=9s", 9},
		{"pingInterval=500ms", 0},
		{"pingInterval=-1s", -1},
		{"pingInterval=-1ms", -1},
		{"pingInterval=11s poolMaxSessionsPerShard=2", 11},
	} {
		cp, err := dsn.Parse("user=u password=p connectString=h/s standaloneConnection=0 " + tC.param)
		if err != nil {
			t.Fatalf("%q: %v", tC.param, err)
		}
		P := commonAndPoolParams{CommonParams: base.CommonParams, PoolParams: base.PoolParams}
		P.MinSessions, P.MaxSessions, P.SessionIncrement = 0, 2, 1
		P.PingInterval, P.MaxSessionsPerShard = cp.PingInterval, cp.MaxSessionsPerShard
		p, err := d.getPool(P)
		if err != nil {
			t.Fatalf("%q: getPool: %v", tC.param, err)
		}
		if got := int(p.dpiPool.pingInterval); got != tC.want {
			t.Errorf("%q: the pool has pingInterval=%d, wanted %d", tC.param, got, tC.want)
		}
	}
}
