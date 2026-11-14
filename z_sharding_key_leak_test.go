// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"database/sql"
	"runtime"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestShardingKeyConnect connects with string, []byte and int sharding key
// values. A non-sharded database accepts (and ignores) the sharding key of
// a standalone connection.
//
// The C copies of the string and []byte values used to leak on every
// connect. To see that, run it alone with -asan and leak detection on:
// LeakSanitizer then reports connects x 1 object of 78 bytes (the string)
// and of 61 bytes (the []byte).
func TestShardingKeyConnect(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	P.ShardingKey = []any{strings.Repeat("s", 77), bytes.Repeat([]byte{'b'}, 61), 42}

	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	const connects = 100
	for range connects {
		db := sql.OpenDB(godror.NewConnector(P))
		err := db.PingContext(ctx)
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	// LeakSanitizer scans the Go heap too, and dead Go objects keep
	// their pointers until reused: reuse them, so a leak shows up.
	runtime.GC()
	var keep []*[2]uintptr
	for range 1 << 20 {
		keep = append(keep, new([2]uintptr))
	}
	runtime.KeepAlive(keep)
}
