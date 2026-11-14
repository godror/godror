// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestSuperShardingKeyIsPassed checks that SuperShardingKey reaches ODPI-C.
//
// There is no sharded database here, so it relies on ODPI-C's own check:
// a super sharding key without a sharding key is rejected with DPI-1073.
// If the super sharding key is dropped, the connection succeeds.
func TestSuperShardingKeyIsPassed(t *testing.T) {
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	ping := func(P godror.ConnectionParams) error {
		db := sql.OpenDB(godror.NewConnector(P))
		defer db.Close()
		return db.PingContext(ctx)
	}

	P.ShardingKey, P.SuperShardingKey = nil, []any{"gold"}
	if err := ping(P); err == nil {
		t.Error("super sharding key without sharding key: no error, so the super sharding key was dropped")
	} else if !strings.Contains(err.Error(), "DPI-1073") {
		t.Errorf("super sharding key without sharding key: wanted DPI-1073, got %v", err)
	}

	// A non-sharded database ignores both keys.
	P.ShardingKey, P.SuperShardingKey = []any{"silver", 42}, []any{"gold", []byte("bronze")}
	if err := ping(P); err != nil {
		t.Errorf("sharding key and super sharding key: %+v", err)
	}
}
