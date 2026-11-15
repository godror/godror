// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestDriverConnNoGlobalLock checks that a DriverConn call that waits for a
// free connection does not block DriverConn on a transaction that holds one.
func TestDriverConnNoGlobalLock(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	db, err := sql.Open("godror", testConStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// This one has to wait for the transaction's connection.
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()
	waitDone := make(chan error, 1)
	go func() {
		_, err := godror.DriverConn(waitCtx, db)
		waitDone <- err
	}()
	time.Sleep(300 * time.Millisecond)

	txDone := make(chan error, 1)
	go func() {
		_, err := godror.DriverConn(ctx, tx)
		if err == nil {
			err = tx.Commit()
		}
		txDone <- err
	}()
	select {
	case err := <-txDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("DriverConn(ctx, tx) is blocked by DriverConn(ctx, db) that waits for the transaction's connection")
		waitCancel()
		<-txDone
	}
	waitCancel()
	<-waitDone
}
