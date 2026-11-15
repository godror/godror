// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestQueuePurgeExpiredAfterClose checks that PurgeExpired on a closed Queue
// returns the same error as Enqueue, instead of a nil pointer dereference.
func TestQueuePurgeExpiredAfterClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	qName := strings.ToUpper("TEST_QPURGE" + tblSuffix)
	setUpRawQueue(ctx, t, testDb, qName)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q, err := godror.NewQueue(ctx, conn, qName, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = q.PurgeExpired(ctx); err != nil {
		t.Errorf("PurgeExpired on an open queue: %v", err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	closedErr := q.Enqueue([]godror.Message{{Raw: []byte("x")}})
	if closedErr == nil {
		t.Fatal("Enqueue on a closed queue: wanted error")
	}
	if err = q.PurgeExpired(ctx); !errors.Is(err, closedErr) {
		t.Errorf("PurgeExpired on a closed queue: got %v, wanted %v", err, closedErr)
	}
}
