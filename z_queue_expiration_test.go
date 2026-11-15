// Copyright 2026 The Godror Authors
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

// TestQueueZeroExpiration checks that a message enqueued with the zero
// Message.Expiration never expires, and that a sub-second Expiration
// is not truncated to "expire immediately".
func TestQueueZeroExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	qName := strings.ToUpper("TEST_QEXP" + tblSuffix)
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
	defer q.Close()

	if err = q.Enqueue([]godror.Message{
		{Raw: []byte("a"), Correlation: "zero"},
		{Raw: []byte("b"), Correlation: "subsecond", Expiration: 500 * time.Millisecond},
		{Raw: []byte("c"), Correlation: "hour", Expiration: time.Hour},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}

	// What the database has stored: NULL means the message never expires.
	rows, err := conn.QueryContext(ctx, "SELECT corr_id, expiration FROM AQ$"+qName+"_TBL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]sql.NullInt64)
	for rows.Next() {
		var corr string
		var exp sql.NullInt64
		if err = rows.Scan(&corr, &exp); err != nil {
			t.Fatal(err)
		}
		got[corr] = exp
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	for corr, want := range map[string]sql.NullInt64{
		"zero":      {},
		"subsecond": {Int64: 1, Valid: true},
		"hour":      {Int64: 3600, Valid: true},
	} {
		if got[corr] != want {
			t.Errorf("%s: stored expiration is %+v, wanted %+v", corr, got[corr], want)
		}
	}

	// Give the queue's time manager a chance to expire the messages.
	time.Sleep(3 * time.Second)
	msgs := make([]godror.Message, 3)
	n, err := q.Dequeue(msgs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, m := range msgs[:n] {
		if m.Correlation == "zero" {
			found = true
			if m.Expiration != 0 {
				t.Errorf("dequeued Expiration is %s, wanted 0", m.Expiration)
			}
		}
	}
	if !found {
		t.Errorf("the message with zero Expiration has expired (dequeued %d messages)", n)
	}
}
