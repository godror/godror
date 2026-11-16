// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestQueueDedicatedConn checks that a Queue made from an *sql.DB works on
// a connection of its own, which the DB does not give to others till
// Queue.Close, and that a Queue made from an *sql.Conn or *sql.Tx
// still works on the caller's connection.
func TestQueueDedicatedConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 120*time.Second)
	defer cancel()
	qName := strings.ToUpper("TEST_QDEDIC" + tblSuffix)
	setUpRawQueue(ctx, t, testDb, qName)

	enqImm := godror.WithEnqOptions(godror.EnqOptions{Visibility: godror.VisibleImmediate, DeliveryMode: godror.DeliverPersistent})
	deqImm := godror.WithDeqOptions(godror.DeqOptions{Mode: godror.DeqRemove, Visibility: godror.VisibleImmediate, DeliveryMode: godror.DeliverPersistent, Navigation: godror.NavFirst})
	onCommit := godror.EnqOptions{Visibility: godror.VisibleOnCommit, DeliveryMode: godror.DeliverPersistent}

	for _, standalone := range []bool{true, false} {
		name := "pooled"
		if standalone {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) {
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.StandaloneConnection = godror.Bool(standalone)
			// A DB of our own, to see its Stats.
			const maxConns = 3
			db := sql.OpenDB(godror.NewConnector(P))
			defer db.Close()
			db.SetMaxOpenConns(maxConns)
			db.SetMaxIdleConns(maxConns)

			// messagesSeen returns the number of messages each of the n
			// connections sees in the queue table, uncommitted ones included.
			// It holds all n connections at the same time.
			messagesSeen := func(n int) []int {
				t.Helper()
				counts := make([]int, n)
				for i := range counts {
					conn, err := db.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					if err = conn.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+qName+"_TBL").Scan(&counts[i]); err != nil {
						t.Fatal(err)
					}
				}
				return counts
			}
			inUse := func(when string, want int) {
				t.Helper()
				if got := db.Stats().InUse; got != want {
					t.Errorf("%s: the DB has %d connections in use, wanted %d", when, got, want)
				}
			}
			dequeue := func(q *godror.Queue) []string {
				t.Helper()
				msgs := make([]godror.Message, 4)
				n, err := q.Dequeue(msgs)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, m := range msgs[:n] {
					got = append(got, string(m.Raw))
				}
				return got
			}

			q, err := godror.NewQueue(ctx, db, qName, "", enqImm, deqImm)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			inUse("after NewQueue(db)", 1)

			// Round trip.
			if err = q.Enqueue([]godror.Message{{Raw: []byte("one")}}); err != nil {
				t.Fatal(err)
			}
			if got := dequeue(q); len(got) != 1 || got[0] != "one" {
				t.Errorf("dequeued %q, wanted the one message", got)
			}

			// An uncommitted message is seen only in the session of the Queue:
			// no other user of the DB may get that session.
			if err = q.EnqueueWithOptions([]godror.Message{{Raw: []byte("pending")}}, &onCommit); err != nil {
				t.Fatal(err)
			}
			for i, n := range messagesSeen(maxConns - 1) {
				if n != 0 {
					t.Errorf("connection %d of the DB sees %d uncommitted message of the open Queue", i, n)
				}
			}
			inUse("with the Queue open", 1)

			// Close gives the connection back, once, without the uncommitted message.
			if err = q.Close(); err != nil {
				t.Error(err)
			}
			inUse("after Queue.Close", 0)
			if err = q.Close(); err != nil {
				t.Error(err)
			}
			inUse("after the second Queue.Close", 0)
			for i, n := range messagesSeen(maxConns) {
				if n != 0 {
					t.Errorf("connection %d of the DB sees %d message after Queue.Close", i, n)
				}
			}

			// A failing NewQueue gives the connection back, too.
			_, err = godror.NewQueue(ctx, db, qName, "TEST_QDEDIC_NO_SUCH_TYPE")
			var oraErr *godror.OraErr
			if !errors.As(err, &oraErr) {
				t.Fatalf("NewQueue with a nonexistent payload type: got %v, wanted an ORA- error", err)
			}
			t.Log("NewQueue with a nonexistent payload type:", oraErr.Code())
			inUse("after the failed NewQueue(db)", 0)

			// A Queue on an *sql.Conn uses that connection, and leaves it open.
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err = godror.NewQueue(ctx, conn, qName, "TEST_QDEDIC_NO_SUCH_TYPE"); !errors.As(err, &oraErr) {
				t.Fatalf("NewQueue with a nonexistent payload type: got %v, wanted an ORA- error", err)
			}
			qc, err := godror.NewQueue(ctx, conn, qName, "", deqImm)
			if err != nil {
				t.Fatal(err)
			}
			defer qc.Close()
			inUse("after NewQueue(conn)", 1)
			if err = qc.Enqueue([]godror.Message{{Raw: []byte("conn")}}); err != nil {
				t.Fatal(err)
			}
			if err = qc.Close(); err != nil {
				t.Error(err)
			}
			inUse("after Close of the Queue of conn", 1)
			// The caller commits, after the Close, too.
			if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
				t.Fatal(err)
			}
			if err = conn.Close(); err != nil {
				t.Error(err)
			}

			// A Queue on an *sql.Tx is part of that transaction.
			for _, commit := range []bool{false, true} {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				qt, err := godror.NewQueue(ctx, tx, qName, "")
				if err != nil {
					t.Fatal(err)
				}
				defer qt.Close()
				inUse("after NewQueue(tx)", 1)
				if err = qt.Enqueue([]godror.Message{{Raw: []byte("tx")}}); err != nil {
					t.Fatal(err)
				}
				if err = qt.Close(); err != nil {
					t.Error(err)
				}
				if commit {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
				}
				inUse("after the end of the transaction", 0)
			}

			// What was committed: the message of conn, and of the committed tx.
			q2, err := godror.NewQueue(ctx, db, qName, "", enqImm, deqImm)
			if err != nil {
				t.Fatal(err)
			}
			defer q2.Close()
			if got := dequeue(q2); len(got) != 2 || got[0] != "conn" || got[1] != "tx" {
				t.Errorf("dequeued %q, wanted [conn tx]", got)
			}
			if err = q2.Close(); err != nil {
				t.Error(err)
			}
			inUse("at the end", 0)
		})
	}
}
