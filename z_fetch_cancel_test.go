// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A fetch must stop when the context of the query is canceled,
// for a nested cursor, too; and the BREAK must not hit the next call.
func TestFetchCancel(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()
	// A DB of its own, with fresh sessions: every canceled fetch leaves
	// a cursor open on its session, which must not stay in the shared pool.
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	db := sql.OpenDB(godror.NewDriver().NewConnector(P))
	defer db.Close()
	fn := "test_fetch_cancel" + tblSuffix
	if _, err := db.ExecContext(ctx, "CREATE OR REPLACE FUNCTION "+fn+"(n IN NUMBER) RETURN NUMBER IS BEGIN DBMS_SESSION.SLEEP(n); RETURN n; END;"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP FUNCTION "+fn)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { conn.Close() }()
	// followUp reports whether the connection is usable after a canceled fetch.
	var badConn int
	followUp := func(name string) {
		var n int
		err := conn.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&n)
		if err == nil {
			return
		}
		if !errors.Is(err, driver.ErrBadConn) {
			t.Errorf("%s: follow-up: %v", name, err)
		}
		// Sometimes the BREAK leaves the session unusable.
		badConn++
		conn.Close()
		if conn, err = db.Conn(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The execute fetches nothing, so it is the first fetch that is slow.
	opts := []any{godror.PrefetchCount(0), godror.FetchArraySize(30)}

	// 30 rows * 0.1 s = 3 s for the fetch of one array.
	slow := "SELECT " + fn + "(0.1) FROM dual CONNECT BY level <= 30"
	for _, tc := range []struct {
		name, qry string
		deadline  bool
		want      error
	}{
		{"cancel", slow, false, context.Canceled},
		{"nested, cancel", "SELECT CURSOR(" + slow + ") FROM dual", false, context.Canceled},
		{"nested, deadline", "SELECT CURSOR(" + slow + ") FROM dual", true, nil},
	} {
		func() {
			qctx, qcancel := context.WithCancel(ctx)
			if tc.deadline {
				qctx, qcancel = context.WithTimeout(ctx, 500*time.Millisecond)
			}
			defer qcancel()
			rows, err := conn.QueryContext(qctx, tc.qry, opts...)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			defer rows.Close()
			if !tc.deadline {
				time.AfterFunc(500*time.Millisecond, qcancel)
			}
			start := time.Now()
			sub := rows
			if rows.Next() && tc.qry != slow {
				sub = new(sql.Rows)
				if err = rows.Scan(sub); err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				sub.Next()
			}
			d := time.Since(start)
			err = sub.Err()
			t.Logf("%s: fetch returned %v after %s", tc.name, err, d)
			if d > 2*time.Second {
				t.Errorf("%s: the fetch took %s, the context was done after 0.5s", tc.name, d)
			}
			if err == nil {
				t.Errorf("%s: no error", tc.name)
			} else if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("%s: got %v, wanted %v", tc.name, err, tc.want)
			}
		}()
		followUp(tc.name)
	}

	// Cancel at about the end of the fetch: a late BREAK must not hit
	// the follow-up. (Each canceled call leaves a cursor open on the
	// server, a separate issue, so keep the rounds few.)
	fast := "SELECT " + fn + "(0.01) FROM dual"
	var canceled int
	for i := range 100 {
		func() {
			qctx, qcancel := context.WithCancel(ctx)
			defer qcancel()
			rows, err := conn.QueryContext(qctx, fast, opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			time.AfterFunc(10*time.Millisecond+time.Duration(i%40)*50*time.Microsecond, qcancel)
			if rows.Next(); rows.Err() != nil {
				canceled++
			}
		}()
		followUp("late BREAK")
	}
	t.Logf("%d canceled fetches of 100, %d bad connections", canceled, badConn)
}
