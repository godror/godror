// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestStaleCallTimeout checks that a call timeout set from a context deadline
// does not stick to the connection and break a later call without a deadline.
func TestStaleCallTimeout(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const sleepQry = "BEGIN DBMS_SESSION.SLEEP(2); END;"
	const sleepSelect = "WITH FUNCTION slp RETURN NUMBER IS BEGIN DBMS_SESSION.SLEEP(2); RETURN 1; END; SELECT slp FROM DUAL"

	first := map[string]func(context.Context, *sql.Conn) error{
		"ExecOK": func(ctx context.Context, c *sql.Conn) error {
			_, err := c.ExecContext(ctx, "BEGIN NULL; END;")
			return err
		},
		"ExecErr": func(ctx context.Context, c *sql.Conn) error {
			_, _ = c.ExecContext(ctx, "BEGIN RAISE_APPLICATION_ERROR(-20001, 'x'); END;")
			return nil
		},
		"Query": func(ctx context.Context, c *sql.Conn) error {
			var n int
			return c.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&n)
		},
	}
	second := map[string]func(context.Context, *sql.Conn) error{
		"Exec": func(ctx context.Context, c *sql.Conn) error {
			_, err := c.ExecContext(ctx, sleepQry)
			return err
		},
		"Query": func(ctx context.Context, c *sql.Conn) error {
			var n int
			return c.QueryRowContext(ctx, sleepSelect).Scan(&n)
		},
	}
	for fName, f := range first {
		for sName, s := range second {
			t.Run(fName+"-"+sName, func(t *testing.T) {
				conn, err := testDb.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()

				shortCtx, shortCancel := context.WithTimeout(ctx, time.Second)
				err = f(shortCtx, conn)
				shortCancel()
				if err != nil {
					t.Fatalf("first (%s): %+v", fName, err)
				}

				// No deadline: must not inherit the 1s call timeout.
				start := time.Now()
				if err := s(context.Background(), conn); err != nil {
					t.Errorf("second (%s) after %s: %+v", sName, time.Since(start), err)
				}
			})
		}
	}
}
