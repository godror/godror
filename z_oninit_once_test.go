package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestOnInitOnce(t *testing.T) {
	for i, tc := range []struct {
		name                  string
		standalone, initOnNew bool
	}{
		{"standalone", true, false},
		{"pooled", false, false},
		{"pooledInitOnNewConn", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := testContext(t, time.Minute)
			defer cancel()
			P, err := godror.ParseDSN(testConStr)
			if err != nil {
				t.Fatal(err)
			}
			P.StandaloneConnection = godror.Bool(tc.standalone)
			P.InitOnNewConn = tc.initOnNew
			// A pool of its own with one session, so the second
			// connection reuses the session of the first.
			P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 1, 1
			P.SessionTimeout = time.Duration(4200+i) * time.Second
			var n atomic.Int32
			P.OnInit = func(context.Context, driver.ConnPrepareContext) error {
				n.Add(1)
				return nil
			}
			// A driver of its own, so no pool is left for the next run.
			drv := godror.NewDriver()
			defer drv.Close()
			db := sql.OpenDB(drv.NewConnector(P))
			defer db.Close()
			// Close the driver conn on release, returning the session to the pool.
			db.SetMaxIdleConns(0)

			connect := func() {
				t.Helper()
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if err = conn.PingContext(ctx); err != nil {
					t.Fatal(err)
				}
			}
			connect()
			if got := n.Load(); got != 1 {
				t.Errorf("OnInit ran %d times for the first connection, wanted 1", got)
			}
			connect()
			want := int32(2)
			if tc.initOnNew {
				want = 1 // the session is reused
			}
			if got := n.Load(); got != want {
				t.Errorf("OnInit ran %d times after the second connection, wanted %d", got, want)
			}
		})
	}
}

// TestOnInitFailedSessionNotReused checks that a pooled session whose OnInit
// failed (here: the caller's context was canceled) is not handed out later
// as an initialized, not new session.
func TestOnInitFailedSessionNotReused(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	P.InitOnNewConn = true
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 1, 1
	P.SessionTimeout = 4210 * time.Second // a pool of its own
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	var calls atomic.Int32
	P.OnInit = func(ctx context.Context, cx driver.ConnPrepareContext) error {
		if calls.Add(1) == 1 {
			// The request that asked for the connection is gone.
			cancelFirst()
		}
		st, err := cx.PrepareContext(ctx, `ALTER SESSION SET NLS_DATE_FORMAT = '"INIT"YYYY'`)
		if err != nil {
			return err
		}
		defer st.Close()
		_, err = st.(driver.StmtExecContext).ExecContext(ctx, nil)
		return err
	}
	drv := godror.NewDriver()
	defer drv.Close()
	db := sql.OpenDB(drv.NewConnector(P))
	defer db.Close()
	db.SetMaxIdleConns(0)

	if conn, err := db.Conn(firstCtx); err == nil {
		conn.Close()
		t.Fatal("the first connection succeeded, though its context was canceled in OnInit")
	}
	for i := range 3 {
		var s string
		if err := db.QueryRowContext(ctx, "SELECT TO_CHAR(SYSDATE) FROM DUAL").Scan(&s); err != nil {
			t.Fatal(err)
		}
		if len(s) < 4 || s[:4] != "INIT" {
			t.Errorf("%d. the session has not been initialized: TO_CHAR(SYSDATE)=%q (OnInit calls: %d)", i, s, calls.Load())
		}
	}
}
