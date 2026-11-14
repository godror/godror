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
			db := sql.OpenDB(godror.NewConnector(P))
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
