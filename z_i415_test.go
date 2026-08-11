package godror_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

// TestPoolLeakOnCancel reproduces the ORA-24496 pool leak: a query cancelled
// mid-execute must not pin its pooled session.
func TestPoolLeakOnCancel(t *testing.T) {
	P, err := dsn.Parse(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	P.PoolParams.MinSessions, P.PoolParams.MaxSessions = 2, 4
	P.PoolParams.WaitTimeout = 3 * time.Second

	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxOpenConns(P.PoolParams.MaxSessions)

	ctx, cancel := context.WithTimeout(testContext("PoolLeakOnCancel"), time.Minute)
	defer cancel()

	probe := func(ctx context.Context) error {
		var n int
		return db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&n)
	}
	if err := probe(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cctx, ccancel := context.WithTimeout(ctx, time.Duration(1+i%20)*time.Millisecond)
			defer ccancel()
			_, _ = db.ExecContext(cctx, "BEGIN DBMS_SESSION.SLEEP(1); END;")
		}(i)
	}
	wg.Wait()

	pctx, pcancel := context.WithTimeout(ctx, 15*time.Second)
	defer pcancel()
	for i := 0; i < 3; i++ {
		if err := probe(pctx); err != nil {
			t.Fatalf("pool did not recover after cancelled queries: %+v", err)
		}
	}
}
