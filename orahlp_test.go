// Copyright 2017, 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	godror "github.com/godror/godror"
	"github.com/google/go-cmp/cmp"
)

func TestInt64ConvertValueRange(t *testing.T) {
	for _, v := range []any{
		uint64(1 << 63), uint64(math.MaxUint64),
		float32(1 << 63), math.Nextafter32(-1<<63, float32(math.Inf(-1))),
		float64(1 << 63), math.Nextafter(-1<<63, math.Inf(-1)),
	} {
		if got, err := godror.Int64.ConvertValue(v); !errors.Is(err, strconv.ErrRange) {
			t.Errorf("ConvertValue(%T(%v)) = %v, %v; want range error", v, v, got, err)
		}
	}
	for _, tc := range []struct {
		in   any
		want int64
	}{
		{uint64(math.MaxInt64), math.MaxInt64},
		{float64(math.MinInt64), math.MinInt64},
		{math.Nextafter(1<<63, 0), 1<<63 - 1024},
		{float32(math.MinInt64), math.MinInt64},
		{math.Nextafter32(1<<63, 0), 1<<63 - 1<<39},
	} {
		if got, err := godror.Int64.ConvertValue(tc.in); got != tc.want || err != nil {
			t.Errorf("ConvertValue(%T(%v)) = %v, %v; want %d, nil", tc.in, tc.in, got, err, tc.want)
		}
	}
}

func TestMapToSlice(t *testing.T) {
	for i, tc := range []struct {
		in, await string
		params    []any
	}{
		{
			`SELECT NVL(MAX(F_dazon), :dazon) FROM T_spl_level
			WHERE (to_char(CURRENT_DATE, 'HH:MM') = '00:42' AND F_spl_azon = :lev_azon OR --:lev_azon OR
			       F_ssz = 0 AND F_lev_azon = /*:lev_azon*/:lev_azon)`,
			`SELECT NVL(MAX(F_dazon), :1) FROM T_spl_level
			WHERE (to_char(CURRENT_DATE, 'HH:MM') = '00:42' AND F_spl_azon = :2 OR --:lev_azon OR
			       F_ssz = 0 AND F_lev_azon = /*:lev_azon*/:3)`,
			[]any{"dazon", "lev_azon", "lev_azon"},
		},

		{
			`INSERT INTO PERSON(NAME) VALUES('hello') RETURNING ID INTO :ID`,
			`INSERT INTO PERSON(NAME) VALUES('hello') RETURNING ID INTO :1`,
			[]any{"ID"},
		},

		{
			`DECLARE
  i1 PLS_INTEGER;
  i2 PLS_INTEGER;
  v001 BRUNO.DB_WEB_ELEKTR.KOTVENY_REC_TYP;

BEGIN
  v001.dijkod := :p002#dijkod;

  DB_web.sendpreoffer_31101(p_kotveny=>v001);

  :p002#dijkod := v001.dijkod;

END;
`,
			`DECLARE
  i1 PLS_INTEGER;
  i2 PLS_INTEGER;
  v001 BRUNO.DB_WEB_ELEKTR.KOTVENY_REC_TYP;

BEGIN
  v001.dijkod := :1;

  DB_web.sendpreoffer_31101(p_kotveny=>v001);

  :2 := v001.dijkod;

END;
`,
			[]any{"p002#dijkod", "p002#dijkod"},
		},
	} {

		got, params := godror.MapToSlice(tc.in, func(s string) any { return s })
		d := cmp.Diff(tc.await, got)
		if d != "" {
			t.Errorf("%d. diff:\n%s", i, d)
		}
		if !reflect.DeepEqual(params, tc.params) {
			t.Errorf("%d. params: got\n\t%#v,\nwanted\n\t%#v.", i, params, tc.params)
		}
	}
}

// Only connection acquisition and release are exercised by the pool test.
type poolTestConnector struct{}

func (poolTestConnector) Connect(context.Context) (driver.Conn, error) { return poolTestConn{}, nil }
func (poolTestConnector) Driver() driver.Driver                        { return poolTestConnector{} }
func (poolTestConnector) Open(string) (driver.Conn, error)             { return poolTestConn{}, nil }

type poolTestConn struct{}

func (poolTestConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (poolTestConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (poolTestConn) Close() error                        { return nil }

func TestConnPoolWaitingBorrower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := sql.OpenDB(poolTestConnector{})
		defer db.Close()
		db.SetMaxOpenConns(1)
		pool := godror.NewConnPool(db, 1)
		defer pool.Close()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		first, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()

		borrow := func(ctx context.Context) <-chan error {
			done := make(chan error, 1)
			go func() {
				conn, err := pool.Conn(ctx)
				if err == nil {
					err = conn.Close()
				}
				done <- err
			}()
			return done
		}
		waiter := borrow(ctx)
		cancelCtx, cancelBorrow := context.WithCancel(ctx)
		canceled := borrow(cancelCtx)
		synctest.Wait()
		if n := db.Stats().WaitCount; n != 2 {
			t.Fatalf("database/sql wait count = %d, want 2", n)
		}
		cancelBorrow()
		if err := <-canceled; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled borrower: got %v, want context.Canceled", err)
		}

		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-waiter; err != nil {
			t.Fatalf("waiting borrower after first connection was returned: %v", err)
		}
		if n := db.Stats().InUse; n != 1 {
			t.Errorf("after the last borrower returned: %d connections retained, want 1", n)
		}
	})
}

func TestConnPool(t *testing.T) {
	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.MinSessions, cs.MaxSessions = 3, 3
	db := sql.OpenDB(godror.NewConnector(cs))
	defer db.Close()

	ctx, cancel := testContext(t, 10*time.Second)
	defer cancel()

	p := godror.NewConnPool(db, 2)
	defer p.Close()

	var savedC1 *sql.Conn
	func() {
		c1, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = c1.Close(); err != nil {
			t.Fatal(err)
		}
		if c1, err = p.Conn(ctx); err != nil {
			t.Fatal(err)
		}
		if err = c1.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		savedC1 = c1.Conn
		c2, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		if c1 == c2 {
			t.Fatal("c1==c2")
		}

		old := c2.Conn
		if err := c2.Close(); err != nil {
			t.Fatal(err)
		}
		if c2, err = p.Conn(ctx); err != nil {
			t.Fatal(err)
		}
		if c2.Conn != old {
			t.Errorf("re-acquire got %p wanted %p", c2.Conn, old)
		}
		defer c2.Close()
		old = nil
		_ = old

		c3, err := p.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c3.Close()
		if err = c3.PingContext(ctx); err != nil {
			t.Error(err)
		}
	}()

	if err := savedC1.PingContext(ctx); err != nil {
		t.Error(err)
	}
	savedC1.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := savedC1.PingContext(ctx); err == nil {
		t.Log("connection should be closed after the pool is closed, but works")
	}
}
