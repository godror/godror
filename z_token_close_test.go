// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

// unsignedJWT returns a syntactically valid (but unsigned) JWT that expires at exp.
// The Oracle client reads the exp claim itself and calls the token callback
// once it has passed, before the server sees (and rejects) the token.
func unsignedJWT(exp time.Time) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." +
		enc([]byte(`{"sub":"godror-test","exp":`+strconv.FormatInt(exp.Unix(), 10)+`}`)) + "." +
		enc([]byte("sig"))
}

// TestTokenCallbackPoolClose closes the driver (and so purges the pool) while
// the token refresh callback is running for a session acquisition.
//
// The token callback context (C wrapper + cgo.Handle) must not be freed before
// the ODPI-C pool is closed, otherwise the C trampoline writes the refreshed
// token into freed memory when the callback returns.
//
// No token-capable database is needed: logins are expected to fail.
func TestTokenCallbackPoolClose(t *testing.T) {
	P, err := godror.ParseConnString(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.Username = ""
	P.Password.Reset()
	P.StandaloneConnection = godror.Bool(false)
	P.ExternalAuth = godror.Bool(true)
	expiry := time.Now().Add(2 * time.Second)
	P.Token = unsignedJWT(expiry)

	start := time.Now()
	var calls int32
	inCB := make(chan struct{})
	var once sync.Once
	cb := func(ctx context.Context, tok *dsn.AccessToken) error {
		atomic.AddInt32(&calls, 1)
		t.Logf("%s: callback start", time.Since(start))
		once.Do(func() { close(inCB) })
		time.Sleep(time.Second)
		tok.Token = unsignedJWT(time.Now().Add(time.Hour))
		t.Logf("%s: callback end", time.Since(start))
		return nil
	}
	P.PoolParams = godror.PoolParams{
		MinSessions: 0, MaxSessions: 2, SessionIncrement: 1,
		WaitTimeout: 5 * time.Second, MaxLifeTime: time.Minute, SessionTimeout: time.Minute,
		TokenCB: cb, TokenCBCtx: context.Background(),
	}
	db := sql.OpenDB(godror.NewDriver().NewConnector(P))
	db.SetMaxIdleConns(0)

	// Create the pool while the token is still valid (the login itself fails).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = db.PingContext(ctx)
	cancel()
	t.Logf("%s: first ping (creates pool): %.100v", time.Since(start), err)
	if n := atomic.LoadInt32(&calls); n != 0 {
		db.Close()
		t.Skipf("token callback called during pool creation (%d)", n)
	}
	time.Sleep(time.Until(expiry) + time.Second)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- db.PingContext(ctx)
	}()
	select {
	case <-inCB:
	case err := <-done:
		db.Close()
		t.Skipf("token callback not invoked (ping: %.100v)", err)
	case <-time.After(15 * time.Second):
		db.Close()
		t.Skip("token callback not invoked")
	}
	// The callback is running now: close the driver under it.
	t.Logf("%s: db.Close start", time.Since(start))
	if err := db.Close(); err != nil {
		t.Log("close:", err)
	}
	t.Logf("%s: db.Close end", time.Since(start))
	err = <-done
	t.Logf("%s: ping returned; callbacks: %d; err: %.100v", time.Since(start), atomic.LoadInt32(&calls), err)
}
