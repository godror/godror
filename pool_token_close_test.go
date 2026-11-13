// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"encoding/base64"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

// TestPoolCloseReleasesTokenCallback checks that connPool.Close frees the
// token callback registration (cgo.Handle + C wrapper) of a pool that has no
// other users, so the callback (and what it references) can be collected.
func TestPoolCloseReleasesTokenCallback(t *testing.T) {
	s := os.Getenv("GODROR_TEST_DSN")
	if s == "" {
		t.Skip("GODROR_TEST_DSN is not set")
	}
	cp, err := dsn.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDriver()
	defer d.Close()

	collected := make(chan struct{})
	if err := func() error {
		P := commonAndPoolParams{CommonParams: cp.CommonParams, PoolParams: cp.PoolParams}
		P.Username = ""
		P.Password.Reset()
		P.ExternalAuth = Bool(true)
		P.MinSessions, P.MaxSessions, P.SessionIncrement = 0, 1, 1
		enc := base64.RawURLEncoding.EncodeToString
		P.Token = enc([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." +
			enc([]byte(`{"sub":"godror-test","exp":`+strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+`}`)) + "." +
			enc([]byte("sig"))

		// marker is referenced only by the token callback.
		marker := new([64]byte)
		runtime.AddCleanup(marker, func(ch chan struct{}) { close(ch) }, collected)
		P.TokenCB = func(context.Context, *dsn.AccessToken) error {
			runtime.KeepAlive(marker)
			return nil
		}
		P.TokenCBCtx = context.Background()

		if err := d.init(P.ConfigDir, P.LibDir); err != nil {
			return err
		}
		pool, err := d.createPool(P)
		if err != nil {
			return err
		}
		return pool.Close()
	}(); err != nil {
		t.Skipf("cannot create token pool: %+v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Error("token callback still referenced after connPool.Close: registration leaked")
}
