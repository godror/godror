// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

// TestPoolKeyDistinguishesParams checks that parameters which change how a
// pool is created (or which identity it uses) give a separate pool.
func TestPoolKeyDistinguishesParams(t *testing.T) {
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

	base := commonAndPoolParams{CommonParams: cp.CommonParams, PoolParams: cp.PoolParams}
	// No sessions are created up front, so wrong credentials still give a pool.
	base.MinSessions, base.MaxSessions, base.SessionIncrement = 0, 2, 1

	// Two passwords whose SHA-256 hashes share the first 4 bytes.
	pw1, pw2 := collidingPasswords()

	enc := base64.RawURLEncoding.EncodeToString
	fakeToken := func(sub string) string {
		return enc([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." +
			enc([]byte(`{"sub":"`+sub+`","exp":`+strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+`}`)) + "." +
			enc([]byte("sig"))
	}
	tok1, tok2 := fakeToken("godror-test-1"), fakeToken("godror-test-2")
	tokenParams := func(token string) commonAndPoolParams {
		P := base
		P.Username = ""
		P.Password.Reset()
		P.ExternalAuth = Bool(true)
		P.Token = token
		return P
	}

	for _, tc := range []struct {
		Name   string
		P1, P2 func() commonAndPoolParams
	}{
		{Name: "charset",
			P1: func() commonAndPoolParams { return base },
			P2: func() commonAndPoolParams { P := base; P.Charset = "WE8ISO8859P1"; return P },
		},
		{Name: "stmtCacheSize",
			P1: func() commonAndPoolParams { return base },
			P2: func() commonAndPoolParams { P := base; P.StmtCacheSize = 7; return P },
		},
		{Name: "pwHashPrefix",
			P1: func() commonAndPoolParams { P := base; P.Password = dsn.NewPassword(pw1); return P },
			P2: func() commonAndPoolParams { P := base; P.Password = dsn.NewPassword(pw2); return P },
		},
		{Name: "token",
			P1: func() commonAndPoolParams { return tokenParams(tok1) },
			P2: func() commonAndPoolParams { return tokenParams(tok2) },
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			p1, err := d.getPool(tc.P1())
			if err != nil {
				t.Skipf("first pool: %+v", err)
			}
			if again, err := d.getPool(tc.P1()); err != nil {
				t.Fatal(err)
			} else if again != p1 {
				t.Fatal("the same params got a different pool")
			}
			p2, err := d.getPool(tc.P2())
			if err != nil {
				t.Skipf("second pool: %+v", err)
			}
			if p1 == p2 {
				t.Errorf("different %s got the same pool", tc.Name)
			}
		})
	}
}

// collidingPasswords returns two different strings whose SHA-256 hashes have
// the same first 4 bytes (a birthday search, about 2^16 hashes).
func collidingPasswords() (string, string) {
	seen := make(map[[4]byte]string)
	for i := 0; ; i++ {
		s := "pw" + strconv.Itoa(i)
		h := sha256.Sum256([]byte(s))
		k := [4]byte(h[:4])
		if o, ok := seen[k]; ok {
			return o, s
		}
		seen[k] = s
	}
}
