// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

const (
	secretPassword = "Secret-Pass-123"
	secretToken    = "Secret-Token-456"
	secretKey      = "Secret-Key-789"
)

func TestParseErrorHidesPassword(t *testing.T) {
	for _, s := range []string{
		"oracle://scott:" + secretPassword + "@host:1521/svc%zz", // invalid URL escape
		`user=scott password=` + secretPassword + ` connectString="unterminated`,
	} {
		_, err := Parse(s)
		if err == nil {
			t.Errorf("%q: wanted a parse error", s)
			continue
		}
		if strings.Contains(err.Error(), secretPassword) {
			t.Errorf("parse error contains the password: %v", err)
		}
	}
}

func TestLogValueHidesSecrets(t *testing.T) {
	var P ConnectionParams
	P.Username = "scott"
	P.Password.Set(secretPassword)
	P.Token = secretToken
	P.PrivateKey = secretKey
	P.ConnectString = "host:1521/svc"

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger.Debug("x", "params", P, "common", P.CommonParams, "simple", P.CommonSimpleParams)
	out := buf.String()
	for _, s := range []string{secretPassword, secretToken, secretKey} {
		if strings.Contains(out, s) {
			t.Errorf("JSON log contains %q:\n%s", s, out)
		}
	}
	if !strings.Contains(out, "scott") {
		t.Errorf("JSON log lost the non-secret fields:\n%s", out)
	}
}
