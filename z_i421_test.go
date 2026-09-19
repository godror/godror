// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"

	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

// TestIssue421_CharsetMemoryLeak regression-tests Issue #421:
// verify that creating pools or standalone connectors with custom Charset
// correctly releases C.CString(charset) via freeCommonCreateParams without leaking C-heap.
func TestIssue421_CharsetMemoryLeak(t *testing.T) {
	if testConStr == "" {
		t.Skip("no test database connection string")
	}
	P, err := dsn.Parse(testConStr)
	if err != nil {
		t.Fatal(err)
	}

	// Specify non-empty charset to exercise the P.encoding allocation path
	P.CommonParams.Charset = "AL32UTF8"
	connector := godror.NewConnector(P)

	var startRSS uint64
	for i := range 50 {
		db := sql.OpenDB(connector)
		if err := db.Ping(); err != nil {
			_ = db.Close()
			t.Fatalf("iter %d: Ping failed: %v", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("iter %d: Close failed: %v", i, err)
		}

		// Have the first 3 runs be a warmup
		if i == 3 {
			if startRSS, err = readMem(); err != nil {
				t.Fatal(err)
			}
		}
	}
	endRSS, err := readMem()
	if err != nil {
		t.Fatal(err)
	}

	gainPercent := float32(endRSS*100) / float32(startRSS)
	t.Logf("start=%d end=%d diff=%d: %.03f%%", startRSS, endRSS, endRSS-startRSS, gainPercent)
	if endRSS*10 > startRSS*11 {
		t.Errorf("diff: %.03f%%", gainPercent)
	}
}
