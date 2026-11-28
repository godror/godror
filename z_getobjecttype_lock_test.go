// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestGetObjectTypeConcurrent calls GetObjectType on one connection from
// several goroutines. Run it with -race: it checks the object type cache
// of the connection.
func TestGetObjectTypeConcurrent(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	names := make([]string, 8)
	for i := range names {
		names[i] = strings.ToUpper(fmt.Sprintf("test_gotc%d%s", i, tblSuffix))
		if _, err := testDb.ExecContext(ctx, "CREATE OR REPLACE TYPE "+names[i]+" AS OBJECT (a NUMBER, b VARCHAR2(10))"); err != nil {
			t.Fatal(err)
		}
		defer testDb.ExecContext(context.Background(), "DROP TYPE "+names[i])
	}
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	conn, err := godror.DriverConn(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Go(func() {
			ot, err := conn.GetObjectType(name)
			if err != nil {
				t.Error(err)
				return
			}
			o, err := ot.NewObject()
			if err != nil {
				t.Error(err)
				return
			}
			o.Close()
		})
	}
	wg.Wait()
}
