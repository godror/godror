// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestFailedBatchRollback checks that a batch that fails outside of a
// transaction leaves nothing behind in the session.
func TestFailedBatchRollback(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_fbr_batch" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxOpenConns(1) // everything runs on the same session
	db.SetMaxIdleConns(1)

	qry := "INSERT INTO " + tbl + " (id) VALUES (:1)"
	// committed returns the number of committed rows, after an unrelated
	// statement has been run on db's only session.
	committed := func(t *testing.T) int {
		t.Helper()
		var s string
		if err := db.QueryRowContext(ctx, "SELECT 'x' FROM DUAL").Scan(&s); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wantDup := func(t *testing.T, err error) {
		t.Helper()
		var oe *godror.OraErr
		if !errors.As(err, &oe) || oe.Code() != 1 {
			t.Fatalf("got %v, wanted ORA-00001", err)
		}
	}

	// Each subtest starts with an empty table.
	run := func(name string, f func(t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			if _, err := testDb.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				t.Fatal(err)
			}
			f(t)
		})
	}

	run("autocommit", func(t *testing.T) {
		_, err := db.ExecContext(ctx, qry, []int{1, 2, 3, 3, 5})
		wantDup(t, err)
		if n := committed(t); n != 0 {
			t.Errorf("the next statement committed %d rows of the failed batch", n)
		}
	})

	run("serializable", func(t *testing.T) {
		_, err := db.ExecContext(ctx, qry, []int{1, 2, 2})
		wantDup(t, err)
		// ORA-01453 if the session is still in the transaction of the batch.
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
	})

	// The transaction is the caller's: the rows before the failing one stay.
	run("tx", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, qry, 100); err != nil {
			t.Fatal(err)
		}
		_, err = tx.ExecContext(ctx, qry, []int{101, 102, 102})
		wantDup(t, err)
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if n := committed(t); n != 3 {
			t.Errorf("got %d rows, wanted 3 (100, 101, 102)", n)
		}
	})
}
