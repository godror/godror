// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestBatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("Batch"), time.Minute)
	defer cancel()

	const create = `CREATE TABLE test_batch (F_int NUMBER(9), F_num NUMBER, F_text VARCHAR2(1000), F_date DATE)`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		const del = "DROP TABLE test_batch"
		_, _ = testDb.ExecContext(context.Background(), del)
	}()
	const insQry = `INSERT INTO test_batch (F_int, F_num, F_text, F_date) VALUES (:1, :2, :3, :4)`

	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	b := godror.Batch{Stmt: stmt, Limit: 2}
	numRows := b.Limit + 1
	for i := 0; i < numRows; i++ {
		if err = b.Add(ctx, i, float64(i)+0.1, fmt.Sprintf("a-%d", i), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	const qry = "SELECT * FROM test_batch"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var fInt int32
		var fNum, fTxt string
		var fDt sql.NullTime
		if err = rows.Scan(&fInt, &fNum, &fTxt, &fDt); err != nil {
			t.Fatal(err)
		}
		t.Log(i, fInt, fNum, fTxt, fDt)
		i++
		if fNum == "" {
			t.Error("nil inserted")
		}
	}
	if i != numRows {
		t.Errorf("wanted %d rows, got %d", 3, i)
	}
}
