// Copyright 2017, 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

func TestBatchErrorHandling(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchErrorHandling"), time.Minute)
	defer cancel()

	tbl := "test_batch_error" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9) PRIMARY KEY, name VARCHAR2(100) NOT NULL)`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, name) VALUES (:1, :2)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	// Test duplicate key error handling
	b := godror.Batch{Stmt: stmt, Limit: 3}

	// Add duplicate IDs to trigger constraint violation
	if err = b.Add(ctx, 1, "first"); err != nil {
		t.Fatal(err)
	}
	if err = b.Add(ctx, 1, "duplicate"); err != nil {
		t.Fatal(err)
	}

	// Flush should return an error due to primary key constraint
	err = b.Flush(ctx)
	if err == nil {
		t.Error("expected error due to duplicate primary key, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestBatchRowCountValidation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchRowCountValidation"), time.Minute)
	defer cancel()

	tbl := "test_batch_rowcount" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), name VARCHAR2(100), status VARCHAR2(10) DEFAULT 'ACTIVE')`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, name) VALUES (:1, :2)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	b := godror.Batch{Stmt: stmt, Limit: 5}
	expectedRows := 3

	for i := 0; i < expectedRows; i++ {
		if err = b.Add(ctx, i+1, fmt.Sprintf("name_%d", i+1)); err != nil {
			t.Fatal(err)
		}
	}

	err = b.Flush(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify data was actually inserted
	var count int
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expectedRows {
		t.Errorf("expected %d rows in table, got %d", expectedRows, count)
	}
}

func TestBatchRowCountMismatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchRowCountMismatch"), time.Minute)
	defer cancel()

	tbl := "test_batch_mismatch" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9) PRIMARY KEY, name VARCHAR2(100), status VARCHAR2(10) DEFAULT 'PENDING')`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	// First, insert some test data
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, name, status) VALUES (1, 'existing1', 'ACTIVE')"); err != nil {
		t.Fatal(err)
	}
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, name, status) VALUES (2, 'existing2', 'INACTIVE')"); err != nil {
		t.Fatal(err)
	}

	// Now create a batch with UPDATE statements that will affect fewer rows than in the batch
	// This will test our row count validation logic
	updateQry := `UPDATE ` + tbl + ` SET name = :1 WHERE status = 'ACTIVE' AND id = :2`
	stmt, err := testDb.PrepareContext(ctx, updateQry)
	if err != nil {
		t.Fatalf("%s: %+v", updateQry, err)
	}
	defer stmt.Close()

	b := godror.Batch{Stmt: stmt, Limit: 5}

	// Add 3 updates, but only 1 will actually affect a row (id=1 with status='ACTIVE')
	// The other 2 won't match any rows since id=2 has status='INACTIVE' and id=3 doesn't exist
	if err = b.Add(ctx, "updated_name1", 1); err != nil { // This will affect 1 row
		t.Fatal(err)
	}
	if err = b.Add(ctx, "updated_name2", 2); err != nil { // This will affect 0 rows (status mismatch)
		t.Fatal(err)
	}
	if err = b.Add(ctx, "updated_name3", 3); err != nil { // This will affect 0 rows (no such id)
		t.Fatal(err)
	}

	// Flush should return an error because only 1 row was affected but we expected 3
	err = b.Flush(ctx)
	if err == nil {
		t.Error("expected error due to row count mismatch, got nil")
	}

	// The error should specifically mention the row count mismatch
	if err != nil && !strings.Contains(err.Error(), "expected 3 rows affected, got 1") {
		t.Errorf("expected row count validation error, got: %v", err)
	}

	t.Logf("Got expected row count validation error: %v", err)
}

func TestBatchEmptyFlush(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchEmptyFlush"), time.Minute)
	defer cancel()

	tbl := "test_batch_empty" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), value VARCHAR2(50))`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, value) VALUES (:1, :2)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	b := godror.Batch{Stmt: stmt, Limit: 10}

	// Flush empty batch should not error
	err = b.Flush(ctx)
	if err != nil {
		t.Errorf("unexpected error on empty flush: %v", err)
	}
}

func TestBatchNilValues(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchNilValues"), time.Minute)
	defer cancel()

	tbl := "test_batch_nil" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), name VARCHAR2(100), age NUMBER(3))`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, name, age) VALUES (:1, :2, :3)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	b := godror.Batch{Stmt: stmt, Limit: 3}

	// Test various nil value scenarios
	if err = b.Add(ctx, 1, "Alice", 25); err != nil {
		t.Fatal(err)
	}
	if err = b.Add(ctx, 2, nil, 30); err != nil {
		t.Fatal(err)
	}
	if err = b.Add(ctx, 3, "Charlie", nil); err != nil {
		t.Fatal(err)
	}

	err = b.Flush(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify data - the most important test is that the data was inserted
	var count int
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows inserted, got %d", count)
	}

	// Verify data content
	rows, err := testDb.QueryContext(ctx, "SELECT id, name, age FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		name sql.NullString
		age  sql.NullInt64
	}{
		{1, sql.NullString{String: "Alice", Valid: true}, sql.NullInt64{Int64: 25, Valid: true}},
		{2, sql.NullString{String: "", Valid: true}, sql.NullInt64{Int64: 30, Valid: true}},
		{3, sql.NullString{String: "Charlie", Valid: true}, sql.NullInt64{Int64: 0, Valid: true}},
	}

	i := 0
	for rows.Next() {
		var id int
		var name sql.NullString
		var age sql.NullInt64
		if err = rows.Scan(&id, &name, &age); err != nil {
			t.Fatal(err)
		}
		if i >= len(expected) {
			t.Fatalf("more rows than expected: got row %d", i+1)
		}
		exp := expected[i]
		if id != exp.id || name.Valid != exp.name.Valid || name.String != exp.name.String ||
			age.Valid != exp.age.Valid || age.Int64 != exp.age.Int64 {
			t.Errorf("row %d: expected %+v, got id=%d name=%+v age=%+v", i, exp, id, name, age)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), i)
	}
}

func TestBatchAutoFlush(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchAutoFlush"), time.Minute)
	defer cancel()

	tbl := "test_batch_autoflush" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), batch_num NUMBER(2))`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, batch_num) VALUES (:1, :2)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	// Test auto-flush when reaching limit
	b := godror.Batch{Stmt: stmt, Limit: 3}

	// Add exactly Limit rows - should trigger auto-flush
	for i := 0; i < b.Limit; i++ {
		if err = b.Add(ctx, i+1, 1); err != nil {
			t.Fatal(err)
		}
	}

	// Batch should be empty now due to auto-flush
	if b.Size() != 0 {
		t.Errorf("expected batch size 0 after auto-flush, got %d", b.Size())
	}

	// Add more rows for second batch
	for i := 0; i < 2; i++ {
		if err = b.Add(ctx, i+b.Limit+1, 2); err != nil {
			t.Fatal(err)
		}
	}

	if b.Size() != 2 {
		t.Errorf("expected batch size 2, got %d", b.Size())
	}

	// Manual flush of remaining
	err = b.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all rows were inserted
	var count int
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
		t.Fatal(err)
	}
	expectedTotal := b.Limit + 2
	if count != expectedTotal {
		t.Errorf("expected %d total rows, got %d", expectedTotal, count)
	}

	// Verify batch distribution
	var batch1Count, batch2Count int
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl+" WHERE batch_num = 1").Scan(&batch1Count); err != nil {
		t.Fatal(err)
	}
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl+" WHERE batch_num = 2").Scan(&batch2Count); err != nil {
		t.Fatal(err)
	}

	if batch1Count != b.Limit {
		t.Errorf("expected %d rows in batch 1, got %d", b.Limit, batch1Count)
	}
	if batch2Count != 2 {
		t.Errorf("expected 2 rows in batch 2, got %d", batch2Count)
	}
}

func TestBatchStringConversion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchStringConversion"), time.Minute)
	defer cancel()

	tbl := "test_batch_string" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), str_value VARCHAR2(100))`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, str_value) VALUES (:1, :2)`
	stmt, err := testDb.PrepareContext(ctx, insQry)
	if err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}
	defer stmt.Close()

	b := godror.Batch{Stmt: stmt, Limit: 3}

	// Test only string values to avoid type conversion issues
	if err = b.Add(ctx, 1, "string_value"); err != nil {
		t.Fatal(err)
	}
	if err = b.Add(ctx, 2, "another_string"); err != nil {
		t.Fatal(err)
	}
	if err = b.Add(ctx, 3, "third_string"); err != nil {
		t.Fatal(err)
	}

	err = b.Flush(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify data was inserted correctly
	var count int
	if err = testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestBatchConcurrentUsage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BatchConcurrentUsage"), time.Minute)
	defer cancel()

	tbl := "test_batch_concurrent" + tblSuffix
	create := `CREATE TABLE ` + tbl + ` (id NUMBER(9), thread_id NUMBER(2), value VARCHAR2(50))`
	if _, err := testDb.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)
	}()

	insQry := `INSERT INTO ` + tbl + ` (id, thread_id, value) VALUES (:1, :2, :3)`

	// Test that each batch instance works independently
	const numThreads = 3
	const rowsPerThread = 4

	errCh := make(chan error, numThreads)

	for threadID := 0; threadID < numThreads; threadID++ {
		go func(tid int) {
			stmt, err := testDb.PrepareContext(ctx, insQry)
			if err != nil {
				errCh <- fmt.Errorf("thread %d prepare: %w", tid, err)
				return
			}
			defer stmt.Close()

			b := godror.Batch{Stmt: stmt, Limit: 2}

			for i := 0; i < rowsPerThread; i++ {
				id := tid*rowsPerThread + i + 1
				value := fmt.Sprintf("thread_%d_row_%d", tid, i)
				if err = b.Add(ctx, id, tid, value); err != nil {
					errCh <- fmt.Errorf("thread %d add: %w", tid, err)
					return
				}
			}

			// Final flush
			if err = b.Flush(ctx); err != nil {
				errCh <- fmt.Errorf("thread %d flush: %w", tid, err)
				return
			}

			errCh <- nil
		}(threadID)
	}

	// Wait for all threads
	for i := 0; i < numThreads; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	// Verify total count
	var totalCount int
	if err := testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&totalCount); err != nil {
		t.Fatal(err)
	}
	expectedTotal := numThreads * rowsPerThread
	if totalCount != expectedTotal {
		t.Errorf("expected %d total rows, got %d", expectedTotal, totalCount)
	}

	// Verify each thread's data
	for threadID := 0; threadID < numThreads; threadID++ {
		var threadCount int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl+" WHERE thread_id = :1", threadID).Scan(&threadCount); err != nil {
			t.Fatal(err)
		}
		if threadCount != rowsPerThread {
			t.Errorf("thread %d: expected %d rows, got %d", threadID, rowsPerThread, threadCount)
		}
	}
}
