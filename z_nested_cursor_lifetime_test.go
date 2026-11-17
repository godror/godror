package godror_test

import (
	"database/sql/driver"
	godror "github.com/godror/godror"
	"testing"
	"time"
)

func TestNestedCursorSurvivesParentNext(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	rows, err := testDb.QueryContext(ctx, "SELECT CURSOR(SELECT 42 FROM DUAL) FROM DUAL CONNECT BY LEVEL <= 2", godror.FetchArraySize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("first row: %v", rows.Err())
	}
	var child driver.Rows
	if err := rows.Scan(&child); err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if !rows.Next() {
		t.Fatalf("second row: %v", rows.Err())
	}
	var second driver.Rows
	if err := rows.Scan(&second); err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	values := make([]driver.Value, 1)
	if err := child.Next(values); err != nil {
		t.Fatalf("child after parent advanced: %v", err)
	}
}
