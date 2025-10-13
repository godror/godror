package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func Test119GetTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const qry = "SELECT ALL_TABLES.TABLE_NAME,COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DECODE(NULLABLE,'Y','Yes','N','No','X') AS NULLALOWED,'X' AS X FROM ALL_TABLES JOIN ALL_TAB_COLUMNS ON (ALL_TABLES.TABLE_NAME=ALL_TAB_COLUMNS.TABLE_NAME) ORDER BY TABLE_NAME,COLUMN_ID"
	rows, err := testDb.QueryContext(ctx, qry, godror.FetchArraySize(1024), godror.PrefetchCount(1024+1))
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer rows.Close()

	type Column struct {
		ColumnName    string
		DataType      string
		NullAllowed   string
		DataLength    sql.NullInt64
		DataPrecision sql.NullInt64
	}
	type Table struct {
		Name    string
		Columns []Column
	}

	var (
		colCnt int
		stat   = make(map[string]int, 3)
		tables = make([]Table, 0, 1024)
		table  = Table{Columns: make([]Column, 0, 32)}
	)
	for rows.Next() {
		var column Column
		var tableName, x string
		if err := rows.Scan(&tableName, &column.ColumnName, &column.DataType, &column.DataLength, &column.DataPrecision, &column.NullAllowed, &x); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		stat[column.NullAllowed] += 1
		if table.Name != tableName {
			if table.Name != "" {
				tables = append(tables, table)
				table = Table{Name: tableName, Columns: make([]Column, 0, 32)}
			} else {
				table.Name = tableName
			}
		}
		table.Columns = append(table.Columns, column)
		colCnt++
	}

	//in the end we have last table to add to result, if there was one
	if table.Name != "" {
		tables = append(tables, table)
	}
	if rows.Err() != nil {
		t.Fatalf("%s: %+v", qry, err)
	}

	t.Logf("NullableStat: %+v tables:%d columns:%d", stat, len(tables), colCnt)
}
