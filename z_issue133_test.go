package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestMergeMemory133(t *testing.T) {
	testDb.Exec("DROP PACKAGE tst_bench_25")
	firstRowBytes, _ := strconv.Atoi(os.Getenv("FIRST_ROW_BYTES"))
	if firstRowBytes <= 0 {
		firstRowBytes = 20000
	}
	rowsToInsert, _ := strconv.Atoi(os.Getenv("ROWS_TO_INSERT"))
	if rowsToInsert <= 0 {
		rowsToInsert = 1000
	}
	useLobs, _ := strconv.ParseBool(os.Getenv("USE_LOBS"))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, "DROP TABLE test_many_rows")
	qry := "CREATE TABLE test_many_rows (id number(10), text clob)"
	if _, err := conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer conn.ExecContext(ctx, "DROP TABLE test_many_rows")
	qry = "CREATE index test_many_rows_id_idx on test_many_rows(id)"
	if _, err := conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Logf("first row bytes len: %d", firstRowBytes)

	var m runtime.MemStats
	for loopCnt := 0; loopCnt < 64; loopCnt++ {
		t.Logf("Start merge loopCnt: %d\n", loopCnt)
		if err := issue133Inner(ctx, t, conn, rowsToInsert, firstRowBytes, useLobs); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&m)
		t.Logf("Alloc: %.3f, totalAlloc: %.3f, Heap: %.3f, Sys: %.3f , NumGC: %d\n",
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024,
			float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

		pss, err := readSmaps()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Current process memory (Pss): %.3f MiB\n", float64(pss)/1024)
	}
}

func issue133Inner(ctx context.Context, t *testing.T, conn *sql.Conn, rowsToInsert, firstRowBytes int, useLobs bool) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginTx: %w", err)
	}
	defer tx.Rollback()

	const qry = `MERGE /*+ INDEX(x)*/ INTO test_many_rows x 
			USING (select :ID ID, :text text from dual) y 
			ON (x.id = y.id) 
			WHEN MATCHED 
			THEN UPDATE SET  x.text = y.text 
			WHEN NOT MATCHED 
			THEN INSERT (x.ID, x.TEXT) VALUES (y.ID, y.TEXT)`
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer stmt.Close()

	const batchSize = 100
	const nextRowBytes = 75
	firstRow := strings.Repeat("a", firstRowBytes)
	nextRow := strings.Repeat("b", nextRowBytes)

	t.Logf("Total size of clob strings: %d\n", firstRowBytes+(rowsToInsert-1)*nextRowBytes)

	mergeIds := make([]sql.NullInt64, 0, batchSize)
	mergeLobs := make([]godror.Lob, 0, batchSize)
	mergeStrings := make([]string, 0, batchSize)

	var dur time.Duration
	totalRowsMerged := 0
	for rowsToInsert > 0 {
		mergeIds, mergeStrings, mergeLobs = mergeIds[:0], mergeStrings[:0], mergeLobs[:0]
		for i := 0; i < batchSize; i++ {
			mergeIds = append(mergeIds, sql.NullInt64{Int64: int64(i), Valid: true})
			row := nextRow
			if i == 0 {
				row = firstRow
			}
			if useLobs {
				mergeLobs = append(mergeLobs, godror.Lob{Reader: strings.NewReader(row), IsClob: true})
			} else {
				mergeStrings = append(mergeStrings, row)
			}
		}
		var mergeSth interface{} = mergeStrings
		if useLobs {
			mergeSth = mergeLobs
		}

		start := time.Now()
		res, err := stmt.ExecContext(ctx,
			sql.Named("id", mergeIds),
			sql.Named("text", mergeSth),
		)
		if err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}

		dur += time.Since(start)

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}
		totalRowsMerged += int(rowsAffected)
		rowsToInsert -= int(rowsAffected)
	}
	tx.Commit()
	t.Logf("Merge done, number of rows merged: %d, merge timing: %s\n", totalRowsMerged, dur)

	stmt.Close()
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	return nil
}

func readSmaps() (uint64, error) {
	b, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/smaps", os.Getpid()))
	if err != nil {
		return 0, err
	}
	var pss uint64
	pfx := []byte("Pss:")
	for _, line := range bytes.Split(b, []byte("\n")) {
		if bytes.HasPrefix(line, pfx) {
			line = bytes.TrimSpace(line[len(pfx):])
			j := bytes.IndexByte(line, ' ')
			if j >= 0 {
				line = line[:j]
			}
			size, err := strconv.ParseUint(string(line), 10, 64)
			if err != nil {
				return 0, err
			}
			pss += size
		}
	}
	return pss, nil
}
