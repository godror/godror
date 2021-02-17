package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMergeMemory133(t *testing.T) {
	testDb.Exec("DROP PACKAGE tst_bench_25")
	firstRowBytes, _ := strconv.Atoi(os.Getenv("FIRST_ROW_BYTES"))
	if firstRowBytes <= 0 {
		firstRowBytes = 20000
	}

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
	const rowsToInsert = 10000
	t.Logf("first row bytes len: %d", firstRowBytes)

	var m runtime.MemStats
	for loopCnt := 0; loopCnt < 64; loopCnt++ {
		t.Logf("Start merge loopCnt: %d\n", loopCnt)
		if err := issue133Inner(ctx, t, conn, rowsToInsert, firstRowBytes); err != nil {
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

func issue133Inner(ctx context.Context, t *testing.T, conn *sql.Conn, rowsToInsert, firstRowBytes int) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginTx: %w", err)
	}
	defer tx.Rollback()

	const qry = `MERGE /*+ INDEX(x)*/ INTO test_many_rows x 
			USING (select  :ID ID, :text text from dual) y 
			ON (x.id = y.id ) 
			WHEN MATCHED 
			THEN UPDATE SET  x.text = y.text 
			WHEN NOT MATCHED 
			THEN INSERT (x.ID, x.TEXT) VALUES (y.ID, y.TEXT)`
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer stmt.Close()

	mergeIds := make([]sql.NullInt64, 0, rowsToInsert)
	mergeStrings := make([]string, 0, rowsToInsert)
	totalBytes := 0
	notFirst := strings.Repeat("a", 75)
	for i := 0; i < rowsToInsert; i++ {
		str := notFirst
		if i == 0 {
			str = strings.Repeat("a", firstRowBytes)
		}
		mergeStrings = append(mergeStrings, str)
		totalBytes += len(str)
		mergeIds = append(mergeIds, sql.NullInt64{Int64: int64(i), Valid: true})
	}
	t.Logf("Total size of clob strings: %d\n", totalBytes)

	var maxValuesPerOp int = 10000
	maxLoopCount := int(math.Ceil(float64(rowsToInsert) / float64(maxValuesPerOp)))
	var dur time.Duration
	totalRowsMerged := 0
	for loopIdx := 0; loopIdx < maxLoopCount; loopIdx++ {
		sliceStartIdx := loopIdx * maxValuesPerOp
		sliceEndIdx := int(rowsToInsert)
		if sliceStartIdx+maxValuesPerOp < int(rowsToInsert) {
			sliceEndIdx = sliceStartIdx + maxValuesPerOp
		}

		binds := make([]interface{}, 0, 2)

		int64Elements := make([]sql.NullInt64, sliceEndIdx-sliceStartIdx)
		copy(int64Elements, mergeIds[sliceStartIdx:sliceEndIdx])
		binds = append(binds, sql.Named("id", int64Elements))
		stringElements := make([]string, sliceEndIdx-sliceStartIdx)
		copy(stringElements, mergeStrings[sliceStartIdx:sliceEndIdx])
		binds = append(binds, sql.Named("text", stringElements))

		var res sql.Result
		start := time.Now()
		if res, err = stmt.ExecContext(ctx, binds...); err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}

		dur += time.Since(start)

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			fmt.Errorf("%s: %w", qry, err)
		}
		totalRowsMerged += int(rowsAffected)
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
