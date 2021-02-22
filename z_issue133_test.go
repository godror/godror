package godror_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestMemoryAlloc133(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-run=^TestMergeMemory133$", "-count=1")
	cmd.Env = append(os.Environ(), "DPI_DEBUG_LEVEL=32", "RUNS=1")
	cmd.Stderr = os.Stderr
	rc, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	t.Logf("Start separate test process of env DPI_DEBUG_LEVEL=32 RUNS=1 %v", cmd.Args)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	startPss, err := readSmaps(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	var endPss uint64
	allocs := make(map[uintptr]string)
	prefix := []byte("ODPI [")

	br := bufio.NewScanner(rc)
	for br.Scan() {
		line := br.Bytes()
		if !bytes.HasPrefix(line, prefix) {
			continue
		}
		line = line[len(prefix):]
		i := bytes.Index(line, []byte(": "))
		if i < 0 {
			continue
		}
		line = line[i+2:]
		t.Log(string(line))
		if bytes.HasPrefix(line, []byte("OCI allocated ")) || bytes.HasPrefix(line, []byte("allocated ")) {
			ptr, err := getptr(line[8:])
			if err != nil {
				t.Fatalf("%s: %+v", string(line), err)
			}
			allocs[ptr] = string(line)
		} else if bytes.Contains(line, []byte("freed ptr at ")) {
			ptr, err := getptr(line[12:])
			if err != nil {
				t.Fatalf("%s: %+v", string(line), err)
			}
			delete(allocs, ptr)
		}
		pss, err := readSmaps(cmd.Process.Pid)
		if err != nil {
			if endPss == 0 {
				t.Error(err)
			}
		} else if pss != 0 {
			endPss = pss
		}
	}
	t.Logf("start: %d end: %d", startPss, endPss)
	if len(allocs) != 0 {
		t.Error("Remaining allocations:", allocs)
	}
}

func getptr(line []byte) (uintptr, error) {
	i := bytes.Index(line, []byte(" 0x"))
	if i < 0 {
		return 0, fmt.Errorf("no 0x")
	}
	line = line[i+3:]
	var a [16]byte
	for i, c := range line {
		if '0' <= c && c <= '9' ||
			'a' <= c && c <= 'f' ||
			'A' <= c && c <= 'F' {
			a[cap(a)-1-i] = c
		} else {
			break
		}
	}
	src, dst := a[:], a[:]
	for i, c := range src {
		if c != 0 {
			if (cap(a)-i)%2 == 0 {
				src = src[i:]
			} else {
				src = src[i-1:]
				src[0] = '0'
			}
			break
		}
	}
	n, err := hex.Decode(dst, src)
	if err != nil {
		return 0, err
	}
	for i := n; i < 8; i++ {
		dst[i] = 0
	}
	return uintptr(binary.LittleEndian.Uint64(dst[:8])), nil
}

func TestMergeMemory133(t *testing.T) {
	testDb.Exec("DROP PACKAGE tst_bench_25")
	firstRowBytes, _ := strconv.Atoi(os.Getenv("FIRST_ROW_BYTES"))
	if firstRowBytes <= 0 {
		firstRowBytes = 20000
	}
	rowsToInsert, _ := strconv.Atoi(os.Getenv("ROWS_TO_INSERT"))
	if rowsToInsert <= 0 {
		rowsToInsert = 200000
	}
	useLobs, _ := strconv.ParseBool(os.Getenv("USE_LOBS"))
	runs, _ := strconv.Atoi(os.Getenv("RUNS"))
	if runs <= 0 {
		runs = 64
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
	t.Logf("first row bytes len: %d", firstRowBytes)

	var m runtime.MemStats
	pid := os.Getpid()
	for loopCnt := 0; loopCnt < runs; loopCnt++ {
		t.Logf("Start merge loopCnt: %d\n", loopCnt)
		if err := issue133Inner(ctx, t, conn, rowsToInsert, firstRowBytes, useLobs); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&m)
		t.Logf("Alloc: %.3f, totalAlloc: %.3f, Heap: %.3f, Sys: %.3f , NumGC: %d\n",
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1025/1024,
			float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

		pss, err := readSmaps(pid)
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

	const qry = `MERGE /*+ INDEX(x) */ INTO test_many_rows x 
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

	const batchSize = 10000
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

func readSmaps(pid int) (uint64, error) {
	b, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/smaps", pid))
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
