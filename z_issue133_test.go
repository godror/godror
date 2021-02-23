package godror_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
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

var flagAlloc133 = flag.String("alloc133", "alloc133.txt", "file to write alloc info")

const beginMergeLoopCnt = "---BEGIN merge loopCnt: "
const endMergeLoopCnt = "---END merge loopCnt: "

func BenchmarkMemoryAlloc133(t *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := exec.CommandContext(ctx, "go", "test", "-c").Run(); err != nil {
		t.Fatalf("compiling: %+v", err)
	}
	stopConnStats()
	const runs = "3"
	cmd := exec.CommandContext(ctx, "./godror.test", "-test.run=^TestMergeMemory133$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "DPI_DEBUG_LEVEL=32", "RUNS="+runs)
	rc, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	cmd.Stderr = cmd.Stdout
	t.Logf("Start separate test process of env DPI_DEBUG_LEVEL=32 RUNS="+runs+" %v", cmd.Args)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	type alloc struct {
		Line string
		Size uint32
	}
	allocs := make(map[uintptr]alloc)
	prefix := []byte("ODPI [")
	var dealloc uint64
	countRemaining := func() uint64 {
		var remainder uint64
		for _, v := range allocs {
			remainder += uint64(v.Size)
		}
		if remainder > 1<<20 {
			t.Errorf("Remained %d bytes from %d in %d allocations.", remainder, dealloc+remainder, len(allocs))
		} else {
			t.Logf("Remained %d bytes from %d in %d allocations.", remainder, dealloc+remainder, len(allocs))
		}
		dealloc = 0
		return remainder
	}

	fh, err := os.Create(*flagAlloc133)
	if err != nil {
		t.Fatalf("%q: %+v", *flagAlloc133, err)
	}
	defer fh.Close()

	var inLoop bool
	br := bufio.NewScanner(io.TeeReader(rc, fh))
	for br.Scan() {
		line := br.Bytes()
		if !inLoop {
			if bytes.Contains(line, []byte(": "+beginMergeLoopCnt)) {
				inLoop = true
			} else {
				continue
			}
		} else if bytes.Contains(line, []byte(": "+endMergeLoopCnt)) {
			t.Log(string(line))
			inLoop = false
			countRemaining()
			continue
		}

		if !bytes.HasPrefix(line, prefix) {
			if bytes.Contains(line, []byte("z_issue133_test.go:")) {
				t.Log(string(line))
			}
			continue
		}

		line = line[len(prefix):]
		i := bytes.Index(line, []byte(": "))
		if i < 0 {
			continue
		}
		line = line[i+2:]
		if bytes.HasPrefix(line, []byte("OCI allocated ")) || bytes.HasPrefix(line, []byte("allocated ")) {
			ptr, err := getptr(line[8:])
			if err != nil {
				t.Fatalf("%s: %+v", string(line), err)
			}
			size, err := getsize(line)
			if err != nil {
				t.Fatalf("%s: %+v", string(line), err)
			}
			allocs[ptr] = alloc{Line: string(line), Size: size}
		} else if bytes.Contains(line, []byte("freed ptr at ")) {
			ptr, err := getptr(line[12:])
			if err != nil {
				t.Fatalf("%s: %+v", string(line), err)
			}
			dealloc += uint64(allocs[ptr].Size)
			delete(allocs, ptr)
		}
	}
}

func getsize(line []byte) (uint32, error) {
	i := bytes.Index(line, []byte(" bytes "))
	line = line[:i]
	n, err := strconv.ParseUint(string(line[bytes.LastIndexByte(line, ' ')+1:]), 10, 32)
	return uint32(n), err
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

func BenchmarkMergeMemory133(t *testing.B) {
	firstRowBytes, _ := strconv.Atoi(os.Getenv("FIRST_ROW_BYTES"))
	if firstRowBytes <= 0 {
		firstRowBytes = 20000
	}
	rowsToInsert, _ := strconv.Atoi(os.Getenv("ROWS_TO_INSERT"))
	if rowsToInsert <= 0 {
		rowsToInsert = 100000
	}
	useLobs, _ := strconv.ParseBool(os.Getenv("USE_LOBS"))
	runs, _ := strconv.Atoi(os.Getenv("RUNS"))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatalf("%q: %+v", testConStr, err)
	}
	P.StandaloneConnection = true
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxIdleConns(0)

	db.ExecContext(ctx, "DROP TABLE test_many_rows")
	qry := "CREATE TABLE test_many_rows (id number(10), text clob)"
	if _, err := db.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer db.ExecContext(ctx, "DROP TABLE test_many_rows")
	qry = "CREATE index test_many_rows_id_idx on test_many_rows(id)"
	if _, err := db.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Logf("first row bytes len: %d", firstRowBytes)

	var m runtime.MemStats
	pid := os.Getpid()
	for loopCnt := 0; loopCnt < t.N; loopCnt++ {
		if runs != 0 && loopCnt == runs {
			break
		}
		t.Logf(beginMergeLoopCnt+"%d\n", loopCnt)
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = issue133Inner(ctx, t, conn, rowsToInsert, firstRowBytes, useLobs)
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&m)
		t.Logf("Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n",
			float64(m.Alloc)/1024/1024, float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

		rss, err := readSmaps(pid)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf(endMergeLoopCnt+"%d; process memory (rss): %.3f MiB\n", loopCnt, float64(rss)/1024)
	}
	if runs < 64 {
		t.Log("SLEEP a minute")
		time.Sleep(time.Minute)
	}
}

func issue133Inner(ctx context.Context, t testing.TB, conn *sql.Conn, rowsToInsert, firstRowBytes int, useLobs bool) error {
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
	var rss uint64
	m := make(map[string]uint64)
	pfx := []byte("Rss:")
	var key string
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
			rss += size
			m[key] += size
		} else if bytes.IndexByte(line, '-') > 0 {
			f := bytes.Fields(line)
			if len(f) > 5 {
				key = string(f[5])
			}
		}
	}
	fmt.Println(m)
	return rss, nil
}
