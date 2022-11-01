package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/godror/godror"
)

func TestObjOpenClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSqlObjectDirect"), 30*time.Second)
	defer cancel()
	defer tl.enableLogging(t)()

	createTypes := func(ctx context.Context, db *sql.DB) error {
		qry := []string{
			`create or replace type test_type force as object (
   	  id    number(10)
    );`,
			`CREATE OR REPLACE PACKAGE test_pkg_sample AS
	PROCEDURE test_record_in (
		rec IN OUT test_type
	);
	END test_pkg_sample;`,
			`CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS
	PROCEDURE test_record_in (
		rec IN OUT test_type
	) IS
	BEGIN
		rec.id := rec.id + 1;
	END test_record_in;
	END test_pkg_sample;`,
		}
		for _, ddl := range qry {
			_, err := db.ExecContext(ctx, ddl)
			if err != nil {
				return err
			}
		}
		return nil
	}
	dropTypes := func(db *sql.DB) {
		for _, qry := range []string{
			"DROP PACKAGE test_pkg_sample",
			"DROP TYPE test_type",
		} {
			if _, err := db.Exec(qry); err != nil {
				t.Logf("%s: %+v", qry, err)
			}
		}
	}

	readMem := func(pid int32) (uint64, error) {
		b, err := os.ReadFile("/proc/" + strconv.FormatInt(int64(pid), 10) + "/status")
		if err != nil {
			return 0, err
		}
		if i := bytes.Index(b, []byte("\nRssAnon:")); i >= 0 {
			b = b[i+1+3+4+1+1:]
			if i = bytes.IndexByte(b, '\n'); i >= 0 {
				b = b[:i]
				var n uint64
				var u string
				_, err := fmt.Sscanf(string(b), "%d %s", &n, &u)
				if err != nil {
					return 0, fmt.Errorf("%s: %w", string(b), err)
				}
				switch u {
				case "kB":
					n <<= 10
				case "MB":
					n <<= 20
				case "GB":
					n <<= 30
				}
				return n, nil
			}
		}
		return 0, nil
	}
	callObjectType := func(ctx context.Context, db *sql.DB) error {
		cx, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer cx.Close()

		tx, err := cx.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Commit()

		objType, err := godror.GetObjectType(ctx, tx, "TEST_TYPE")
		if err != nil {
			return err
		}
		//defer objType.Close()

		obj, err := objType.NewObject()
		if err != nil {
			return err
		}
		defer obj.Close()

		rec := MyObject{Object: obj, ID: 1}
		params := []interface{}{
			sql.Named("rec", sql.Out{Dest: &rec, In: true}),
		}
		_, err = tx.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)

		return err
	}

	if err := createTypes(ctx, testDb); err != nil {
		t.Fatal(err)
	}
	defer dropTypes(testDb)

	var m runtime.MemStats
	pid := int32(os.Getpid())

	var startMem uint64

	const step = 100

	loopCnt := 0
	printStats := func() {
		runtime.GC()
		runtime.ReadMemStats(&m)
		t.Logf("Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n",
			float64(m.Alloc)/1024/1024, float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

		rss, err := readMem(int32(pid))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d; process memory (rss): %.3f MiB\n", loopCnt, float64(rss)/1024/1024)
		if rss > startMem*2 {
			t.Errorf("started with RSS %d, got %d", startMem, rss)
		}
	}

	dl, _ := ctx.Deadline()
	dl = dl.Add(-3 * time.Second)
	for ; time.Now().Before(dl); loopCnt++ {
		if err := callObjectType(ctx, testDb); err != nil {
			t.Fatal(err)
		}
		if startMem == 0 {
			runtime.GC()
			var err error
			if startMem, err = readMem(pid); err != nil {
				t.Fatal(err)
			}
		}

		if loopCnt%step == 0 {
			printStats()
		}
	}
	printStats()
}

type MyObject struct {
	*godror.Object
	ID int64
}

func (r *MyObject) Scan(src interface{}) error {
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	id, err := obj.Get("ID")
	if err != nil {
		return err
	}
	r.ID = id.(int64)

	return nil
}

// WriteObject update godror.Object with struct attributes values.
// Implement this method if you need the record as an input parameter.
func (r MyObject) WriteObject() error {
	// all attributes must be initialized or you get an "ORA-21525: attribute number or (collection element at index) %s violated its constraints"
	err := r.ResetAttributes()
	if err != nil {
		return err
	}

	var data godror.Data
	err = r.GetAttribute(&data, "ID")
	if err != nil {
		return err
	}
	data.SetInt64(r.ID)
	r.SetAttribute("ID", &data)

	return nil
}
