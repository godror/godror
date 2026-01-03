// Copyright 2019, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestPlSqlNestedObj(t *testing.T) {
	t.Run("200", func(t *testing.T) {
		testPlSqlNestedObj(t, 200)
	})
	t.Run("1", func(t *testing.T) { testPlSqlNestedObj(t, 1) })
}

type (
	i390Pair struct {
		// godror.ObjectTypeName `godror:"pair" json:"-"`

		Key   int32 `godror:"KEY"`
		Value int64 `godror:"VALUE"`
	}

	i390PobjStruct struct {
		// godror.ObjectTypeName `godror:"pobj" json:"-"`
		ID    int32      `godror:"ID"`
		Pairs []i390Pair `godror:",type=pair_list"`
	}
	i390PsliceStruct struct {
		// godror.ObjectTypeName `json:"-"`
		ObjSlice []i390PobjStruct `godror:",type=pobj_t"`
	}
)

func (i390Pair) ObjectTypeName() string       { return "pair" }
func (i390PobjStruct) ObjectTypeName() string { return "pobj" }
func (os i390PobjStruct) WriteObject(o *godror.Object) error {
	return godror.StructWriteObject(o, &os)
}
func (os *i390PobjStruct) Scan(v any) error     { return godror.StructScan(os, v) }
func (i390PsliceStruct) ObjectTypeName() string { return "pobj_t" }
func (ss i390PsliceStruct) WriteObject(o *godror.Object) error {
	return godror.SliceWriteObject(o, ss.ObjSlice)
}
func (ss i390PsliceStruct) Scan(v any) error { return godror.SliceScan(&ss.ObjSlice, v) }

func testPlSqlNestedObj(t *testing.T, step int) {
	if step < 2 {
		step = 2
	}
	stepS := strconv.Itoa(step)
	ctx, cancel := context.WithTimeout(testContext("PlSqlTypes["+stepS+"]"), 1*time.Minute)
	defer cancel()
	// defer tl.enableLogging(t)()

	createTypes := func(ctx context.Context, db *sql.DB) error {
		qry := []string{
			`create or replace type pair force as object (
   	  key    number(3),
	  value  number(18)
    );`,
			`create or replace type pair_list force as table of pair;`,

			`create or replace type pobj force as object (
   	  id    number(10),
	  pairs pair_list
    );`,
			`create or replace type pobj_t is table of pobj;`,

			`CREATE OR REPLACE PACKAGE test_pkg_sample AS
	PROCEDURE test_pobj_in (
		recs IN OUT pobj_t
	);
	END test_pkg_sample;`,
			`CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS
	PROCEDURE test_pobj_in (
		recs IN OUT pobj_t
	) IS
	BEGIN --6
		IF recs IS NULL THEN
			recs := pobj_t();
		END IF;
		IF recs.COUNT = 0 THEN
			recs.extend(` + stepS + `);
			FOR i IN 1..` + stepS + ` LOOP
			  recs(i) := pobj(1, NULL);
			END LOOP;
		END IF;
		FOR i IN 1 .. recs.COUNT LOOP
			recs(i).id := recs(i).id + 1;
			IF recs(i).pairs IS NULL THEN
				recs(i).pairs := pair_list();
			END IF;
			IF recs(i).pairs.COUNT = 0 THEN
				recs(i).pairs.extend(` + stepS + `/2);
				FOR j IN 1..` + stepS + `/2 LOOP
					recs(i).pairs(j) := pair(3, 9);
				END LOOP;
			END IF;
			FOR j IN 1 .. recs(i).pairs.COUNT LOOP
				recs(i).pairs(j).value := recs(i).pairs(j).value + 10;
			END LOOP;
		END LOOP;
	END test_pobj_in;
	END test_pkg_sample;`,
		}
		for _, ddl := range qry {
			_, err := db.ExecContext(ctx, ddl)
			if err != nil {
				return err
			}
		}

		cErrs, gcErr := godror.GetCompileErrors(ctx, db, false)
		if gcErr != nil {
			t.Logf("get compile errors: %+v", gcErr)
		} else if len(cErrs) != 0 {
			for _, ce := range cErrs {
				t.Log(ce)
			}
		}

		return nil
	}

	dropTypes := func(db *sql.DB) {
		for _, qry := range []string{
			"DROP PACKAGE test_pkg_sample",
			"DROP TYPE pobj_t",
			"DROP TYPE pobj",
			"DROP TYPE pair_list",
			"DROP TYPE pair",
		} {
			if _, err := db.Exec(qry); err != nil {
				t.Logf("%s: %+v", qry, err)
			}
		}
	}

	if err := createTypes(ctx, testDb); err != nil {
		t.Fatal(err)
	}
	defer dropTypes(testDb)

	readMem := func(pid int32) (uint64, error) {
		var info syscall.Rusage
		err := syscall.Getrusage(syscall.RUSAGE_SELF, &info)
		if err != nil {
			return 0, err
		}

		// On macOS, Maxrss is in bytes; on Linux, it's in kilobytes
		if runtime.GOOS == "darwin" {
			return uint64(info.Maxrss), nil
		}

		return uint64(info.Maxrss << 10), nil
	}

	var m runtime.MemStats
	pid := int32(os.Getpid())
	startMem := make(map[string]uint64)

	const MiB = 1 << 20

	loopCnt := 0
	printStats := func(t *testing.T) {
		runtime.GC()
		runtime.ReadMemStats(&m)
		t.Logf("%s: Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n", t.Name(),
			float64(m.Alloc)/MiB, float64(m.HeapInuse)/MiB, float64(m.Sys)/MiB, m.NumGC)

		rss, err := readMem(int32(pid))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d; process memory (rss): %.3f MiB\n", t.Name(), loopCnt, float64(rss)/MiB)
		if start, ok := startMem[t.Name()]; ok && rss > start*2 {
			t.Fatalf("%s: started with RSS %d, got %d (%.3f%%)",
				t.Name(),
				startMem[t.Name()]/MiB, rss/MiB, float64(rss*100)/float64(startMem[t.Name()]))
		} else {
			t.Logf("%s: started with RSS %d, got %d (%.3f%%)",
				t.Name(),
				start/MiB, rss/MiB, float64(rss*100)/float64(start))
		}
	}

	pslice := func(nobjs, npairs int) i390PsliceStruct {
		s := i390PsliceStruct{ObjSlice: make([]i390PobjStruct, nobjs)}
		for i := range s.ObjSlice {
			s.ObjSlice[i].ID = int32(i + 1)
			s.ObjSlice[i].Pairs = make([]i390Pair, npairs)
			for j := range s.ObjSlice[i].Pairs {
				s.ObjSlice[i].Pairs[j].Key = int32(j + 1)
				s.ObjSlice[i].Pairs[j].Value = int64((i+1)*1000 + (j + 1))
			}
		}
		return s
	}(step, step/2) // 100 objects, each with 50 pairs

	type direction uint8
	const (
		// justIn = direction(1)
		justOut = direction(2)
		inOut   = direction(3)
	)

	// godror.GuardWithFinalizers(true)
	// godror.LogLingeringResourceStack(true)
	// defer godror.LogLingeringResourceStack(false)

	callObjectType := func(ctx context.Context, db *sql.DB, dir direction) error {
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

		s := pslice

		var param any
		switch dir {
		// case justIn: param=s
		case justOut:
			param = sql.Out{Dest: &s}
		case inOut:
			param = sql.Out{Dest: &s, In: true}
		}
		const qry = `begin test_pkg_sample.test_pobj_in(:1); end;`
		_, err = tx.ExecContext(ctx, qry, param)
		// t.Log(pslice)
		return err
	}

	dirs := []direction{justOut, inOut}

	dl, _ := ctx.Deadline()
	dl = dl.Add(-3 * time.Second)
	dur := time.Until(dl) / time.Duration(len(dirs))
	run := func(t *testing.T, dir direction) {
		var name string
		switch dir {
		// case justIn: name ="justIn"
		case justOut:
			name = "justOut"
		case inOut:
			name = "inOut"
		}
		t.Run(name, func(t *testing.T) {
			dl := time.Now().Add(dur)
			loopCnt = 0
			t.Logf("dl: %v dur:%v", dl, dur)
			var lastPrint time.Time
			for ; time.Now().Before(dl); loopCnt++ {
				if err := callObjectType(ctx, testDb, dir); err != nil {
					t.Fatal("callObjectType:", err)
				}
				if step <= 2 {
					break
				}
				if startMem[t.Name()] == 0 {
					runtime.GC()
					var err error
					if startMem[t.Name()], err = readMem(pid); err != nil {
						t.Fatal(err)
					}
					continue
				}
				now := time.Now()
				if lastPrint.Before(now.Add(-2 * time.Second)) {
					printStats(t)
					lastPrint = now
				}
			}
			printStats(t)
		})
	}

	for _, dir := range dirs {
		run(t, dir)
	}
}
