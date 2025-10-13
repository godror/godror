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
	"syscall"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestPlSqlNestedObj(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSqlTypes"), 1*time.Minute)
	defer cancel()

	//godror.SetLogger(zlog.NewT(t).SLog())

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
	BEGIN
		FOR i IN 1 .. recs.COUNT LOOP
			recs(i).id := recs(i).id + 1;
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

	var startMem uint64

	const step = 100
	const MiB = 1 << 20

	loopCnt := 0
	printStats := func() {
		runtime.GC()
		runtime.ReadMemStats(&m)
		t.Logf("Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n",
			float64(m.Alloc)/MiB, float64(m.HeapInuse)/MiB, float64(m.Sys)/MiB, m.NumGC)

		rss, err := readMem(int32(pid))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d; process memory (rss): %.3f MiB\n", loopCnt, float64(rss)/MiB)
		if rss > startMem*2 {
			t.Errorf("started with RSS %d, got %d (%.3f%%)", startMem/MiB, rss/MiB, float64(rss*100)/float64(startMem))
		}
	}

	type pair struct {
		godror.ObjectTypeName `godror:"pair" json:"-"`

		Key   int32 `godror:"KEY"`
		Value int64 `godror:"VALUE"`
	}

	type pobjStruct struct {
		godror.ObjectTypeName `godror:"pobj" json:"-"`
		ID                    int32  `godror:"ID"`
		Pairs                 []pair `godror:",type=pair_list"`
	}
	type psliceStruct struct {
		godror.ObjectTypeName `json:"-"`
		ObjSlice              []pobjStruct `godror:",type=pobj_t"`
	}

	pslice := func(nobjs, npairs int) psliceStruct {
		s := psliceStruct{ObjSlice: make([]pobjStruct, nobjs)}
		for i := range s.ObjSlice {
			s.ObjSlice[i].ID = int32(i + 1)
			s.ObjSlice[i].Pairs = make([]pair, npairs)
			for j := range s.ObjSlice[i].Pairs {
				s.ObjSlice[i].Pairs[j].Key = int32(j + 1)
				s.ObjSlice[i].Pairs[j].Value = int64((i+1)*1000 + (j + 1))
			}
		}
		return s
	}(step, step/2) // 100 objects, each with 50 pairs

	godror.GuardWithFinalizers(true)
	godror.LogLingeringResourceStack(true)
	defer godror.LogLingeringResourceStack(false)

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

		const qry = `begin test_pkg_sample.test_pobj_in(:1); end;`
		_, err = tx.ExecContext(ctx,
			qry,
			sql.Out{Dest: &pslice, In: true},
		)
		// t.Log(pslice)
		return err
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
