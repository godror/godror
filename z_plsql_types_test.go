// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
	godror "github.com/godror/godror"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/sync/errgroup"
)

var _ godror.ObjectScanner = new(MyRecord)
var _ godror.ObjectWriter = new(MyRecord)

var _ godror.ObjectScanner = new(MyTable)

// MYRecord represents TEST_PKG_TYPES.MY_RECORD
type MyRecord struct {
	*godror.Object
	Txt, AClob string
	ID         int64
	DT         time.Time
}

type coder interface{ Code() int }

func (r *MyRecord) Scan(src interface{}) error {
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	id, err := obj.Get("ID")
	if err != nil {
		return err
	}
	r.ID = id.(int64)

	txt, err := obj.Get("TXT")
	if err != nil {
		return err
	}
	r.Txt = txt.(string)

	dt, err := obj.Get("DT")
	if err != nil {
		return err
	}
	r.DT = dt.(time.Time)

	aclob, err := obj.Get("ACLOB")
	if err != nil {
		return err
	}
	if aclob != nil {
		if lob, ok := aclob.(*godror.Lob); ok && lob != nil {
			b, err := io.ReadAll(lob)
			r.AClob = string(b)
			return err
		}
	}
	return nil
}

// WriteObject update godror.Object with struct attributes values.
// Implement this method if you need the record as an input parameter.
func (r MyRecord) WriteObject() error {
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

	if r.Txt != "" {
		err = r.GetAttribute(&data, "TXT")
		if err != nil {
			return err
		}

		data.SetBytes([]byte(r.Txt))
		r.SetAttribute("TXT", &data)
	}

	if !r.DT.IsZero() {
		err = r.GetAttribute(&data, "DT")
		if err != nil {
			return err
		}
		data.SetTime(r.DT)
		r.SetAttribute("DT", &data)
	}

	if r.AClob != "" {
		err = r.GetAttribute(&data, "ACLOB")
		if err != nil {
			return err
		}
		data.SetBytes([]byte(r.AClob))
		r.SetAttribute("ACLOB", &data)
	}

	return nil
}

// MYTable represents TEST_PKG_TYPES.MY_TABLE
type MyTable struct {
	godror.ObjectCollection
	Items []*MyRecord
	conn  interface {
		NewData(baseType interface{}, sliceLen, bufSize int) ([]*godror.Data, error)
	}
}

func (t *MyTable) Scan(src interface{}) error {
	//fmt.Printf("Scan(%T(%#v))\n", src, src)
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	collection := obj.Collection()
	length, err := collection.Len()
	//fmt.Printf("Collection[%d] %#v: %+v\n", length, collection, err)
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	t.Items = make([]*MyRecord, 0, length)
	var i int
	for i, err = collection.First(); err == nil; i, err = collection.Next(i) {
		//fmt.Printf("Scan[%d]: %+v\n", i, err)
		var data godror.Data
		err = collection.GetItem(&data, i)
		if err != nil {
			return err
		}

		o := data.GetObject()
		defer o.Close()
		//fmt.Printf("%d. data=%#v => o=%#v\n", i, data, o)

		var item MyRecord
		err = item.Scan(o)
		//fmt.Printf("%d. item=%#v: %+v\n", i, item, err)
		if err != nil {
			return err
		}
		t.Items = append(t.Items, &item)
	}
	if err == godror.ErrNotExist {
		return nil
	}
	return err
}

func (r MyTable) WriteObject(ctx context.Context) error {
	if len(r.Items) == 0 {
		return nil
	}

	data, err := r.conn.NewData(r.Items[0], len(r.Items), 0)
	if err != nil {
		return err
	}

	for i, item := range r.Items {
		err = item.WriteObject()
		if err != nil {
			return err
		}
		d := data[i]
		d.SetObject(item.ObjectRef())
		r.Append(d)
	}
	return nil
}

func createPackages(ctx context.Context) error {
	qry := []string{`
	CREATE OR REPLACE PACKAGE test_pkg_types AS

	TYPE my_other_record IS RECORD (
		id    NUMBER(5),
		txt   VARCHAR2(200)
	);
	TYPE my_record IS RECORD (
		id    NUMBER(5),
		other test_pkg_types.my_other_record,
		txt   VARCHAR2(200),
		dt    DATE,
		aclob CLOB,
		child test_pkg_types.my_other_record,
		int32 PLS_INTEGER
	);
	TYPE my_table IS TABLE OF my_record;

	TYPE number_list IS TABLE OF NUMBER(10);

	TYPE osh_record IS RECORD (
		id    NUMBER(5),
		numbers test_pkg_types.number_list, 
		rec test_pkg_types.my_record
	);
	TYPE osh_table IS TABLE OF osh_record;
	
	END test_pkg_types;
	`,

		`CREATE OR REPLACE PACKAGE test_pkg_sample AS
	PROCEDURE test_record (
		id    IN    NUMBER,
		txt   IN    VARCHAR,
		rec   OUT   test_pkg_types.my_record
	);

	PROCEDURE test_record_in (
		rec IN OUT test_pkg_types.my_record
	);

	FUNCTION test_table (
		x NUMBER
	) RETURN test_pkg_types.my_table;

	PROCEDURE test_table_in (
		tb IN OUT test_pkg_types.my_table
	);
	
	PROCEDURE test_osh(
		numbers in test_pkg_types.number_list,
		res_list out test_pkg_types.osh_table
	);

	END test_pkg_sample;`,

		`CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS

	PROCEDURE test_record (
		id    IN    NUMBER,
		txt   IN    VARCHAR,
		rec   OUT   test_pkg_types.my_record
	) IS
	BEGIN
		rec.id := id;
		rec.txt := txt;
		rec.dt := SYSDATE;
		--DBMS_LOB.createtemporary(rec.aclob, TRUE);
		--DBMS_LOB.write(rec.aclob, LENGTH(txt)*2+1, 1, txt||'+'||txt);
	END test_record;

	PROCEDURE test_record_in (
		rec IN OUT test_pkg_types.my_record
	) IS
	BEGIN
		rec.id := rec.id + 1;
		rec.txt := rec.txt || ' changed '||TO_CHAR(rec.dt, 'YYYY-MM-DD HH24:MI:SS');
		rec.dt := SYSDATE;
		--IF rec.aclob IS NOT NULL AND DBMS_LOB.isopen(rec.aclob) <> 0 THEN
		--  DBMS_LOB.writeappend(rec.aclob, 7, ' changed');
		--END IF;
	END test_record_in;

	FUNCTION test_table (
		x NUMBER
	) RETURN test_pkg_types.my_table IS
		tb     test_pkg_types.my_table;
		item   test_pkg_types.my_record;
	BEGIN
		tb := test_pkg_types.my_table();
		FOR c IN (
			SELECT
				level "LEV"
			FROM
				"SYS"."DUAL" "A1"
			CONNECT BY
				level <= x
		) LOOP
			item.id := c.lev;
			item.dt := SYSDATE;
			item.txt := 'test - ' || ( c.lev * 2 );
			tb.extend();
			tb(tb.count) := item;
		END LOOP;

		RETURN tb;
	END test_table;

	PROCEDURE test_table_in (
		tb IN OUT test_pkg_types.my_table
	) IS
	BEGIN
	null;
	END test_table_in;

	PROCEDURE test_osh(
		numbers in test_pkg_types.number_list,
		res_list out test_pkg_types.osh_table
	) 
	AS
		rec test_pkg_types.osh_record;
	BEGIN
		res_list := test_pkg_types.osh_table();
				
		rec.numbers := numbers;
		
		FOR i IN 1..3 LOOP
			res_list.extend();
			rec.id := i;
			rec.rec.id := 2*i+1;  --initialize sub-record for #283
			rec.rec.dt := SYSDATE;
			--test_record(rec.id, rec.id, rec.rec);
			res_list(res_list.count) := rec;
		END LOOP;

	END test_osh;

	END test_pkg_sample;`}

	for _, ddl := range qry {
		_, err := testDb.ExecContext(ctx, ddl)
		if err != nil {
			return err

		}
	}

	errs, err := godror.GetCompileErrors(ctx, testDb, false)
	if err != nil {
		return err
	}
	for _, err := range errs {
		return err
	}

	return nil
}

func dropPackages(ctx context.Context) {
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_types`)
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_sample`)
}

type objectStruct struct {
	godror.ObjectTypeName `godror:"test_pkg_types.my_record" json:"-"`
	ID                    int32         `godror:"ID"`
	Txt                   string        `godror:"TXT"`
	DT                    time.Time     `godror:"DT"`
	AClob                 string        `godror:"ACLOB"`
	NInt32                sql.NullInt32 `godror:"INT32"`
}
type sliceStruct struct {
	godror.ObjectTypeName `json:"-"`
	ObjSlice              []objectStruct `godror:",type=test_pkg_types.my_table"`
}

type oshNumberList struct {
	godror.ObjectTypeName `json:"-"`

	NumberList []float64 `godror:",type=test_pkg_types.number_list"`
}

type oshStruct struct {
	godror.ObjectTypeName `godror:"test_pkg_types.osh_record" json:"-"`

	ID      int32         `godror:"ID"`
	Numbers oshNumberList `godror:"NUMBERS,type=test_pkg_types.number_list"`
	Record  objectStruct  `godror:"REC,type=test_pkg_types.my_record"`
}

type oshSliceStruct struct {
	godror.ObjectTypeName `json:"-"`

	List []oshStruct `godror:",type=test_pkg_types.osh_table"`
}

func TestPlSqlTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSqlTypes"), 30*time.Second)
	defer cancel()

	errOld := errors.New("client or server < 12")
	if err := godror.Raw(ctx, testDb, func(conn godror.Conn) error {
		serverVersion, err := conn.ServerVersion()
		if err != nil {
			return err
		}
		clientVersion, err := conn.ClientVersion()
		if err != nil {
			return err
		}

		if serverVersion.Version < 12 || clientVersion.Version < 12 {
			return errOld
		}
		return nil
	}); err != nil {
		if errors.Is(err, errOld) {
			t.Skip(err)
		} else {
			t.Fatal(err)
		}
	}

	if err := createPackages(ctx); err != nil {
		t.Fatal(err)
	}
	defer dropPackages(ctx)

	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		t.Fatal(err)
	}
	if false && Verbose {
		defer tl.enableLogging(t)()
		godror.SetLogger(zlog.NewT(t).SLog())
	}

	t.Run("Struct", func(t *testing.T) {
		var s objectStruct
		const qry = `begin test_pkg_sample.test_record(:1, :2, :3); end;`
		_, err := cx.ExecContext(ctx, qry, 43, "abraka dabra", sql.Out{Dest: &s})
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		t.Logf("struct: %+v", s)
	})

	t.Run("Slice", func(t *testing.T) {
		s := sliceStruct{ObjSlice: []objectStruct{{ID: 1, Txt: "first"}}}
		const qry = `begin test_pkg_sample.test_table_in(:1); end;`
		_, err := cx.ExecContext(ctx,
			qry,
			sql.Out{Dest: &s, In: true},
		)
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		t.Logf("struct: %+v", s)
	})

	t.Run("Osh", func(t *testing.T) {
		in := oshNumberList{NumberList: []float64{1, 2, 3}}
		var out oshSliceStruct
		const qry = `begin test_pkg_sample.test_osh(:1, :2); end;`
		_, err := cx.ExecContext(ctx, qry, in, sql.Out{Dest: &out})
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		} else if len(out.List) == 0 {
			t.Fatal("no records found")
		} else if out.List[0].ID != 1 || len(out.List[0].Numbers.NumberList) == 0 {
			t.Fatalf("wrong data from the array: %#v", out.List)
		}
		t.Logf("struct: %+v", out)
		if j, e := json.Marshal(out); e != nil {
			t.Fatalf("error during json conversion: %s", e)
		} else {
			t.Log("json marshaled: ", string(j))
		}
	})

	t.Run("Record", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		obj, err := objType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()

		for tName, tCase := range map[string]struct {
			txt  string
			want MyRecord
			ID   int64
		}{
			"default":    {ID: 1, txt: "test", want: MyRecord{Object: obj, ID: 1, Txt: "test"}},
			"emptyTxt":   {ID: 2, txt: "", want: MyRecord{Object: obj, ID: 2}},
			"zeroValues": {want: MyRecord{Object: obj}},
		} {
			rec := MyRecord{Object: obj}
			params := []interface{}{
				sql.Named("id", tCase.ID),
				sql.Named("txt", tCase.txt),
				sql.Named("rec", sql.Out{Dest: &rec}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_record(:id, :txt, :rec); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 21779 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if d := cmp.Diff(rec.String(), tCase.want.String()); d != "" {
				t.Errorf("%s: record got %v, wanted %v\n%s", tName, rec, tCase.want, d)
			}
		}
	})

	t.Run("Record IN OUT", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		for tName, tCase := range map[string]struct {
			wantTxt string
			in      MyRecord
			wantID  int64
		}{
			"zeroValues": {in: MyRecord{}, wantID: 1, wantTxt: " changed "},
			"default":    {in: MyRecord{ID: 1, Txt: "test"}, wantID: 2, wantTxt: "test changed "},
			"emptyTxt":   {in: MyRecord{ID: 2, Txt: ""}, wantID: 3, wantTxt: " changed "},
		} {

			obj, err := objType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			rec := MyRecord{Object: obj, ID: tCase.in.ID, Txt: tCase.in.Txt}
			params := []interface{}{
				sql.Named("rec", sql.Out{Dest: &rec, In: true}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 21779 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if rec.ID != tCase.wantID {
				t.Errorf("%s: ID got %d, wanted %d", tName, rec.ID, tCase.wantID)
			}
			if rec.Txt != tCase.wantTxt {
				t.Errorf("%s: Txt got %s, wanted %s", tName, rec.Txt, tCase.wantTxt)
			}
		}
	})

	t.Run("Table", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}

		items := []*MyRecord{{ID: 1, Txt: "test - 2"}, {ID: 2, Txt: "test - 4"}}

		for tName, tCase := range map[string]struct {
			want MyTable
			in   int64
		}{
			"one": {in: 1, want: MyTable{Items: items[:1]}},
			"two": {in: 2, want: MyTable{Items: items}},
		} {

			obj, err := objType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			tb := MyTable{ObjectCollection: obj.Collection(), conn: conn}
			params := []interface{}{
				sql.Named("x", tCase.in),
				sql.Named("tb", sql.Out{Dest: &tb}),
			}
			_, err = cx.ExecContext(ctx, `begin :tb := test_pkg_sample.test_table(:x); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 30757 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if len(tb.Items) != len(tCase.want.Items) {
				t.Errorf("%s: table got %v items, wanted %d items", tName, tb.Items, len(tCase.want.Items))
			} else {
				for i := 0; i < len(tb.Items); i++ {
					got := tb.Items[i]
					want := tCase.want.Items[i]
					if got.ID != want.ID {
						t.Errorf("%s: record ID got %v, wanted %v", tName, got.ID, want.ID)
					}
					if got.Txt != want.Txt {
						t.Errorf("%s: record TXT got %v, wanted %v", tName, got.Txt, want.Txt)
					}
				}
			}
		}
	})

	t.Run("Table IN", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		tableObjType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}

		recordObjType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

		items := make([]*MyRecord, 0)

		obj1, err := recordObjType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj1.Close()
		items = append(items, &MyRecord{ID: 1, Txt: "test - 2", Object: obj1})

		obj2, err := recordObjType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj2.Close()
		items = append(items, &MyRecord{ID: 2, Txt: "test - 4", Object: obj2})

		for tName, tCase := range map[string]struct {
			want MyTable
		}{
			"one": {want: MyTable{Items: items[:1]}},
			"two": {want: MyTable{Items: items}},
		} {

			obj, err := tableObjType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			tb := MyTable{ObjectCollection: obj.Collection(), Items: tCase.want.Items, conn: conn}
			params := []interface{}{
				sql.Named("tb", sql.Out{Dest: &tb, In: true}),
			}
			_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_table_in(:tb); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr) && cdr.Code() == 30757 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if len(tb.Items) != len(tCase.want.Items) {
				t.Errorf("%s: table got %v items, wanted %v items", tName, len(tb.Items), len(tCase.want.Items))
			} else {
				for i := 0; i < len(tb.Items); i++ {
					got := tb.Items[i]
					want := tCase.want.Items[i]
					if got.ID != want.ID {
						t.Errorf("%s: record ID got %v, wanted %v", tName, got.ID, want.ID)
					}
					if got.Txt != want.Txt {
						t.Errorf("%s: record TXT got %v, wanted %v", tName, got.Txt, want.Txt)
					}
				}
			}
		}
	})

}

func TestSelectObjectTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("SelectObjectTable"), 30*time.Second)
	defer cancel()
	const objTypeName, objTableName, pkgName = "test_selectObject", "test_selectObjTab", "test_selectObjPkg"
	cleanup := func() {
		testDb.Exec("DROP PACKAGE " + pkgName)
		testDb.Exec("DROP TYPE " + objTableName)
		testDb.Exec("DROP TYPE " + objTypeName)
	}
	cleanup()
	for _, qry := range []string{
		`CREATE OR REPLACE TYPE ` + objTypeName + ` AS OBJECT (
    AA NUMBER(2),
    BB NUMBER(13,2),
    CC NUMBER(13,2),
    DD NUMBER(13,2),
    MSG varchar2(100))`,
		"CREATE OR REPLACE TYPE " + objTableName + " AS TABLE OF " + objTypeName,
		`CREATE OR REPLACE PACKAGE ` + pkgName + ` AS
    function FUNC_1( p1 in varchar2, p2 in varchar2) RETURN ` + objTableName + `;
    END;`,
		`CREATE OR REPLACE PACKAGE BODY ` + pkgName + ` AS
	FUNCTION func_1( p1 IN VARCHAR2, p2 IN VARCHAR2) RETURN ` + objTableName + ` is
    	ret ` + objTableName + ` := ` + objTableName + `();
    begin
		ret.extend;
		ret(ret.count):= ` + objTypeName + `( 11, 22, 33, 44, p1||'success!'||p2);
		ret.extend;
		ret(ret.count):= ` + objTypeName + `( 55, 66, 77, 88, p1||'failed!'||p2);
		return ret;
	end;
	END;`,
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Error(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer cleanup()

	const qry = "select " + pkgName + ".FUNC_1('aa','bb') from dual"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	for rows.Next() {
		var objI interface{}
		if err = rows.Scan(&objI); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		obj := objI.(*godror.Object).Collection()
		defer obj.Close()
		t.Log(obj.FullName())
		i, err := obj.First()
		if err != nil {
			t.Fatal(err)
		}
		var objData, attrData godror.Data
		for {
			if err = obj.GetItem(&objData, i); err != nil {
				t.Fatal(err)
			}
			if err = objData.GetObject().GetAttribute(&attrData, "MSG"); err != nil {
				t.Fatal(err)
			}
			msg := string(attrData.GetBytes())

			t.Logf("%d. msg: %+v", i, msg)

			if i, err = obj.Next(i); err != nil {
				break
			}
		}
	}
}

func TestFuncBool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("FuncBool"), 3*time.Second)
	defer cancel()
	const pkgName = "test_bool"
	cleanup := func() { testDb.Exec("DROP PROCEDURE " + pkgName) }
	cleanup()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = godror.EnableDbmsOutput(ctx, conn); err != nil {
		t.Error(err)
	}
	const crQry = "CREATE OR REPLACE PROCEDURE " + pkgName + `(p_in IN BOOLEAN, p_not OUT BOOLEAN, p_num OUT NUMBER) IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('in='||(CASE WHEN p_in THEN 'Y' ELSE 'N' END));
  p_not := NOT p_in;
  p_num := CASE WHEN p_in THEN 1 ELSE 0 END;
END;`
	if _, err = conn.ExecContext(ctx, crQry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", crQry, err))
	}
	defer cleanup()

	const qry = "BEGIN " + pkgName + "(p_in=>:1, p_not=>:2, p_num=>:3); END;"
	var buf bytes.Buffer
	for _, in := range []bool{true, false} {
		var out bool
		var num int
		if _, err = conn.ExecContext(ctx, qry, in, sql.Out{Dest: &out}, sql.Out{Dest: &num}); err != nil {
			if srv, err := godror.ServerVersion(ctx, conn); err != nil {
				t.Log(err)
			} else if srv.Version < 18 {
				t.Skipf("%q: %v", qry, err)
			} else {
				t.Errorf("%q: %v", qry, err)
			}
			continue
		}
		t.Logf("in:%v not:%v num:%v", in, out, num)
		want := 0
		if in {
			want = 1
		}
		if num != want || out != !in {
			buf.Reset()
			if err = godror.ReadDbmsOutput(ctx, &buf, conn); err != nil {
				t.Error(err)
			}
			t.Log(buf.String())
			t.Errorf("got %v/%v wanted %v/%v", out, num, want, !in)
		}
	}
}

func TestPlSqlObjectDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSqlObjectDirect"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}

	const crea = `CREATE OR REPLACE PACKAGE test_pkg_obj IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(10), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;

  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ, p_int IN PLS_INTEGER);
END;`
	const crea2 = `CREATE OR REPLACE PACKAGE BODY test_pkg_obj IS
  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ, p_int IN PLS_INTEGER) IS
    v_idx PLS_INTEGER := NVL(p_obj.LAST, 0) + 1;
  BEGIN
    p_obj(v_idx).int := p_int;
    p_obj(v_idx).num := 314/100;
	p_obj(v_idx).vc  := 'abraka';
	p_obj(v_idx).c   := 'X';
	p_obj(v_idx).dt  := SYSDATE;
  END modify;
END;`
	if err = prepExec(ctx, testCon, crea); err != nil {
		t.Fatal(err)
	}
	//defer prepExec(ctx, testCon, "DROP PACKAGE test_pkg_obj")
	if err = prepExec(ctx, testCon, crea2); err != nil {
		t.Fatal(err)
	}

	//defer tl.enableLogging(t)()
	clientVersion, _ := godror.ClientVersion(ctx, testDb)
	serverVersion, _ := godror.ServerVersion(ctx, testDb)
	t.Logf("clientVersion: %#v, serverVersion: %#v", clientVersion, serverVersion)
	cOt, err := testCon.GetObjectType(strings.ToUpper("test_pkg_obj.tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatalf("%+v", err)
		}
		t.Log(err)
		t.Skipf("client=%d or server=%d < 12", clientVersion.Version, serverVersion.Version)
	}
	t.Log(cOt)

	// create object from the type
	coll, err := cOt.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer coll.Close()

	// create an element object
	elt, err := cOt.CollectionOf.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer elt.Close()
	elt.ResetAttributes()
	if err = elt.Set("C", "Z"); err != nil {
		t.Fatal(err)
	}
	if err = elt.Set("INT", int32(-2)); err != nil {
		t.Fatal(err)
	}

	// append to the collection
	t.Logf("elt: %s", elt)
	coll.AppendObject(elt)

	const mod = "BEGIN test_pkg_obj.modify(:1, :2); END;"
	if err = prepExec(ctx, testCon, mod,
		driver.NamedValue{Ordinal: 1, Value: coll},
		driver.NamedValue{Ordinal: 2, Value: 42},
	); err != nil {
		t.Error(err)
	}
	t.Logf("coll: %s", coll)
	var data godror.Data
	for i, err := coll.First(); err == nil; i, err = coll.Next(i) {
		if err = coll.GetItem(&data, i); err != nil {
			t.Fatal(err)
		}
		elt.ResetAttributes()
		elt = data.GetObject()

		t.Logf("elt[%d]: %s", i, elt)
		for _, attr := range elt.AttributeNames() {
			val, err := elt.Get(attr)
			if err != nil {
				if godror.DpiVersionNumber <= 30201 {
					t.Log(err, attr)
				} else {
					t.Error(err, attr)
				}
			}
			t.Logf("elt[%d].%s=%v", i, attr, val)
		}
	}
}
func prepExec(ctx context.Context, testCon driver.ConnPrepareContext, qry string, args ...driver.NamedValue) error {
	stmt, err := testCon.PrepareContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	_, err = stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	stmt.Close()
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	return nil
}

func TestPlSqlObject(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("PlSqlObject"), 10*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pkg := strings.ToUpper("test_pkg_obj" + tblSuffix)
	qry := `CREATE OR REPLACE PACKAGE ` + pkg + ` IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(1000), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;
END;`
	if _, err = conn.ExecContext(ctx, qry); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP PACKAGE " + pkg)

	defer tl.enableLogging(t)()
	ot, err := godror.GetObjectType(ctx, conn, pkg+strings.ToUpper(".int_tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatalf("%+v", err)
		}
		t.Log(err)
		t.Skip("client or server version < 12")
	}
	t.Log(ot)
}

func TestCallWithObject(t *testing.T) {
	t.Parallel()
	cleanup := func() {
		for _, drop := range []string{
			"DROP PROCEDURE test_cwo_getSum",
			"DROP TYPE test_cwo_tbl_t",
			"DROP TYPE test_cwo_rec_t",
		} {
			testDb.Exec(drop)
		}
	}

	const crea = `CREATE OR REPLACE TYPE test_cwo_rec_t FORCE AS OBJECT (
  numberpart1 VARCHAR2(6),
  numberpart2 VARCHAR2(10),
  code VARCHAR(7),
  CONSTRUCTOR FUNCTION test_cwo_rec_t RETURN SELF AS RESULT
);

CREATE OR REPLACE TYPE test_cwo_tbl_t FORCE AS TABLE OF test_cwo_rec_t;

CREATE OR REPLACE PROCEDURE test_cwo_getSum(
  p_operation_id IN OUT VARCHAR2,
  a_languagecode_i IN VARCHAR2,
  a_username_i IN VARCHAR2,
  a_channelcode_i IN VARCHAR2,
  a_mcalist_i IN test_cwo_tbl_t,
  a_validfrom_i IN DATE,
  a_validto_i IN DATE,
  a_statuscode_list_i IN VARCHAR2 ,
  a_type_list_o OUT SYS_REFCURSOR
) IS
  cnt PLS_INTEGER;
BEGIN
  cnt := a_mcalist_i.COUNT;
  OPEN a_type_list_o FOR
    SELECT cnt FROM DUAL;
END;
`

	ctx, cancel := context.WithTimeout(testContext("CallWithObject"), time.Minute)
	defer cancel()

	cleanup()
	for _, qry := range strings.Split(crea, "CREATE OR") {
		if qry == "" {
			continue
		}
		qry = "CREATE OR" + qry
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
	}

	var p_operation_id string
	var a_languagecode_i string
	var a_username_i string
	var a_channelcode_i string
	var a_mcalist_i *godror.Object
	var a_validfrom_i string
	var a_validto_i string
	var a_statuscode_list_i string
	var a_type_list_o driver.Rows

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	typ, err := godror.GetObjectType(ctx, conn, "test_cwo_tbl_t")
	if err != nil {
		t.Fatal(err)
	}
	defer typ.Close()
	if a_mcalist_i, err = typ.NewObject(); err != nil {
		t.Fatal(err)
	}
	defer a_mcalist_i.Close()
	if typ, err = godror.GetObjectType(ctx, conn, "test_cwo_rec_t"); err != nil {
		t.Fatal("GetObjectType(test_cwo_rec_t):", err)
	}
	defer typ.Close()
	elt, err := typ.NewObject()
	if err != nil {
		t.Fatalf("NewObject(%s): %+v", typ, err)
	}
	defer elt.Close()
	if err = elt.Set("NUMBERPART1", "np1"); err != nil {
		t.Fatal("set NUMBERPART1:", err)
	}
	if err = a_mcalist_i.Collection().Append(elt); err != nil {
		t.Fatal("append to collection:", err)
	}
	elt.Close()

	const qry = `BEGIN test_cwo_getSum(:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9); END;`
	if _, err := conn.ExecContext(ctx, qry,
		sql.Named("v1", sql.Out{Dest: &p_operation_id, In: true}),
		sql.Named("v2", &a_languagecode_i),
		sql.Named("v3", &a_username_i),
		sql.Named("v4", &a_channelcode_i),
		sql.Named("v5", &a_mcalist_i),
		sql.Named("v6", &a_validfrom_i),
		sql.Named("v7", &a_validto_i),
		sql.Named("v8", &a_statuscode_list_i),
		sql.Named("v9", sql.Out{Dest: &a_type_list_o}),
	); err != nil {
		t.Fatal(err)
	}
	defer a_type_list_o.Close()
	t.Logf("%[1]p %#[1]v", a_type_list_o)

	rows, err := godror.WrapRows(ctx, conn, a_type_list_o)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var n int
		if err = rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		i++
		t.Logf("%d. %d", i, n)
	}
	// Test the Finalizers.
	runtime.GC()
}

func BenchmarkObjArray(b *testing.B) {
	cleanup := func() { testDb.Exec("DROP FUNCTION test_objarr"); testDb.Exec("DROP TYPE test_vc2000_arr") }
	cleanup()
	qry := "CREATE OR REPLACE TYPE test_vc2000_arr AS TABLE OF VARCHAR2(2000)"
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	defer cleanup()
	qry = `CREATE OR REPLACE FUNCTION test_objarr(p_arr IN test_vc2000_arr) RETURN PLS_INTEGER IS BEGIN RETURN p_arr.COUNT; END;`
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(fmt.Errorf("%s: %w", qry, err))
	}

	ctx, cancel := context.WithCancel(testContext("BenchmarObjArray"))
	defer cancel()

	b.Run("object", func(b *testing.B) {
		b.StopTimer()
		const qry = `BEGIN :1 := test_objarr(:2); END;`
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		defer stmt.Close()
		typ, err := godror.GetObjectType(ctx, testDb, "TEST_VC2000_ARR")
		if err != nil {
			b.Fatal(err)
		}
		obj, err := typ.NewObject()
		if err != nil {
			b.Fatal(err)
		}
		defer obj.Close()
		coll := obj.Collection()

		var rc int
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			length, err := coll.Len()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if b.N < 1024 {
				for length < b.N {
					if err = coll.Append(fmt.Sprintf("--test--%010d--", i)); err != nil {
						b.Fatal(err)
					}
					length++
				}
			}
			if _, err := stmt.ExecContext(ctx, sql.Out{Dest: &rc}, obj); err != nil {
				b.Fatal(err)
			}
			if rc != length {
				b.Error("got", rc, "wanted", length)
			}
		}
	})

	b.Run("plsarr", func(b *testing.B) {
		b.StopTimer()
		const qry = `DECLARE
  TYPE vc2000_tab_typ IS TABLE OF VARCHAR2(2000) INDEX BY PLS_INTEGER;
  v_tbl vc2000_tab_typ := :1;
  v_idx PLS_INTEGER;
  v_arr test_vc2000_arr := test_vc2000_arr();
BEGIN
  -- copy the PL/SQL associative array to the nested table:
  v_idx := v_tbl.FIRST;
  WHILE v_idx IS NOT NULL LOOP
    v_arr.EXTEND;
    v_arr(v_arr.LAST) := v_tbl(v_idx);
    v_idx := v_tbl.NEXT(v_idx);
  END LOOP;
  -- call the procedure:
  :2 := test_objarr(p_arr=>v_arr);
END;`
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatal(fmt.Errorf("%s: %w", qry, err))
		}
		defer stmt.Close()
		b.StartTimer()

		var rc int
		var array []string
		for i := 0; i < b.N; i++ {
			if b.N < 1024 {
				for len(array) < b.N {
					array = append(array, fmt.Sprintf("--test--%010d--", i))
				}
			}
			if _, err := stmt.ExecContext(ctx, godror.PlSQLArrays, array, sql.Out{Dest: &rc}); err != nil {
				b.Fatal(err)
			}
			if rc != len(array) {
				b.Error(rc)
			}
		}
	})
}

// See https://github.com/godror/godror/issues/179
func TestObjectTypeClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ObjectTypeClose"), 30*time.Second)
	defer cancel()
	const typeName = "test_typeclose_t"
	const del = `DROP TYPE ` + typeName + ` CASCADE`
	testDb.ExecContext(ctx, del)
	// createType
	const ddl = `create or replace type ` + typeName + ` force as object (
     id NUMBER(10),  
	 balance NUMBER(18));`
	_, err := testDb.ExecContext(ctx, ddl)
	if err != nil {
		t.Fatalf("%s: %+v", ddl, err)
	}
	defer testDb.ExecContext(context.Background(), del)

	getObjectType := func(ctx context.Context, db *sql.DB) error {
		cx, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer cx.Close()

		objType, err := godror.GetObjectType(ctx, cx, typeName)
		if err != nil {
			return err
		}
		defer objType.Close()

		return nil
	}

	maxConn := maxSessions * 2
	for j := 0; j < 5; j++ {
		t.Logf("Run %d group\n", j)
		var start sync.WaitGroup
		g, ctx := errgroup.WithContext(ctx)
		start.Add(1)
		for i := 0; i < maxConn/2; i++ {
			g.Go(func() error {
				start.Wait()
				return getObjectType(ctx, testDb)
			})
		}
		start.Done()
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

// See https://github.com/godror/godror/issues/180
func TestSubObjectTypeClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("SubObjectTypeClose"), 30*time.Second)
	defer cancel()
	const typeName = "test_subtypeclose"
	dels := []string{
		`DROP TYPE ` + typeName + `_ot CASCADE`,
		`DROP TYPE ` + typeName + `_lt CASCADE`,
	}
	for _, del := range dels {
		testDb.ExecContext(ctx, del)
	}
	// createType
	for _, ddl := range []string{
		`CREATE OR REPLACE TYPE ` + typeName + `_lt FORCE AS VARRAY(30) OF VARCHAR2(30);`,
		`CREATE OR REPLACE TYPE ` + typeName + `_ot FORCE AS OBJECT (
     id NUMBER(10),  
	 list ` + typeName + `_lt);`,
	} {
		_, err := testDb.ExecContext(ctx, ddl)
		if err != nil {
			t.Fatalf("%s: %+v", ddl, err)
		}
	}
	defer func() {
		for _, del := range dels {
			_, _ = testDb.ExecContext(context.Background(), del)
		}
	}()

	getObjectType := func(ctx context.Context, db *sql.DB) error {
		cx, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer cx.Close()

		objType, err := godror.GetObjectType(ctx, cx, typeName+"_ot")
		if err != nil {
			return err
		}
		defer objType.Close()

		return nil
	}

	maxConn := maxSessions * 2
	for j := 0; j < 5; j++ {
		t.Logf("Run %d group\n", j)
		var start sync.WaitGroup
		g, ctx := errgroup.WithContext(ctx)
		start.Add(1)
		for i := 0; i < maxConn/2; i++ {
			g.Go(func() error {
				start.Wait()
				return getObjectType(ctx, testDb)
			})
		}
		start.Done()
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestObjectGetList(t *testing.T) {
	t.Parallel()
	tblSuffix := "_OL_" + tblSuffix
	objects := []struct {
		Name, Type, Create string
	}{
		{"TEST_CHECK_BLOCKED_REV_BEC" + tblSuffix, "PROCEDURE",
			`(p_id IN NUMBER, p_list OUT test_consolidation_rev_bec_list` + tblSuffix + `) IS 
BEGIN 
  p_list := test_consolidation_rev_bec_list` + tblSuffix + `();
  p_list.extend;
  p_list(1) := test_consolidation_rev_bec_rec` + tblSuffix + `(cons_year=>2021, cons_01=>p_id);
  p_list.extend;
  p_list(2) := NULL;
END;`},
		{"TEST_CONSOLIDATION_REV_BEC_LIST" + tblSuffix, "TYPE", " IS TABLE OF test_consolidation_rev_bec_rec" + tblSuffix},
		{"TEST_CONSOLIDATION_REV_BEC_REC" + tblSuffix, "TYPE", ` IS OBJECT (
         cons_year       number,
         cons_01         number)`},
	}
	drop := func(ctx context.Context) {
		for _, obj := range objects {
			qry := "DROP " + obj.Type + " " + obj.Name
			if _, err := testDb.ExecContext(ctx, qry); err != nil {
				var ec interface{ Code() int }
				if !(errors.As(err, &ec) && ec.Code() == 4043) {
					t.Logf("%s: %+v", qry, err)
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(testContext("ObjectGetList"), 30*time.Second)
	defer cancel()

	drop(ctx)
	defer drop(context.Background())
	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]
		qry := "CREATE OR REPLACE " + obj.Type + " " + obj.Name + obj.Create
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
		t.Fatal(err)
	} else if len(ces) != 0 {
		t.Fatal(ces)
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	qry := "CALL " + objects[0].Name + "(:1, :2)"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer stmt.Close()

	rt, err := godror.GetObjectType(ctx, conn, objects[1].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rs, err := rt.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	t.Logf("rs: %#v", rs)

	if _, err := stmt.ExecContext(ctx, "10212", sql.Out{Dest: &rs}); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	t.Log("rs:", rs.String())

	t.Run("direct", func(t *testing.T) {
		length, err := rs.Len()
		t.Logf("length: %d", length)
		if err != nil {
			t.Fatal(err)
		} else if want := 2; length != want {
			t.Errorf("length=%d wanted %d", length, want)
		}

		for i, err := rs.First(); err == nil; i, err = rs.Next(i) {
			elt, err := rs.Get(i)
			t.Logf("%d. %v (%+v)", i, elt, err)
			o := elt.(*godror.Object)
			if o == nil {
				continue
			}
			yearI, err := o.Get("CONS_YEAR")
			t.Logf("year: %T(%#v) (%+v)", yearI, yearI, err)
			year := yearI.(float64)
			if !(err == nil && year == 2021) {
				o.Close()
				t.Errorf("got (%#v, %+v), wanted (2021, nil)", year, err)
			}

			m, err := o.AsMap(true)
			o.Close()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d. %v", i, m)
		}
	})

	t.Run("AsMapSlice", func(t *testing.T) {
		m, err := rs.AsMapSlice(true)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("mapSlice:", m)
		want := []map[string]interface{}{
			{"CONS_YEAR": float64(2021), "CONS_01": float64(10212)},
			nil,
		}
		if d := cmp.Diff(m, want); d != "" {
			t.Error(d)
		}
	})

	t.Run("ToJSON", func(t *testing.T) {
		var buf strings.Builder
		if err := rs.ToJSON(&buf); err != nil {
			t.Fatal(err)
		}
		t.Log(buf.String())
		if got, want := buf.String(), `[{"CONS_01":10212,"CONS_YEAR":2021},nil]`; got != want {
			t.Errorf("got %q, wanted %q", got, want)
		}
	})
}

func TestObjectInObject(t *testing.T) {
	t.Parallel()
	tblSuffix := "_OO_" + tblSuffix
	objects := []struct {
		Name, Type, Create string
	}{
		{"TEST_GET_CARD_DETAIL" + tblSuffix, "PROCEDURE",
			`(p_detail           OUT TEST_DETAIL_REC` + tblSuffix + `) IS
BEGIN
  p_detail := test_detail_rec` + tblSuffix + `(
    string=>'some nice string',
    history=>TEST_CARD_HISTORY_LIST` + tblSuffix + `(
      TEST_CARD_HISTORY_REC` + tblSuffix + `(status=>'happyEND'))
  );
END;`},
		{"TEST_DETAIL_REC" + tblSuffix, "TYPE", ` IS OBJECT
  (string      varchar2(2000),
   history TEST_CARD_HISTORY_LIST` + tblSuffix + `);`},
		{"TEST_CARD_HISTORY_LIST" + tblSuffix, `TYPE`, ` IS TABLE OF TEST_CARD_HISTORY_REC` + tblSuffix + `;`},
		{"TEST_CARD_HISTORY_REC" + tblSuffix, `TYPE`, ` IS OBJECT
  (status varchar2(200));`},
	}
	drop := func(ctx context.Context) {
		for _, obj := range objects {
			qry := "DROP " + obj.Type + " " + obj.Name
			if obj.Type == "TYPE" {
				qry += " FORCE"
			}
			if _, err := testDb.ExecContext(ctx, qry); err != nil {
				var ec interface{ Code() int }
				if !(errors.As(err, &ec) && ec.Code() == 4043) {
					t.Logf("%s: %+v", qry, err)
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(testContext("ObjectInObject"), 30*time.Second)
	defer cancel()

	drop(ctx)
	defer drop(context.Background())
	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]
		qry := "CREATE OR REPLACE " + obj.Type + " " + obj.Name + obj.Create
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
		t.Fatal(err)
	} else if len(ces) != 0 {
		t.Fatal(ces)
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	qry := "CALL " + objects[0].Name + "(:1)"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer stmt.Close()

	rt, err := godror.GetObjectType(ctx, conn, objects[1].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rs, err := rt.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if _, err := stmt.ExecContext(ctx, sql.Out{Dest: rs}); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	t.Log("rs:", rs.String())

	t.Run("direct", func(t *testing.T) {
		textI, err := rs.Get("STRING")
		t.Logf("text: %T(%#v) (%+v)", textI, textI, err)
		text := textI.(string)
		if !(err == nil && text == "some nice string") {
			t.Errorf("got (%#v, %+v), wanted ('some nice string', nil)", text, err)
		}

		hc, err := rs.Get("HISTORY")
		if err != nil {
			t.Fatal(err)
		}
		hs := hc.(*godror.ObjectCollection)
		defer hs.Close()
		length, err := hs.Len()
		t.Logf("length: %d", length)
		if err != nil {
			t.Fatal(err)
		} else if want := 1; length != want {
			t.Errorf("length=%d wanted %d", length, want)
		}

		for i, err := hs.First(); err == nil; i, err = hs.Next(i) {
			elt, err := hs.Get(i)
			if err != nil {
				t.Fatalf("%d. %+v", i, err)
			}
			t.Logf("%d. %v (%+v)", i, elt, err)
			o := elt.(*godror.Object)
			if o == nil {
				continue
			}
			statusI, err := o.Get("STATUS")
			o.Close()
			if err != nil {
				t.Error(err)
			}
			t.Logf("%d. status: %+v", i, statusI)

		}
	})

	t.Run("AsMap", func(t *testing.T) {
		m, err := rs.AsMap(true)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("AsMap:", m)
		want := map[string]interface{}{
			"STRING": "some nice string",
			"HISTORY": []map[string]interface{}{
				{"STATUS": "happyEND"},
			},
		}
		if d := cmp.Diff(m, want); d != "" {
			t.Error(d)
		}
	})

	t.Run("ToJSON", func(t *testing.T) {
		var buf strings.Builder
		if err := rs.ToJSON(&buf); err != nil {
			t.Fatal(err)
		}
		t.Log(buf.String())
		if got, want := buf.String(), `{"HISTORY":[{"STATUS":"happyEND"}],"STRING":"some nice string"}`; want != got {
			t.Errorf("got %q, wanted %q", got, want)
		}
	})
}

func TestObjectFromMap(t *testing.T) {
	t.Parallel()
	tblSuffix := "_OM_" + tblSuffix
	objects := []struct {
		Name, Type, Create string
	}{
		{"TEST_DETAIL_REC" + tblSuffix, "TYPE", ` IS OBJECT (string varchar2(2000),history TEST_CARD_HISTORY_LIST` + tblSuffix + `);`},
		{"TEST_CARD_HISTORY_LIST" + tblSuffix, `TYPE`, ` IS TABLE OF TEST_CARD_HISTORY_REC` + tblSuffix + `;`},
		{"TEST_CARD_HISTORY_REC" + tblSuffix, `TYPE`, ` IS OBJECT (status varchar2(200));`},
	}

	drop := func(ctx context.Context) {
		for _, obj := range objects {
			qry := "DROP " + obj.Type + " " + obj.Name
			if obj.Type == "TYPE" {
				qry += " FORCE"
			}
			if _, err := testDb.ExecContext(ctx, qry); err != nil {
				var ec interface{ Code() int }
				if !(errors.As(err, &ec) && ec.Code() == 4043) {
					t.Logf("%s: %+v", qry, err)
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(testContext("ObjectInObject"), 30*time.Second)
	defer cancel()

	drop(ctx)
	defer drop(context.Background())
	for i := len(objects) - 1; i >= 0; i-- {
		obj := objects[i]
		qry := "CREATE OR REPLACE " + obj.Type + " " + obj.Name + obj.Create
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
		t.Fatal(err)
	} else if len(ces) != 0 {
		t.Fatal(ces)
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rt, err := godror.GetObjectType(ctx, conn, objects[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rs, err := rt.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()

	m := make(map[string]interface{})
	m["STRING"] = "String value"
	arrM := make([]map[string]interface{}, 0, 2)
	for i := 0; i < 2; i++ {
		app := make(map[string]interface{})
		var stat string
		if i%2 == 0 {
			stat = "even"
		} else {
			stat = "odd"
		}
		app["STATUS"] = stat
		arrM = append(arrM, app)
	}
	m["HISTORY"] = arrM
	if err := rs.FromMap(true, m); err != nil {
		t.Fatal(err)
	}

	const jsonString = `{"HISTORY":[{"STATUS":"even"},{"STATUS":"odd"}],"STRING":"String value"}`
	var want map[string]interface{}
	if err := json.Unmarshal([]byte(jsonString), &want); err != nil {
		t.Fatal(err)
	}
	got, err := rs.AsMap(true)
	if err != nil {
		t.Fatal(err)
	}

	if d := cmp.Diff(fmt.Sprintf("%v", want), fmt.Sprintf("%v", got)); d != "" {
		t.Fatal(d)
	}

	obj, err := rt.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	if err = obj.FromJSON(json.NewDecoder(strings.NewReader(jsonString))); err != nil {
		t.Fatalf("FromJSON: %+v", err)
	}
	var buf strings.Builder
	if err = obj.ToJSON(&buf); err != nil {
		t.Fatalf("ToJSON: %+v", err)
	}
	obj.Close()
	if d := cmp.Diff(jsonString, buf.String()); d != "" {
		t.Errorf("ToJSON: %s", d)
	}
}

func TestObjectWithNativeSlice(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ObjectWithNativeSlice"), 10*time.Second)
	defer cancel()
	t.Logf("dbstats: %#v", testDb.Stats())
	name := "test_ons" + tblSuffix

	cleanup := func() {
		testDb.Exec("DROP PROCEDURE " + name + "_proc")
		testDb.Exec("DROP TYPE " + name + "_obj")
		testDb.Exec("DROP TYPE " + name + "_at")
	}
	cleanup()
	crea := []string{
		`CREATE OR REPLACE TYPE ` + name + `_at AS TABLE OF varchar2(4000);`,
		`CREATE OR REPLACE TYPE ` + name + `_obj AS OBJECT (dt ` + name + `_at);`,
		`CREATE OR REPLACE PROCEDURE ` + name + `_proc(p_oo out ` + name + `_obj) IS
  v_arr ` + name + `_at := ` + name + `_at();
BEGIN
  v_arr.EXTEND;
  v_arr(1) := 'let it work';
  p_oo := ` + name + `_obj(v_arr);
END;`,
	}

	for _, q := range crea {
		if _, err := testDb.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %+v", q, err)
		}
	}
	defer cleanup()
	if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
		t.Fatal(err)
	} else if len(ces) != 0 {
		t.Fatal(ces)
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer testCon.Close()
	if err = testCon.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	qry := "CALL " + name + "_proc(:1)"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer stmt.Close()

	cOt, err := testCon.GetObjectType(strings.ToUpper(name + "_obj"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ObjectType:", cOt)

	cO, err := cOt.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer cO.Close()
	t.Log(cOt)

	if _, err = stmt.ExecContext(ctx,
		sql.Out{Dest: cO},
	); err != nil {
		t.Errorf("%s: %+v", qry, err)
	}

	if cO.ObjectType != nil {
		m, err := cO.AsMap(true)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("map:", m)
	}
}

func TestInputWithNativeSlice(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("InputWithNativeSlice"), 10*time.Second)
	defer cancel()
	t.Logf("dbstats: %#v", testDb.Stats())
	name := "test_ons" + tblSuffix

	cleanup := func() {
		testDb.Exec("DROP PROCEDURE " + name + "_proc")
		testDb.Exec("DROP TYPE " + name + "_list")
	}
	cleanup()
	defer cleanup()
	create_query := []string{
		`CREATE OR REPLACE TYPE ` + name + `_list AS TABLE OF NUMBER;`,
		`CREATE OR REPLACE PROCEDURE ` + name + `_proc(p_in IN ` + name + `_list, p_out OUT NUMBER) IS
         v_result NUMBER := 0;
         BEGIN
           FOR v IN 1..p_in.COUNT LOOP
             v_result := v_result + p_in(v);
           END LOOP;
		
           p_out := v_result;
         END;`,
	}

	for _, q := range create_query {
		if _, err := testDb.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %+v", q, err)
		}
	}
	if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
		t.Fatal(err)
	} else if len(ces) != 0 {
		t.Fatal(ces)
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	testCon, err := godror.DriverConn(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	defer testCon.Close()
	if err = testCon.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	qry := "CALL " + name + "_proc(:1, :2)"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer stmt.Close()

	cCt, err := testCon.GetObjectType(strings.ToUpper(name + "_list"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ObjectType:", cCt)

	cC, err := cCt.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()
	fSlice := []float64{1, 3.14}
	var want float64
	iSlice := make([]interface{}, len(fSlice))
	for i, f := range fSlice {
		iSlice[i] = f
		want += f
	}
	if err = cC.FromSlice(iSlice); err != nil {
		t.Fatal(err)
	}
	length, err := cC.Len()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("length of cC:", length)

	var sum float64
	if _, err = stmt.ExecContext(ctx,
		cC,
		sql.Out{Dest: &sum},
	); err != nil {
		t.Errorf("%s: %+v", qry, err)
	}

	t.Log("Sum:", sum)
	if fmt.Sprintf("%f", sum) != fmt.Sprintf("%f", want) {
		t.Errorf("got %f, wanted %f", sum, want)
	}
}

func TestPlSQLNumSlice(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("PlSQLNumSlice"), 30*time.Second)
	defer cancel()

	name := "test_plsql_numslice" + tblSuffix
	dropQry := `DROP PACKAGE ` + name
	_, _ = testDb.ExecContext(ctx, dropQry)
	defer func() { _, _ = testDb.ExecContext(context.Background(), dropQry) }()

	for _, creQry := range []string{
		`CREATE OR REPLACE PACKAGE ` + name + ` IS
subtype type_Product_Code is varchar2(6);
subtype type_Number_String is varchar2(20);
type type_Product_Code_Array is table of type_Product_Code index by pls_integer;
type type_Number_String_Array is table of type_Number_String index by pls_integer;

FUNCTION Basket_Add_Product(
  pProductCodeTable  in type_Product_Code_Array,
  pVolumeTable       in type_Number_String_Array,
  pCollectionIdTable in type_Number_String_Array
) RETURN VARCHAR2;
END;`,
		`CREATE OR REPLACE PACKAGE BODY ` + name + ` IS

FUNCTION Basket_Add_Product(
  pProductCodeTable  in type_Product_Code_Array,
  pVolumeTable       in type_Number_String_Array,
  pCollectionIdTable in type_Number_String_Array
) RETURN VARCHAR2 IS
  v_out VARCHAR2(4000);
BEGIN
  v_out := pProductCodeTable.COUNT||','||pVolumeTable.COUNT||','||pCollectionIdTable.COUNT||CHR(10);
  FOR i IN pProductCodeTable.FIRST .. pProductCodeTable.LAST LOOP
    v_out := v_out||'pProductCodeTable('||i||')='||pProductCodeTable(i)||CHR(10);
  END LOOP;

  FOR i IN pVolumeTable.FIRST .. pVolumeTable.LAST LOOP
    v_out := v_out||'pVolumeTable('||i||')='||pVolumeTable(i)||CHR(10);
  END LOOP;

  FOR i IN pCollectionIdTable.FIRST .. pCollectionIdTable.LAST LOOP
    v_out := v_out||'pCollectionIdTable('||i||')='||pCollectionIdTable(i)||CHR(10);
  END LOOP;
   
  RETURN(v_out);

END;
END;`,
	} {
		if _, err := testDb.ExecContext(ctx, creQry); err != nil {
			t.Fatalf("%s: %+v", creQry, err)
		}
	}
	errs, gcErr := godror.GetCompileErrors(ctx, testDb, false)
	if gcErr != nil {
		t.Error(gcErr)
	} else if len(errs) != 0 {
		t.Fatal(errs)
	}

	ExecProcedureVarChar := func(ctx context.Context, params ...interface{}) (result string, err error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
				var ok bool
				if err, ok = r.(error); !ok {
					err = fmt.Errorf("Recover from ExecProcedure: %v", r)
				}
			}
		}()

		db := testDb

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		var out string
		requestParams := make([]interface{}, 0, 2+len(params))
		requestParams = append(requestParams, godror.PlSQLArrays)
		requestParams = append(requestParams, sql.Out{Dest: &out})
		requestParams = append(requestParams, params...)
		qry := "BEGIN :1 := " + name + ".basket_add_product(:2, :3, :4); END;"
		_, err = db.ExecContext(ctx, qry, requestParams...)
		if err != nil {
			return "", fmt.Errorf("%s [%v]: %w", qry, requestParams, err)
		}

		return out, err
	}

	s, err := ExecProcedureVarChar(ctx,
		[]string{"012345", "abcdef"},
		[]string{"A", "B", "C"},
		[]string{"f56d430cxx123", "le;ri';ghr", "asdasf", "rretwrt"})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(s, len(s))
}

func TestXMLType(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("XMLType"), 30*time.Second)
	defer cancel()

	const qry = "SELECT 'text' AS text, XMLElement(\"Date\", SYSDATE) AS xml FROM DUAL"
	var txt, xml string
	if err := testDb.QueryRowContext(ctx, qry).Scan(&txt, &xml); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	t.Logf("txt=%q xml=%q", txt, xml)

	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("coltype: %v, %v", types[0], types[1])
	defer rows.Close()
	for rows.Next() {
		var i, j interface{}
		if err = rows.Scan(&i, &j); err != nil {
			t.Fatal(err)
		}
		t.Logf("column[0]: (%[1]T) %#[1]v", i)
		t.Logf("column[1]: (%[1]T) %#[1]v", j)

	}
}

func TestBigXMLType(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("BigXMLType"), 30*time.Second)
	defer cancel()
	{
		_, _ = testDb.ExecContext(context.Background(), "DROP TABLE test_xml")
		const qry = "create table test_xml(id number,data xmltype)"
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%q: %+v", qry, err)
		}
		defer testDb.ExecContext(context.Background(), "DROP TABLE test_xml")
	}
	// defer db.Exec("drop table test_xml")

	const want = `<root>
  <person
  firstname="Marsiella"
  lastname="Schonfeld"
  city="Arbil"
  country="Nicaragua"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person2
  firstname="Emilia"
  lastname="Glovsky"
  city="Cali"
  country="Cyprus"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person3
  firstname="Dari"
  lastname="Mike"
  city="Mata-Utu"
  country="Belize"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person4
  firstname="Ericka"
  lastname="Swanhildas"
  city="Wichita"
  country="Somalia"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person5
  firstname="Luci"
  lastname="Gualtiero"
  city="Iquique"
  country="Korea, Republic of"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person6
  firstname="Ofilia"
  lastname="Ailyn"
  city="Sydney"
  country="Tanzania, United Republic of"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person7
  firstname="Edith"
  lastname="Dannye"
  city="Charlotte Amalie"
  country="Swaziland"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person8
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person9
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person10
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="Ardys"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person11
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="Ardys"
  lastname2="中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person12
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="Ardys"
  lastname2="中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person13
  firstname="Gratia"
  lastname="Ephrem"
  city="Portland"
  country="Oman"
  firstname2="中文测试"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person14
  firstname="Gratia"
  lastname="Ephrem"
  city="中文测试中文测试中文测试中文测试"
  country="中文测试中文测试中文测试中文测试"
  firstname2="中文测试中文测试中文测试中文测试"
  lastname2="中文测试中文测试中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person15
  firstname="中文测试中文测试中文测试中文测试中文测试中文测试"
  lastname="中文测试中文测试中文测试中文测试"
  city="中文测试中文测试中文测试中文测试"
  country="中文测试中文测试中文测试中文测试"
  firstname2="中文测试中文测试中文测试中文测试"
  lastname2="中文测试中文测试中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person16
  firstname="中文测试中文测试中文测试中文测试"
  lastname="中文测试中文测试"
  city="中文测试中文测试中文测试中文测试"
  country="中文测试中文测试中文测试中文测试"
  firstname2="中文测试中文测试"
  lastname2="中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person17
  firstname="中文测试"
  lastname="中文测试"
  city="中文测试"
  country="中文测试"
  firstname2="中文测试"
  lastname2="中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person18
  firstname="中文测试中文测试"
  lastname="中文测试中文测试"
  city="中文测试中文测试"
  country="中文测试中文测试"
  firstname2="中文测试中文测试"
  lastname2="中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person19
  firstname="中文测试中文测试"
  lastname="中文测试中文测试"
  city="中文测试中文测试"
  country="中文测试中文测试"
  firstname2="中文测试中文测试"
  lastname2="Doig"
  email="Ardys.Doig@yopmail.com"
  />
  <person20
  firstname="中文测试中文测试"
  lastname="中文测试中文测试"
  city="中文测试中文测试"
  country="中文测试中文测试"
  firstname2="中文测试中文测试"
  lastname2="中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <person21
  firstname="Gratia"
  lastname="中文测试中文测试"
  city="中文测试中文测试"
  country="中文测试中文测试"
  firstname2="中文测试中文测试"
  lastname2="中文测试中文测试"
  email="Ardys.Doig@yopmail.com"
  />
  <random>38</random>
  <random_float>28.038</random_float>
  <bool>true</bool>
  <date>1983-01-07</date>
  <regEx>hell0</regEx>
  <enum>online</enum>
  <elt>Cherilyn</elt><elt>Frieda</elt><elt>Kimberley</elt><elt>Celestyna</elt><elt>Ethel</elt>  
  <Ardys>
    <age>34</age>
  </Ardys>
</root>`
	{
		const qry = "insert into test_xml(id,data) values (:1,:2)"
		if _, err := testDb.ExecContext(ctx, qry, 1, want); err != nil {
			t.Fatalf("insert %s", err)
		}
	}
	const qry = "select A.id, A.data.getClobVal() AS data FROM test_xml A"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cols {
		t.Logf("column %q: %q", c.Name(), c.DatabaseTypeName())
	}
	sRepl := strings.NewReplacer(" ", "", "\n", "", "\t", "")
	for rows.Next() {
		var id, data string
		if err := rows.Scan(&id, &data); err != nil {
			t.Fatal("scan: %+w", err)
		}
		t.Logf("id=%q data=%d", id, len(data))
		if d := cmp.Diff(sRepl.Replace(data), sRepl.Replace(want)); d != "" {
			t.Error(d)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

}

func TestArrayOfRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ArrayOfRecords"), 10*time.Second)
	defer cancel()
	name := "test_aor"

	cleanup := func() {
		testDb.Exec("DROP PACKAGE " + name + "_pkg")
	}
	cleanup()
	defer cleanup()
	create_query := []string{
		`CREATE OR REPLACE PACKAGE ` + name + `_pkg AS 
  TYPE ` + name + `_rt IS RECORD (
    ratecode_id PLS_INTEGER, 
    ratecode VARCHAR2(20), 
    ratecode_name VARCHAR2(100), 
    rate_structure VARCHAR2(20)
  );
  TYPE ` + name + `_tt IS TABLE OF ` + name + `_rt;

  PROCEDURE ` + name + `_proc(p_in IN ` + name + `_tt, p_out OUT VARCHAR2);
END;`,

		`CREATE OR REPLACE PACKAGE BODY ` + name + `_pkg AS
  PROCEDURE ` + name + `_proc(p_in IN ` + name + `_tt, p_out OUT VARCHAR2) IS
    v_result VARCHAR2(1000);
    v_idx PLS_INTEGER;
  BEGIN
    v_idx := p_in.FIRST;
    WHILE v_idx IS NOT NULL LOOP
      v_result := v_result || p_in(v_idx).ratecode_id||'='||p_in(v_idx).ratecode||', ';
      v_idx := p_in.NEXT(v_idx);
    END LOOP;
    p_out := v_result;
  END;
END;`,
	}

	for _, q := range create_query {
		if _, err := testDb.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %+v", q, err)
		}
		if ces, err := godror.GetCompileErrors(ctx, testDb, false); err != nil {
			t.Fatal(err)
		} else if len(ces) != 0 {
			t.Log(q)
			t.Fatal(ces)
		}
	}

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	qry := `begin ` + name + `_pkg.` + name + `_proc(:1, :2); end;`
	var resp string
	if _, err := conn.ExecContext(ctx, qry, RatePlan{RateCodes: []RateCode{
		{RateCodeId: 1, RateCode: "one"},
		{RateCodeId: 2, RateCode: "two"},
	}},
		sql.Out{Dest: &resp},
	); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	t.Log(resp)
}

type RatePlan struct {
	godror.ObjectTypeName `godror:"test_aor_pkg.test_aor_tt" json:"-"`
	RateCodes             []RateCode `json:"rateCodes"`
}

type RateCode struct {
	godror.ObjectTypeName `godror:"test_aor_pkg.test_aor_rt" json:"-"`
	RateCodeId            int    `godror:"RATECODE_ID" json:"rateCodeId" db:"RATECODE_ID"`
	RateCode              string `json:"rateCode" db:"RATECODE"`
	RateCodeName          string `godror:"RATECODE_NAME" json:"rateCodeName" db:"RATECODE_NAME"`
	RateStucture          string `godror:"RATE_STRUCTURE" json:"rateStucture" db:"RATE_STRUCTURE"`
}

type parentObject struct {
	godror.ObjectTypeName `godror:"test_parent_ot"`
	ID                    int         `godror:"ID"`
	Child                 childObject `godror:"CHILD"`
}
type childObject struct {
	godror.ObjectTypeName `godror:"test_child_ot"`
	ID                    int    `godror:"ID"`
	Name                  string `godror:"NAME"`
}

func TestIssue319(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Issue319"), 10*time.Second)
	defer cancel()
	if Verbose {
		godror.SetLogger(zlog.NewT(t).SLog())
		defer godror.SetLogger(slog.Default())
	}

	cleanup := func() {
		for _, qry := range []string{
			"DROP TYPE test_parent_ot",
			"DROP TYPE test_child_ot",
		} {
			if _, err := testDb.ExecContext(context.Background(), qry); err != nil {
				t.Logf("%s: %+v", qry, err)
			}
		}
	}

	cleanup()
	defer cleanup()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE test_child_ot AS OBJECT (id NUMBER(3), name VARCHAR2(128));",
		"CREATE OR REPLACE TYPE test_parent_ot AS OBJECT (id NUMBER(3), child test_child_ot);",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.MinSessions, P.MaxSessions = 1, 1
	t.Logf("params: %s", P)
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	Exec := func(ctx context.Context, qry string, params ...any) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		stmt, err := tx.PrepareContext(ctx, qry)
		if err != nil {
			t.Fatalf("prepare %s: %+v", qry, err)
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, params...)
		return err
	}

	for i := 0; i < 5; i++ {
		c := childObject{ID: 1, Name: "test"}
		p := parentObject{ID: 1, Child: c}
		var res int

		pre := db.Stats()
		if err := Exec(ctx, `DECLARE
  v_parent test_parent_ot;
BEGIN
  v_parent := :p;
  v_parent.id := v_parent.id + 1;
  :res := v_parent.id;
END;`,
			sql.Named("p", p),
			sql.Named("res", sql.Out{Dest: &res}),
		); err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
		t.Logf("%d. result: %d", i, res)
		post := db.Stats()
		t.Logf("\npre: %+v\npost: %+v", pre, post)
		if post.OpenConnections > pre.OpenConnections+1 {
			t.Errorf("session leakage: was %d, have %d open connections", pre.OpenConnections, post.OpenConnections)
		}
	}
}

func TestObjLobClose(t *testing.T) {
	const dropQry = `DROP TYPE test_clob_ot`
	cleanup := func() { testDb.ExecContext(context.Background(), dropQry) }
	cleanup()
	const qry = `create or replace type test_clob_ot as object(
  id number,
  value clob
)`
	ctx, cancel := context.WithTimeout(testContext("ObjLobClose"), 10*time.Second)
	defer cancel()
	if _, err := testDb.ExecContext(ctx, qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer cleanup()

	Exec := func(ctx context.Context, db *sql.DB, query string, variables ...any) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		_, err = stmt.ExecContext(ctx, variables...)
		return err
	}

	type clob struct {
		godror.ObjectTypeName `godror:"test_clob_ot"`
		Id                    float64 `godror:"ID"`
		Value                 string  `godror:"VALUE"`
	}

	testConnectInClob := func(ctx context.Context, db *sql.DB, i float64) {
		t.Log("start In clob", i)
		var c clob
		c.Id = 1
		c.Value = `test`

		var res float64
		err := Exec(ctx, db, `
	declare
		v_child test_clob_ot;
	begin
		v_child := :c;
		v_child.id := v_child.id + 5;
		:res := v_child.id;
	end;`,
			sql.Named(`c`, c),
			sql.Named(`res`, sql.Out{In: false, Dest: &res}),
		)
		t.Log("result", i, res, err)
	}

	testConnectOutClob := func(ctx context.Context, db *sql.DB, i float64) {
		t.Log("start Out clob", i)
		var c clob

		err := Exec(ctx, db, `
	declare
		v_child test_clob_ot := test_clob_ot(id => null, value => null);
	begin
		v_child.id :=:i + 5;
		v_child.value := 'hello world ' || v_child.id;
		:v_res := v_child;
	end;`,
			sql.Named(`i`, i),
			sql.Named(`v_res`, sql.Out{In: false, Dest: &c}),
		)
		t.Log("result", i, c, err)
	}

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}

	P.PoolParams.SessionTimeout = 30 * time.Second
	P.PoolParams.WaitTimeout = 10 * time.Second
	P.PoolParams.MaxLifeTime = 30 * time.Second
	P.PoolParams.SessionIncrement = 1
	P.PoolParams.MinSessions = 1
	P.PoolParams.MaxSessions = 3
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	for i := 0; i < 10; i++ {
		testConnectInClob(ctx, db, float64(i))
	}

	for i := 0; i < 10; i++ {
		testConnectOutClob(ctx, db, float64(i))
	}
}

type I323Child struct {
	godror.ObjectTypeName `godror:"I323CHILD"`
	ID                    float64 `godror:"ID" json:"id"`
	Name                  string  `godror:"NAME" json:"name"`
}

type I323ChildArray struct {
	godror.ObjectTypeName
	ChildArray []I323Child `godror:",type=I323CHILDARRAY" json:"childArray"`
}

type I323Parent struct {
	godror.ObjectTypeName `godror:"I323PARENT"`
	Id                    float64        `godror:"ID" json:"id"`
	Name                  string         `godror:"NAME" json:"name"`
	ChildArray            I323ChildArray `godror:",type=I323CHILDARRAY"`
}

type I323ParentArray struct {
	godror.ObjectTypeName
	ParentArray []I323Parent `godror:",type=I323PARENTARRAY" json:"parentArray"`
}

type I323Grand struct {
	godror.ObjectTypeName `godror:"I323GRAND"`
	ParentArray           I323ParentArray `godror:",type=I323PARENTARRAY"`
}

func TestIssue323(t *testing.T) {
	drop := func() {
		for _, qry := range []string{
			"DROP TYPE I323GRAND FORCE",
			"DROP TYPE I323PARENTARRAY FORCE",
			"DROP TYPE I323PARENT FORCE",
			"DROP TYPE I323CHILDARRAY FORCE",
			"DROP TYPE I323CHILD FORCE",
		} {
			if _, err := testDb.ExecContext(context.Background(), qry); err != nil {
				t.Logf("%s: %+v", qry, err)
			}
		}
	}
	drop()
	defer drop()

	ctx, cancel := context.WithTimeout(testContext("Issue323"), 10*time.Second)
	defer cancel()
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE I323CHILD AS OBJECT (ID NUMBER, NAME VARCHAR2(100))",
		"CREATE OR REPLACE TYPE I323CHILDARRAY AS TABLE OF I323CHILD",
		"CREATE OR REPLACE TYPE I323PARENT AS OBJECT(ID NUMBER, NAME varchar2(100), childArray I323CHILDARRAY)",
		"CREATE OR REPLACE TYPE I323PARENTARRAY AS TABLE OF I323PARENT",
		"CREATE OR REPLACE TYPE I323Grand AS OBJECT(ParentArray I323ParentArray)",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}

	qry := `
	declare
		procedure test(
			p_grand out I323Grand
		) is
			v_childArray I323ChildArray := I323ChildArray();
		begin
			p_grand :=  I323Grand(null);
			p_grand.parentArray := I323ParentArray();
			for i in 1..2 loop
				p_grand.parentArray.extend;
				p_grand.parentArray(p_grand.parentArray.count) := I323Parent(i, 'value ' || i, null);
				p_grand.ParentArray(p_grand.parentArray.count).childArray := I323ChildArray();
				v_childArray := I323ChildArray();
				for j in 1..2 loop
					v_childArray.extend;
					v_childArray(v_childArray.count) := I323Child(i*100 + j, 'coord '||i||'/'||j);
				end loop;
				p_grand.ParentArray(p_grand.parentArray.count).ChildArray := v_childArray;
			end loop;
		end;
	begin
		test(
			p_grand => :g
		);
	end;`

	if Verbose {
		godror.SetLogger(zlog.NewT(t).SLog())
		defer godror.SetLogger(slog.Default())
	}

	var grand I323Grand
	if _, err := testDb.ExecContext(ctx, qry,
		sql.Named(`g`, sql.Out{In: false, Dest: &grand}),
	); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	p, _ := json.Marshal(grand)
	t.Log("grand:", string(p))

	pa := grand.ParentArray.ParentArray
	if pa[0].ChildArray.ChildArray[0].ID ==
		pa[1].ChildArray.ChildArray[0].ID {
		t.Errorf("pa[0]=%#v\n==\n%#v=pa[1]", pa[0], pa[1])
	}
}
