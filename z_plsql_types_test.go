// Copyright 2019 Walter Wanderley
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package goracle_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"

	goracle "gopkg.in/goracle.v2"
)

var _ goracle.ObjectScanner = new(MyRecord)
var _ goracle.ObjectWriter = new(MyRecord)

var _ goracle.ObjectScanner = new(MyTable)

// MYRecord represents TEST_PKG_TYPES.MY_RECORD
type MyRecord struct {
	*goracle.Object
	ID  int64
	Txt string
}

func (r *MyRecord) Scan(src interface{}) error {

	switch obj := src.(type) {
	case *goracle.Object:
		id, err := obj.Get("ID")
		if err != nil {
			return err
		}
		r.ID = id.(int64)

		txt, err := obj.Get("TXT")
		if err != nil {
			return err
		}
		r.Txt = string(txt.([]byte))

	default:
		return fmt.Errorf("Cannot scan from type %T", src)
	}

	return nil
}

// WriteObject update goracle.Object with struct attributes values.
// Implement this method if you need the record as an input parameter.
func (r MyRecord) WriteObject() error {
	// all attributes must be initialized or you get an "ORA-21525: attribute number or (collection element at index) %s violated its constraints"
	err := r.ResetAttributes()
	if err != nil {
		return err
	}

	var data goracle.Data
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

	return nil
}

// MYTable represents TEST_PKG_TYPES.MY_TABLE
type MyTable struct {
	*goracle.ObjectCollection
	Items []*MyRecord
}

func (t *MyTable) Scan(src interface{}) error {

	switch obj := src.(type) {
	case *goracle.Object:
		collection := goracle.ObjectCollection{Object: obj}
		t.Items = make([]*MyRecord, 0)
		for i, err := collection.First(); err == nil; i, err = collection.Next(i) {
			var data goracle.Data
			err = collection.Get(&data, i)
			if err != nil {
				return err
			}

			o := data.GetObject()
			defer o.Close()

			var item MyRecord
			err = item.Scan(o)
			if err != nil {
				return err
			}
			t.Items = append(t.Items, &item)
		}

	default:
		return fmt.Errorf("Cannot scan from type %T", src)
	}

	return nil
}

func (r MyTable) WriteObject() error {
	if len(r.Items) == 0 {
		return nil
	}

	conn, err := goracle.DriverConn(testDb)
	if err != nil {
		return err
	}

	data, err := conn.NewData(r.Items[0], len(r.Items), 0)
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
	qry := []string{`CREATE OR REPLACE PACKAGE test_pkg_types AS
	TYPE my_record IS RECORD (
		id    NUMBER(5),
		txt   VARCHAR2(200)
	);
	TYPE my_table IS
		TABLE OF my_record;
	END test_pkg_types;`,

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
	END test_record;

	PROCEDURE test_record_in (
		rec IN OUT test_pkg_types.my_record
	) IS
	BEGIN
		rec.id := rec.id + 1;
		rec.txt := rec.txt || ' changed';
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

	END test_pkg_sample;`}

	for _, ddl := range qry {
		_, err := testDb.ExecContext(ctx, ddl)
		if err != nil {
			return err

		}
	}

	return nil
}

func dropPackages(ctx context.Context) {
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_types`)
	testDb.ExecContext(ctx, `DROP PACKAGE test_pkg_sample`)
}

func TestPLSQLTypes(t *testing.T) {
	t.Parallel()

	serverVersion, err := goracle.ServerVersion(testDb)
	if err != nil {
		t.Fatal(err)
	}
	clientVersion, err := goracle.ClientVersion(testDb)
	if err != nil {
		t.Fatal(err)
	}

	if serverVersion.Version < 12 || clientVersion.Version < 12 {
		t.Skip("client or server < 12")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = createPackages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dropPackages(ctx)

	conn, err := goracle.DriverConn(testDb)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Record", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}
		defer objType.Close()

		obj, err := objType.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()

		for tName, tCase := range map[string]struct {
			ID   int64
			txt  string
			want MyRecord
		}{
			"default":    {ID: 1, txt: "test", want: MyRecord{obj, 1, "test"}},
			"emptyTxt":   {ID: 2, txt: "", want: MyRecord{obj, 2, ""}},
			"zeroValues": {want: MyRecord{Object: obj}},
		} {
			rec := MyRecord{Object: obj}
			params := []interface{}{
				sql.Named("id", tCase.ID),
				sql.Named("txt", tCase.txt),
				sql.Named("rec", sql.Out{Dest: &rec}),
			}
			_, err = testDb.ExecContext(ctx, `begin test_pkg_sample.test_record(:id, :txt, :rec); end;`, params...)
			if err != nil {
				t.Fatal(err)
			}

			if rec != tCase.want {
				t.Errorf("%s: record got %v, wanted %v", tName, rec, tCase.want)
			}
		}
	})

	t.Run("Record IN OUT", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}
		defer objType.Close()

		for tName, tCase := range map[string]struct {
			in      MyRecord
			wantID  int64
			wantTxt string
		}{
			"zeroValues": {in: MyRecord{}, wantID: 1, wantTxt: " changed"},
			"default":    {in: MyRecord{ID: 1, Txt: "test"}, wantID: 2, wantTxt: "test changed"},
			"emptyTxt":   {in: MyRecord{ID: 2, Txt: ""}, wantID: 3, wantTxt: " changed"},
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
			_, err = testDb.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)
			if err != nil {
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
		defer objType.Close()

		items := []*MyRecord{&MyRecord{ID: 1, Txt: "test - 2"}, &MyRecord{ID: 2, Txt: "test - 4"}}

		for tName, tCase := range map[string]struct {
			in   int64
			want MyTable
		}{
			"one": {in: 1, want: MyTable{Items: items[:1]}},
			"two": {in: 2, want: MyTable{Items: items}},
		} {

			obj, err := objType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()

			tb := MyTable{ObjectCollection: &goracle.ObjectCollection{obj}}
			params := []interface{}{
				sql.Named("x", tCase.in),
				sql.Named("tb", sql.Out{Dest: &tb}),
			}
			_, err = testDb.ExecContext(ctx, `begin :tb := test_pkg_sample.test_table(:x); end;`, params...)
			if err != nil {
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

	t.Run("Table IN", func(t *testing.T) {
		// you must have execute privilege on package and use uppercase
		tableObjType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}
		defer tableObjType.Close()

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

			tb := MyTable{ObjectCollection: &goracle.ObjectCollection{obj}, Items: tCase.want.Items}
			params := []interface{}{
				sql.Named("tb", sql.Out{Dest: &tb, In: true}),
			}
			_, err = testDb.ExecContext(ctx, `begin test_pkg_sample.test_table_in(:tb); end;`, params...)
			if err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
			t.Error(errors.Wrap(err, qry))
		}
	}
	defer cleanup()

	const qry = "select " + pkgName + ".FUNC_1('aa','bb') from dual"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(errors.Wrap(err, qry))
	}
	defer rows.Close()
	for rows.Next() {
		var objI interface{}
		if err = rows.Scan(&objI); err != nil {
			t.Fatal(errors.Wrap(err, qry))
		}
		obj := goracle.ObjectCollection{Object: objI.(*goracle.Object)}
		defer obj.Close()
		t.Log(obj.FullName())
		i, err := obj.First()
		if err != nil {
			t.Fatal(err)
		}
		var objData, attrData goracle.Data
		for {
			if err = obj.Get(&objData, i); err != nil {
				t.Fatal(err)
			}
			if err := objData.GetObject().GetAttribute(&attrData, "MSG"); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const pkgName = "test_bool"
	cleanup := func() { testDb.Exec("DROP PROCEDURE " + pkgName) }
	cleanup()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = goracle.EnableDbmsOutput(ctx, conn); err != nil {
		t.Error(err)
	}
	const crQry = "CREATE OR REPLACE PROCEDURE " + pkgName + `(p_in IN BOOLEAN, p_not OUT BOOLEAN, p_num OUT NUMBER) IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('in='||(CASE WHEN p_in THEN 'Y' ELSE 'N' END));
  p_not := NOT p_in;
  p_num := CASE WHEN p_in THEN 1 ELSE 0 END;
END;`
	if _, err := conn.ExecContext(ctx, crQry); err != nil {
		t.Fatal(errors.Wrap(err, crQry))
	}
	defer cleanup()

	const qry = "BEGIN " + pkgName + "(p_in=>:1, p_not=>:2, p_num=>:3); END;"
	var buf bytes.Buffer
	for _, in := range []bool{true, false} {
		var out bool
		var num int
		if _, err := conn.ExecContext(ctx, qry, in, sql.Out{Dest: &out}, sql.Out{Dest: &num}); err != nil {
			t.Errorf("%q: %v", qry, err)
			continue
		}
		t.Logf("in:%v not:%v num:%v", in, out, num)
		want := 0
		if in {
			want = 1
		}
		if num != want || out != !in {
			buf.Reset()
			if err = goracle.ReadDbmsOutput(ctx, &buf, conn); err != nil {
				t.Error(err)
			}
			t.Log(buf.String())
			t.Errorf("got %v/%v wanted %v/%v", out, num, want, !in)
		}
	}
}
