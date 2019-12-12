// Copyright 2019 Walter Wanderley
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package goracle_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	errors "golang.org/x/xerrors"

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

type coder interface{ Code() int }

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
			err = collection.GetItem(&data, i)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := goracle.DriverConn(ctx, testDb)
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
	TYPE my_other_record IS RECORD (
		id    NUMBER(5),
		txt   VARCHAR2(200)
	);
	TYPE my_record IS RECORD (
		id    NUMBER(5),
		other test_pkg_types.my_other_record,
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	serverVersion, err := goracle.ServerVersion(ctx, testDb)
	if err != nil {
		t.Fatal(err)
	}
	clientVersion, err := goracle.ClientVersion(ctx, testDb)
	if err != nil {
		t.Fatal(err)
	}

	if serverVersion.Version < 12 || clientVersion.Version < 12 {
		t.Skip("client or server < 12")
	}

	err = createPackages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dropPackages(ctx)

	conn, err := goracle.DriverConn(ctx, testDb)
	if err != nil {
		t.Fatal(err)
	}

	//t.Run("Record", func(t *testing.T) {
	// you must have execute privilege on package and use uppercase
	{
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
				var cdr coder
				if errors.As(err, &cdr); cdr.Code() == 21779 {
					t.Skip(err)
				}
				t.Fatal(err)
			}

			if rec != tCase.want {
				t.Errorf("%s: record got %v, wanted %v", tName, rec, tCase.want)
			}
		}
	}
	//})

	//t.Run("Record IN OUT", func(t *testing.T) {
	// you must have execute privilege on package and use uppercase
	{
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_RECORD")
		if err != nil {
			t.Fatal(err)
		}

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
				var cdr coder
				if errors.As(err, &cdr); cdr.Code() == 21779 {
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
	}
	//})

	//t.Run("Table", func(t *testing.T) {
	{
		// you must have execute privilege on package and use uppercase
		objType, err := conn.GetObjectType("TEST_PKG_TYPES.MY_TABLE")
		if err != nil {
			t.Fatal(err)
		}

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
				var cdr coder
				if errors.As(err, &cdr); cdr.Code() == 30757 {
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
	}
	//})

	//t.Run("Table IN", func(t *testing.T) {
	{
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

			tb := MyTable{ObjectCollection: &goracle.ObjectCollection{obj}, Items: tCase.want.Items}
			params := []interface{}{
				sql.Named("tb", sql.Out{Dest: &tb, In: true}),
			}
			_, err = testDb.ExecContext(ctx, `begin test_pkg_sample.test_table_in(:tb); end;`, params...)
			if err != nil {
				var cdr coder
				if errors.As(err, &cdr); cdr.Code() == 30757 {
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
	}
	//})

}

func TestSelectObjectTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
			t.Error(errors.Errorf("%s: %w", qry, err))
		}
	}
	defer cleanup()

	const qry = "select " + pkgName + ".FUNC_1('aa','bb') from dual"
	rows, err := testDb.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(errors.Errorf("%s: %w", qry, err))
	}
	defer rows.Close()
	for rows.Next() {
		var objI interface{}
		if err = rows.Scan(&objI); err != nil {
			t.Fatal(errors.Errorf("%s: %w", qry, err))
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
			if err = obj.GetItem(&objData, i); err != nil {
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
		t.Fatal(errors.Errorf("%s: %w", crQry, err))
	}
	defer cleanup()

	const qry = "BEGIN " + pkgName + "(p_in=>:1, p_not=>:2, p_num=>:3); END;"
	var buf bytes.Buffer
	for _, in := range []bool{true, false} {
		var out bool
		var num int
		if _, err := conn.ExecContext(ctx, qry, in, sql.Out{Dest: &out}, sql.Out{Dest: &num}); err != nil {
			if srv, err := goracle.ServerVersion(ctx, conn); err != nil {
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
			if err = goracle.ReadDbmsOutput(ctx, &buf, conn); err != nil {
				t.Error(err)
			}
			t.Log(buf.String())
			t.Errorf("got %v/%v wanted %v/%v", out, num, want, !in)
		}
	}
}

func TestPlSqlObjectDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	testCon, err := goracle.DriverConn(ctx, testDb)
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
	clientVersion, _ := goracle.ClientVersion(ctx, testDb)
	serverVersion, _ := goracle.ServerVersion(ctx, testDb)
	t.Logf("clientVersion: %#v, serverVersion: %#v", clientVersion, serverVersion)
	cOt, err := testCon.GetObjectType(strings.ToUpper("test_pkg_obj.tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
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
	var data goracle.Data
	for i, err := coll.First(); err == nil; i, err = coll.Next(i) {
		if err = coll.GetItem(&data, i); err != nil {
			t.Fatal(err)
		}
		elt.ResetAttributes()
		elt = data.GetObject()

		t.Logf("elt[%d]: %s", i, elt)
		for attr := range elt.Attributes {
			val, err := elt.Get(attr)
			if err != nil {
				if goracle.DpiVersionNumber <= 30201 {
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
		return errors.Errorf("%s: %w", qry, err)
	}
	_, err = stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	stmt.Close()
	if err != nil {
		return errors.Errorf("%s: %w", qry, err)
	}
	return nil
}

func TestPlSqlObject(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		t.Fatal(errors.Errorf("%s: %w", qry, err))
	}
	defer testDb.Exec("DROP PACKAGE " + pkg)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	defer tl.enableLogging(t)()
	ot, err := goracle.GetObjectType(ctx, tx, pkg+strings.ToUpper(".int_tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cleanup()
	for _, qry := range strings.Split(crea, "CREATE OR") {
		if qry == "" {
			continue
		}
		qry = "CREATE OR" + qry
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatal(errors.Errorf("%s: %w", qry, err))
		}
	}

	var p_operation_id string
	var a_languagecode_i string
	var a_username_i string
	var a_channelcode_i string
	var a_mcalist_i *goracle.Object
	var a_validfrom_i string
	var a_validto_i string
	var a_statuscode_list_i string
	var a_type_list_o driver.Rows

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	typ, err := goracle.GetObjectType(ctx, tx, "test_cwo_tbl_t")
	if err != nil {
		t.Fatal(err)
	}
	if a_mcalist_i, err = typ.NewObject(); err != nil {
		t.Fatal(err)
	}
	if typ, err = goracle.GetObjectType(ctx, tx, "test_cwo_rec_t"); err != nil {
		t.Fatal(err)
	}
	elt, err := typ.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	if err = elt.Set("NUMBERPART1", "np1"); err != nil {
		t.Fatal(err)
	}
	if err = a_mcalist_i.Collection().Append(elt); err != nil {
		t.Fatal(err)
	}

	const qry = `BEGIN test_cwo_getSum(:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9); END;`
	if _, err := tx.ExecContext(ctx, qry,
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

	rows, err := goracle.WrapRows(ctx, tx, a_type_list_o)
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
}

func BenchmarkObjArray(b *testing.B) {
	cleanup := func() { testDb.Exec("DROP FUNCTION test_objarr"); testDb.Exec("DROP TYPE test_vc2000_arr") }
	cleanup()
	qry := "CREATE OR REPLACE TYPE test_vc2000_arr AS TABLE OF VARCHAR2(2000)"
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(errors.Errorf("%s: %w", qry, err))
	}
	defer cleanup()
	qry = `CREATE OR REPLACE FUNCTION test_objarr(p_arr IN test_vc2000_arr) RETURN PLS_INTEGER IS BEGIN RETURN p_arr.COUNT; END;`
	if _, err := testDb.Exec(qry); err != nil {
		b.Fatal(errors.Errorf("%s: %w", qry, err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("object", func(b *testing.B) {
		b.StopTimer()
		const qry = `BEGIN :1 := test_objarr(:2); END;`
		stmt, err := testDb.PrepareContext(ctx, qry)
		if err != nil {
			b.Fatal(errors.Errorf("%s: %w", qry, err))
		}
		defer stmt.Close()
		typ, err := goracle.GetObjectType(ctx, testDb, "TEST_VC2000_ARR")
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
			b.Fatal(errors.Errorf("%s: %w", qry, err))
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
			if _, err := stmt.ExecContext(ctx, goracle.PlSQLArrays, array, sql.Out{Dest: &rc}); err != nil {
				b.Fatal(err)
			}
			if rc != len(array) {
				b.Error(rc)
			}
		}
	})
}
