// Copyright 2019, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/UNO-SOFT/zlog/v2"
	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
	"github.com/google/go-cmp/cmp"
)

func TestLoadXMLLOB(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("LoadXMLLOB"), 60*time.Second)
	defer cancel()

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx...%+v", err)
	}

	defer tx.Rollback()
	var clobResult godror.Lob = godror.Lob{IsClob: true}

	{
		const qry = `DECLARE 
  clobResult CLOB;
BEGIN
  DBMS_LOB.createtemporary(clobResult, TRUE, DBMS_LOB.SESSION);
  :2 := clobResult;
END;`

		stmt, err := tx.PrepareContext(ctx, qry)
		if err != nil {
			t.Fatalf("PrepareContex...%+v", err)
		}
		defer stmt.Close()

		if _, err = stmt.ExecContext(ctx, godror.LobAsReader(), sql.Out{Dest: &clobResult}); err != nil {
			t.Fatalf("ExecContext lob...%+v", err)
		}
	}

	{
		const qry = `DECLARE
  v_out CLOB := :1;
  v_inp CLOB := :2;
BEGIN
  DBMS_LOB.copy(v_out, v_inp, DBMS_LOB.getlength(v_inp));
END;`
		_, err = tx.ExecContext(ctx, qry,
			sql.Out{Dest: &clobResult, In: true},
			`<?xml version="1.0" charset="utf-8"?>
<!-- a very long xml -->`)
		if err != nil {
			t.Fatalf("ExecContext(%q): %+v", qry, err)
		}
	}

	directLob, err := clobResult.Hijack()
	if err != nil {
		t.Fatalf("Hijack...%+v", err)
	}
	defer directLob.Close()

	var result strings.Builder
	var offset int64
	bufSize := int64(32768)
	buf := make([]byte, bufSize)
	for i := 0; i < 100; i++ {
		count, err := directLob.ReadAt(buf, offset)
		if err != nil {
			t.Fatalf("ReadAt(%d): %+v", offset, err)
		}
		if int64(count) > bufSize/int64(4) {
			count = int(bufSize / 4)
		}
		offset += int64(count)
		result.Write(buf[:count])
		if count == 0 {
			break
		}
	}

	t.Logf("result: %q", result.String())
}

func TestInsertLargeLOB(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("InsertLargeLOB"), 60*time.Second)
	defer cancel()

	tbl := "test_insert_large_lob" + tblSuffix
	drQry := "DROP TABLE " + tbl
	_, _ = testDb.ExecContext(ctx, drQry)
	crQry := "CREATE TABLE " + tbl + " (F_size NUMBER(9) NOT NULL, F_data CLOB NOT NULL)"
	if _, err := testDb.ExecContext(ctx, crQry); err != nil {
		t.Fatal(crQry, err)
	}
	defer func() { _, _ = testDb.ExecContext(context.Background(), drQry) }()

	const str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-"
	insQry := "INSERT INTO " + tbl + " (F_size, F_data) VALUES (:1, :2)"
	selQry := "SELECT DBMS_LOB.getlength(F_data) FROM " + tbl + " WHERE F_size = :1"
	selStmt, err := testDb.PrepareContext(ctx, selQry)
	if err != nil {
		t.Fatalf("%s: %+v", selQry, err)
	}
	defer selStmt.Close()

	shortCtx, shortCancel := context.WithTimeout(ctx, 30*time.Second)
	defer shortCancel()
	dl, _ := shortCtx.Deadline()
	var n int64
	now := time.Now()
	for _, size := range []int{
		2049,
		1<<20 - 1, 1<<20 + 1,
		2017371,
		16<<20 + 1,
		//1_073_741_822 + 1,
	} {
		if testing.Short() && size > 1<<21 {
			break
		}
		if n > 1<<20 {
			if est := (time.Since(now) / time.Duration(n)) * time.Duration(size); est > time.Until(dl) {
				t.Log("SKIP", size, "est", est)
				break
			}
		}
		data := (strings.Repeat(str, (size+len(str)-1)/len(str)) + str[:size%(len(str))])[:size]
		t.Log(size, len(data))
		if _, err := testDb.ExecContext(shortCtx, insQry, size, godror.Lob{Reader: strings.NewReader(data), IsClob: true}); err != nil {
			var ec interface{ Code() int }
			if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ec) && ec.Code() == 1013 {
				t.Log(err)
				break
			}
			t.Fatalf("%s [%d, %d]: %+v", insQry, size, len(data), err)
		}
		n += int64(len(data))

		var got int
		if err := selStmt.QueryRowContext(ctx, size).Scan(&got); err != nil {
			t.Fatalf("%s [%d]: %+v", selQry, size, err)
		}
		if got != size {
			t.Errorf("got %d wanted %d", got, size)
			break
		}
	}
}

func TestCloseTempLOB(t *testing.T) {
	//godror.SetLogger(godror.NewLogfmtLogger(os.Stdout))
	P, err := dsn.Parse(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if P.StandaloneConnection.Valid && P.StandaloneConnection.Bool {
		t.Skip("TestCloseTempLOB needs pooled connection")
	}

	ctx, cancel := context.WithTimeout(testContext("CloseTempLOB"), 30*time.Second)
	defer cancel()

	var wg, start sync.WaitGroup
	start.Add(1)
	maxConn := 2*maxSessions + 1
	wg.Add(maxConn)
	for j := 0; j < maxConn; j++ {
		t.Logf("Run %d\n", j)
		go func(n int) {
			start.Wait()
			err := newTempLob(ctx, testDb)
			wg.Done()
			if err != nil {
				log.Println(err.Error())
			}
			t.Logf("Finish %d\n", n)
		}(j)
	}
	start.Done()
	wg.Wait()
}

func newTempLob(ctx context.Context, db *sql.DB) error {
	data := []byte{0, 1, 2, 3, 4, 5}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return godror.Raw(ctx, conn,
		func(c godror.Conn) error {
			lob, err := c.(interface {
				NewTempLob(bool) (*godror.DirectLob, error)
			}).NewTempLob(false)
			if err != nil {
				return err
			}
			_, err = lob.WriteAt(data, 0)
			if closeErr := lob.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			return err
		},
	)
}

func TestSplitLOB(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("SplitLOB"), 30*time.Second)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	want := "árvíztűrő tükörfúrógép"
	n := 32767 / len(want)
	want = strings.Repeat(want, n)
	runesLen := utf8.RuneCount([]byte(want))
	t.Logf("%d runes, %d bytes", runesLen, len(want))
	const qry = `DECLARE 
  v_text CONSTANT VARCHAR2(32767) := SUBSTR(:1, 1, 32767); 
  v_len CONSTANT PLS_INTEGER := :2;
  v_clob CLOB; 
BEGIN
  DBMS_LOB.createtemporary(v_clob, TRUE, DBMS_LOB.session);
  DBMS_LOB.writeappend(v_clob, v_len, v_text);
  :3 := v_clob;
END;`
	var lob godror.Lob
	lob.IsClob = true
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer stmt.Close()
	if _, err := stmt.ExecContext(ctx, want, runesLen, sql.Out{Dest: &lob}); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	t.Log("lob:", lob)
	b, err := io.ReadAll(lob)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	t.Logf("sent %d bytes, got %d", len(want), len(got))
	if d := cmp.Diff(want, got); d != "" {
		//t.Fatal("mismatch")
		t.Error("mismatch:", d)
	}
}
func TestReadLargeLOB(t *testing.T) {
	qry := `CREATE OR REPLACE FUNCTION test_readlargelob(p_size IN PLS_INTEGER) RETURN CLOB IS
  v_clob CLOB;
  i PLS_INTEGER;
BEGIN
  DBMS_LOB.createtemporary(v_clob, TRUE, DBMS_LOB.session);
  FOR i IN 1..NVL(p_size, 0)/10 + 1 LOOP
    DBMS_LOB.writeappend(v_clob, 10, LPAD(i, 9, ' ')||CHR(10));
  END LOOP;
  RETURN(v_clob);
END;`
	if _, err := testDb.ExecContext(context.Background(), qry); err != nil {
		t.Fatalf("%s: %+v", qry, err)
	}
	defer func() { testDb.ExecContext(context.Background(), "DROP FUNCTION test_readlargelob") }()

	ctx, cancel := context.WithTimeout(testContext("ReadLargeLOB"), 30*time.Second)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	for _, orig := range []int64{12, 512 + 1, 8132/5 + 1, 8132/2 + 1, 8132 + 1, 32768} {
		qry := "BEGIN :1 := DBMS_LOB.getlength(test_readlargelob(:2)); END;"
		var wantSize int64
		if _, err = tx.ExecContext(ctx, qry, sql.Out{Dest: &wantSize}, orig); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		t.Logf("n=%d", wantSize)
		qry = "BEGIN :1 := test_readlargelob(:2); END;"
		stmt, err := tx.PrepareContext(ctx, qry)
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer stmt.Close()

		lob := godror.Lob{IsClob: true}
		if _, err = stmt.ExecContext(ctx, sql.Out{Dest: &lob}, orig); err != nil {
			t.Fatalf("%s [CLOB %d]: %+v", qry, orig, err)
		}
		buf := bytes.NewBuffer(make([]byte, 0, wantSize))
		for i := 0; i < int(wantSize/10); i++ {
			fmt.Fprintf(buf, "% 9d\n", i+1)
		}
		want := buf.Bytes()

		got, err := io.ReadAll(lob)
		gotSize := int64(len(got))
		t.Logf("Read %d bytes from CLOB: %+v", gotSize, err)
		if err != nil {
			t.Error(err)
		}
		if gotSize != wantSize {
			t.Errorf("got %d, wanted %d", gotSize, wantSize)
		}

		if d := cmp.Diff(got, want); d != "" {
			t.Error(d)
		}

		var gotS string
		if _, err = stmt.ExecContext(ctx, sql.Out{Dest: &gotS}, orig); err != nil {
			if orig > 32767 {
				t.Logf("%s [string %d]: %+v", qry, orig, err)
				continue
			}
			t.Fatalf("%s [string %d]: %+v", qry, orig, err)
		}
		gotSize = int64(len(gotS))
		t.Logf("Read %d bytes from LOB as string: %+v", gotSize, err)
		if gotSize != wantSize {
			t.Errorf("got %d, wanted %d", gotSize, wantSize)
		}
		if d := cmp.Diff(got, want); d != "" {
			t.Error(d)
		}
	}
}

func TestLOBAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("LOBAppend"), 30*time.Second)
	defer cancel()

	// To have a valid LOB locator, we have to keep the Stmt around.
	const qry = `BEGIN DBMS_LOB.createtemporary(:1, TRUE, DBMS_LOB.SESSION); END;`
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatalf("%q: %+v", qry, err)
	}
	defer stmt.Close()
	var tmp godror.Lob
	if _, err := stmt.ExecContext(ctx, sql.Out{Dest: &tmp}); err != nil {
		t.Fatalf("Failed to create temporary lob: %+v", err)
	}
	t.Logf("tmp: %#v", tmp)

	want := [...]byte{1, 2, 3, 4, 5}
	if _, err := conn.ExecContext(ctx,
		"BEGIN dbms_lob.append(:1, :2); END;",
		tmp, godror.Lob{Reader: bytes.NewReader(want[:])},
	); err != nil {
		var ec interface{ Code() int }
		if errors.As(err, &ec) && ec.Code() == 3106 || errors.Is(err, driver.ErrBadConn) {
			t.Skip(err)
		}
		t.Fatalf("Failed to write buffer(%v) to lob(%v): %+v", want, tmp, err)
	}

	// Hijack and Close it.
	dl, err := tmp.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	length, err := dl.Size()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("length: %d", length)
	if length != int64(len(want)) {
		t.Errorf("length mismatch: got %d, wanted %d", length, len(want))
	}
}

func TestStatWithLOBs(t *testing.T) {
	//defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(testContext("StatWithLOBs"), 30*time.Second)
	defer cancel()

	ms, err := newMetricSet(ctx, testDb)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	if _, err = ms.Fetch(ctx); err != nil {
		var c interface{ Code() int }
		if errors.As(err, &c) && c.Code() == 942 {
			t.Skip(err)
			return
		}
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		events, err := ms.Fetch(ctx)
		t.Log("events:", len(events))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func newMetricSet(ctx context.Context, db *sql.DB) (*metricSet, error) {
	qry := "select /* metricset: sqlstats */ inst_id, sql_fulltext, last_active_time from gv$sqlstats WHERE ROWNUM < 11"
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	stmt, err := tx.PrepareContext(ctx, qry)
	if err != nil {
		return nil, err
	}

	return &metricSet{tx: tx, stmt: stmt}, nil
}

type metricSet struct {
	tx   *sql.Tx
	stmt *sql.Stmt
}

func (m *metricSet) Close() error {
	tx, st := m.tx, m.stmt
	m.tx, m.stmt = nil, nil
	if st == nil {
		return nil
	}
	st.Close()
	if tx == nil {
		return nil
	}
	return tx.Rollback()
}

// Fetch methods implements the data gathering and data conversion to the right format
// It returns the event which is then forward to the output. In case of an error, a
// descriptive error must be returned.
func (m *metricSet) Fetch(ctx context.Context) ([]event, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := m.stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []event
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.ID, &e.Text, &e.LastActive); err != nil {
			return events, err
		}
		events = append(events, e)
	}

	return events, nil
}

type event struct {
	LastActive time.Time
	Text       string
	ID         int64
}

func TestCLOBSurrogate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const nlsQry = `SELECT parameter, value,
		CASE parameter WHEN 'NLS_CHARACTERSET' THEN UTL_i18n.MAP_CHARSET(value,0,0) ELSE NULL END AS charset
	FROM v$nls_parameters
	WHERE parameter LIKE '%CHAR%'
	ORDER BY 1`
	rows, err := testDb.QueryContext(ctx, nlsQry)
	if err != nil {
		t.Fatalf("%s: %+v", nlsQry, err)
	}
	var dbCS string
	for rows.Next() {
		var k, v, c string
		if err = rows.Scan(&k, &v, &c); err != nil {
			t.Errorf("scan %s: %+v", nlsQry, err)
		}
		t.Logf("%s: %s", k, v)
		if dbCS == "" {
			dbCS = c
		}
	}
	rows.Close()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, "DROP TABLE test_nclob PURGE")
	defer testDb.ExecContext(ctx, "DROP TABLE test_nclob PURGE")

	const tblQry = "CREATE TABLE test_nclob(id NUMBER, col1 NCLOB, col2 CLOB)"
	if _, err := conn.ExecContext(ctx, tblQry); err != nil {
		t.Fatalf("%s: %+v", tblQry, err)
	}

	one := "aáñ⅛🎵"
	const encQry = `SELECT RAWTOHEX(UTL_RAW.cast_to_raw(s)), RAWTOHEX(UTL_i18n.string_to_raw(s, 'AL32UTF8'))
		FROM (SELECT RTRIM(UTL_i18n.raw_to_char(HEXTORAW(:1), 'AL32UTF8'), '?') AS s FROM DUAL)`
	var x, y string
	if err = conn.QueryRowContext(ctx, encQry, fmt.Sprintf("%X", one)).Scan(&x, &y); err != nil {
		t.Fatalf("%s [%q]: %+v", encQry, fmt.Sprintf("%X", one), err)
	}
	twoB, err := hex.DecodeString(x)
	if err != nil {
		t.Fatalf("decode hex %q: %+v", x, err)
	}
	twoX, err := hex.DecodeString(y)
	if err != nil {
		t.Fatalf("decode hex %q: %+v", y, err)
	}
	two := string(twoX)
	t.Logf("one=%q (%x) two=%q (%x)", one, one, two, two)
	const count = 16384
	insQry := `DECLARE
	  v_nstr NVARCHAR2(20) := UTL_i18n.raw_to_nchar(HEXTORAW('` + fmt.Sprintf("%X", one) + `'), 'AL32UTF8');
	  v_str  VARCHAR2(20)  := UTL_i18n.raw_to_char( HEXTORAW('` + fmt.Sprintf("%X", twoB) + `'), NULL);
	  v_nlen CONSTANT SIMPLE_INTEGER := LENGTH2(v_nstr);
	  v_len  CONSTANT SIMPLE_INTEGER := LENGTH2(v_str);
	  v_col1 NCLOB; v_col2 CLOB;
	BEGIN
	  INSERT INTO test_nclob (id, col1, col2) VALUES (1, EMPTY_CLOB, EMPTY_CLOB) RETURNING col1, col2 INTO v_col1, v_col2;
	  FOR i IN 1..` + fmt.Sprintf("%d", count) + ` LOOP
	    --a + that musical note is 2 UTF16 "characters"
	    DBMS_LOB.writeappend(v_col1, v_nlen, v_nstr);
	    DBMS_LOB.writeappend(v_col2, v_len,  v_str);
	  END LOOP;
	END;`
	if _, err = conn.ExecContext(ctx, insQry); err != nil {
		t.Fatalf("%s: %+v", insQry, err)
	}

	const qry = "SELECT id, col1, col2 FROM test_nclob"

	t.Run("LobAsReader", func(t *testing.T) {
		rows, err = testDb.QueryContext(ctx, qry, godror.LobAsReader())
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer rows.Close()
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		for _, colType := range colTypes {
			t.Logf("%#v\n", colType)
		}
		if testing.Verbose() {
			godror.SetLogger(zlog.NewT(t).SLog())
			defer godror.SetLogger(slog.Default())
		}
		for rows.Next() {
			var id godror.Number
			var clobI, nclobI any
			if err := rows.Scan(&id, &nclobI, &clobI); err != nil {
				t.Fatalf("scan %s: %+v", qry, err)
			}
			nclob := nclobI.(*godror.Lob)
			b, err := io.ReadAll(nclob)
			if err != nil {
				t.Fatal("read nclob:", err)
			}
			if !(bytes.HasPrefix(b, []byte(one)) && bytes.HasSuffix(b, []byte(one))) {
				t.Errorf("nclob=%x ... %x (want: %x)", b[:len(one)], b[len(b)-len(one):], one)
			}

			clob := clobI.(*godror.Lob)
			b, err = io.ReadAll(clob)
			if err != nil {
				t.Fatal("read clob:", err)
			}
			if !(bytes.HasPrefix(b, []byte(two)) && bytes.HasSuffix(b, []byte(two))) {
				t.Errorf("clob=%x ... %x (want: %x)", b[:len(two)], b[len(b)-len(two)], two)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("string", func(t *testing.T) {
		rows, err = testDb.QueryContext(ctx, qry)
		if err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
		defer rows.Close()
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		for _, colType := range colTypes {
			t.Logf("%#v\n", colType)
		}
		if testing.Verbose() {
			godror.SetLogger(zlog.NewT(t).SLog())
			defer godror.SetLogger(slog.Default())
		}
		for rows.Next() {
			var id godror.Number
			var nclob, clob string
			if err := rows.Scan(&id, &nclob, &clob); err != nil {
				t.Fatalf("scan %s: %+v", qry, err)
			}
			if !(strings.HasPrefix(nclob, one) && strings.HasSuffix(nclob, one)) {
				t.Errorf("nclob=%x ... %x (want: %x)", nclob[:len(one)], nclob[len(nclob)-len(one):], one)
			}

			if !(strings.HasPrefix(clob, string(two)) && strings.HasSuffix(clob, string(two))) {
				t.Errorf("clob=%x ... %x (want: %x)", clob[:len(two)], clob[len(clob)-len(two):], two)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})

}
