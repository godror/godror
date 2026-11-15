// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// failingReader returns its data, then err.
type failingReader struct {
	r   io.Reader
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err == io.EOF {
		err = r.err
	}
	return n, err
}

// A Lob bind whose Reader fails must fail the Exec, and not store the data
// read till the error.
func TestLobBindReadError(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_lob_read_err" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER, b BLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	errRead := errors.New("read failed")
	// 1000 bytes are set as bytes, 2MiB go through a temporary LOB.
	for _, size := range []int{0, 1000, 2 << 20} {
		rdr := &failingReader{r: bytes.NewReader(make([]byte, size)), err: errRead}
		_, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, b) VALUES (:1, :2)", size, godror.Lob{Reader: rdr})
		if !errors.Is(err, errRead) {
			t.Errorf("size=%d: got %+v, wanted %v", size, err, errRead)
		}
	}
	var n int
	if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d rows were inserted from failing readers", n)
	}

	// A reader that just ends is fine.
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, b) VALUES (:1, :2)", 1, godror.Lob{Reader: bytes.NewReader(make([]byte, 1000))}); err != nil {
		t.Fatal(err)
	}
	if err := testDb.QueryRowContext(ctx, "SELECT DBMS_LOB.GETLENGTH(b) FROM "+tbl+" WHERE id = 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1000 {
		t.Errorf("stored %d bytes, wanted 1000", n)
	}
}
