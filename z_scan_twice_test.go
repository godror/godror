// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestScanTwice checks that a row can be scanned again
// (after a Scan that failed on a later column, for example),
// even when it has a LOB or a cursor column.
func TestScanTwice(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()

	const qry = `SELECT TO_NCLOB('abc'), TO_BLOB(HEXTORAW('010203')),
			CURSOR(SELECT level FROM DUAL CONNECT BY level <= 3),
			CAST(NULL AS NUMBER(3))
		FROM DUAL`
	for _, lobAsReader := range []bool{false, true} {
		func() {
			var opts []any
			if lobAsReader {
				opts = append(opts, godror.LobAsReader())
			}
			// A prepared statement, for the Lobs to remain usable.
			stmt, err := testDb.PrepareContext(ctx, qry)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx, opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			var (
				clob1, blob1, cur1, clob2, blob2, cur2 any
				i                                      int
				ni                                     sql.NullInt64
			)
			if err = rows.Scan(&clob1, &blob1, &cur1, &i); err == nil {
				t.Fatal("wanted an error for NULL into int")
			}
			if err = rows.Scan(&clob2, &blob2, &cur2, &ni); err != nil {
				t.Fatalf("LobAsReader=%t: second Scan: %+v", lobAsReader, err)
			}
			if lobAsReader {
				// The same Lob, as with Next.
				if clob1 != clob2 || blob1 != blob2 {
					t.Errorf("first Scan got %p, %p, the second %p, %p", clob1, blob1, clob2, blob2)
				}
				b, err := io.ReadAll(clob2.(*godror.Lob))
				if err != nil {
					t.Errorf("read CLOB: %+v", err)
				}
				clob2 = string(b)
				if blob2, err = io.ReadAll(blob2.(*godror.Lob)); err != nil {
					t.Errorf("read BLOB: %+v", err)
				}
			}
			if clob2 != "abc" || string(blob2.([]byte)) != "\x01\x02\x03" {
				t.Errorf("LobAsReader=%t: got %q, %q", lobAsReader, clob2, blob2)
			}

			// The cursor that the first Scan returned has the rows.
			cur, ok := cur1.(driver.Rows)
			if !ok {
				t.Fatalf("got %T as cursor", cur1)
			}
			defer cur.Close()
			var got string
			dest := make([]driver.Value, 1)
			for {
				if err = cur.Next(dest); err != nil {
					break
				}
				got += fmt.Sprintf("%v;", dest[0])
			}
			if err != io.EOF {
				t.Errorf("cursor: %+v", err)
			}
			if got != "1;2;3;" {
				t.Errorf("LobAsReader=%t: the cursor of the first Scan gave %q, wanted 1;2;3;", lobAsReader, got)
			}
		}()
	}
}
