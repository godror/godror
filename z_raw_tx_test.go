package godror_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestRawInTx(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxOpenConns(1)

	tbl := "test_rawtx" + tblSuffix
	db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err = db.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(9))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	for i, tc := range []struct {
		name string
		f    func(godror.Execer) error
	}{
		{"DescribeQuery", func(ex godror.Execer) error {
			_, err := godror.DescribeQuery(ctx, ex, "SELECT 1 FROM DUAL")
			return err
		}},
		{"ServerVersion", func(ex godror.Execer) error { _, err := godror.ServerVersion(ctx, ex); return err }},
		{"ClientVersion", func(ex godror.Execer) error { _, err := godror.ClientVersion(ctx, ex); return err }},
		{"Timezone", func(ex godror.Execer) error { _, err := godror.Timezone(ctx, ex); return err }},
		{"Raw", func(ex godror.Execer) error {
			return godror.Raw(ctx, ex, func(c godror.Conn) error { return c.Ping(ctx) })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (:1)", i); err != nil {
				t.Fatal(err)
			}
			if err = tc.f(tx); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatalf("Commit: %+v", err)
			}
			var n int
			if err = testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = :1", i).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("got %d rows after Commit, wanted 1", n)
			}
		})
	}
}

// txOnly has BeginTx but neither Conn nor Raw.
type txOnly struct{ db *sql.DB }

func (x txOnly) ExecContext(ctx context.Context, qry string, args ...any) (sql.Result, error) {
	return x.db.ExecContext(ctx, qry, args...)
}
func (x txOnly) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return x.db.BeginTx(ctx, opts)
}

func TestRawTxerCommitRollback(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_rawtxer" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(9))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	insert := func(c godror.Conn, id int64) error {
		st, err := c.PrepareContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (:1)")
		if err != nil {
			return err
		}
		defer st.Close()
		_, err = st.(driver.StmtExecContext).ExecContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: id}})
		return err
	}
	count := func(id int) int {
		var n int
		if err := testDb.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = :1", id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	ex := txOnly{db: testDb}
	if err := godror.Raw(ctx, ex, func(c godror.Conn) error { return insert(c, 1) }); err != nil {
		t.Fatalf("Raw: %+v", err)
	}
	if n := count(1); n != 1 {
		t.Errorf("successful f: got %d rows, wanted 1 (committed)", n)
	}

	errFail := errors.New("f failed")
	err := godror.Raw(ctx, ex, func(c godror.Conn) error {
		if err := insert(c, 2); err != nil {
			return err
		}
		return errFail
	})
	if !errors.Is(err, errFail) {
		t.Errorf("failing f: got error %+v, wanted %v", err, errFail)
	}
	if n := count(2); n != 0 {
		t.Errorf("failing f: got %d rows, wanted 0 (rolled back)", n)
	}
}
