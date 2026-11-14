package godror_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestTxReadOnly(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_txro" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(9))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	for _, level := range []sql.IsolationLevel{sql.LevelDefault, sql.LevelSerializable} {
		t.Run(level.String(), func(t *testing.T) {
			tx, err := testDb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: level})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			_, err = tx.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (1)")
			if err == nil {
				t.Fatal("INSERT succeeded in a read-only transaction")
			}
			if !strings.Contains(err.Error(), "ORA-01456") {
				t.Errorf("got %+v, wanted ORA-01456", err)
			}
		})
	}
}

// TestTxIsolationPerTx checks that the isolation level of one transaction
// does not stick to the session for the next one.
func TestTxIsolationPerTx(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_txiso" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(9))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var id int
	// seesOtherCommit reports whether a second read in a transaction with
	// the given level sees a row committed meanwhile by another session.
	seesOtherCommit := func(level sql.IsolationLevel) bool {
		t.Helper()
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: level})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		var before, after int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&before); err != nil {
			t.Fatal(err)
		}
		id++
		if _, err = testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (:1)", id); err != nil {
			t.Fatal(err)
		}
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return after != before
	}

	if !seesOtherCommit(sql.LevelDefault) {
		t.Fatal("default level does not see committed rows")
	}
	if seesOtherCommit(sql.LevelSerializable) {
		t.Error("serializable sees rows committed after its start")
	}
	if !seesOtherCommit(sql.LevelDefault) {
		t.Error("default level after a serializable transaction is still serializable")
	}
	if seesOtherCommit(sql.LevelSerializable) {
		t.Error("serializable sees rows committed after its start")
	}
	if !seesOtherCommit(sql.LevelReadCommitted) {
		t.Error("read committed does not see committed rows")
	}
}

// waitReadOnlyReadable waits until a read-only transaction can read tbl.
// Right after CREATE TABLE (up to about 2s), it gets
// ORA-01466: unable to read data - table definition has changed.
func waitReadOnlyReadable(ctx context.Context, t testing.TB, tbl string) {
	t.Helper()
	for {
		tx, err := testDb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		var n int
		err = tx.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl).Scan(&n)
		tx.Rollback()
		if err == nil {
			return
		}
		if !strings.Contains(err.Error(), "ORA-01466") {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
