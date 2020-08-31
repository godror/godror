// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	godror "github.com/godror/godror"
)

func TestQRCN(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext("QRCN"))
	defer cancel()

	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	cx.ExecContext(ctx, "DROP TABLE test_subscr")
	if _, err = cx.ExecContext(ctx, "CREATE TABLE test_subscr (i NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE test_subscr")

	var events []godror.Event
	cb := func(e godror.Event) {
		t.Log(e)
		events = append(events, e)
	}
	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		t.Fatal(err)
	}
	s, err := conn.NewSubscription("subscr", cb)
	if err != nil {
		var ec interface{ Code() int }
		if errors.As(err, &ec) {
			switch ec.Code() {
			case 29970, 65131:
				t.Skip(err.Error())
			case 1031, 29972:
				t.Log("See \"https://docs.oracle.com/database/121/ADFNS/adfns_cqn.htm#ADFNS553\"")
				var User string
				_ = testDb.QueryRow("SELECT USER FROM DUAL").Scan(&User)
				//t.Log("GRANT EXECUTE ON DBMS_CQ_NOTIFICATION TO "+User)
				t.Log("GRANT CHANGE NOTIFICATION TO " + User + ";")
				t.Skip(err.Error())
			}
		}
		t.Fatalf("create %+v", err)
	}
	defer s.Close()
	if err = s.Register("SELECT COUNT(0) FROM test_subscr"); err != nil {
		t.Fatalf("%+v", err)
	}
	qry := "SELECT regid, table_name FROM USER_CHANGE_NOTIFICATION_REGS"
	rows, err := cx.QueryContext(ctx, qry)
	if err != nil {
		t.Fatal(fmt.Errorf("%s: %w", qry, err))
	}
	t.Log("--- Registrations ---")
	for rows.Next() {
		var regID, table string
		if err := rows.Scan(&regID, &table); err != nil {
			t.Error(err)
			break
		}
		t.Logf("%s: %s", regID, table)
	}
	t.Log("---------------------")
	rows.Close()
	testDb.Exec("INSERT INTO test_subscr (i) VALUES (1)")
	testDb.Exec("INSERT INTO test_subscr (i) VALUES (0)")
	t.Log("events:", events)
}
