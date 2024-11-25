// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"github.com/godror/godror/dsn"
)

func TestWrongPassword(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("WrongPassword"), 30*time.Second)
	defer cancel()
	P, err := godror.ParseConnString(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		for _, ok := range []bool{true, false} {
			P2 := P
			if !ok {
				P2.Password = dsn.NewPassword(P2.Password.Secret() + "X")
			}
			db := sql.OpenDB(godror.NewConnector(P2))
			err := db.PingContext(ctx)
			db.Close()
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if ok != (err == nil) {
				t.Errorf("%d. wanted %t got %+v", i, ok, err)
			}
		}
	}
}

// Following are covered
// - standalone=0
//   - heterogeneous pool create with username, password and Query
//     without username, password
//   - Query from pool with session-username (proxyUser) ,
//     session-password(proxyPassword)
//   - Query from pool with session-username (proxyUser) and without
//     session-password
//   - Query from pool with session-username (proxyUser) enclosed
//     under brackets for failure test
// - standalone=1
//   - create connection with username, password

func TestHeterogeneousPoolIntegration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("HeterogeneousPoolIntegration"), 30*time.Second)
	defer cancel()

	const proxyPassword = "myPassword666myPassword"
	const proxyUser = "test_proxyUser"

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if !cs.IsStandalone() {
		cs.Heterogeneous = godror.Bool(true)
	}
	username := cs.Username
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("godror", testHeterogeneousConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testHeterogeneousConStr, err))
	}
	defer testHeterogeneousDB.Close()
	testHeterogeneousDB.SetMaxIdleConns(0)

	// Check that it works
	conn, err := testHeterogeneousDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, fmt.Sprintf("DROP USER %s", proxyUser))

	for _, qry := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY "+proxyPassword, proxyUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", proxyUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", proxyUser, username),
	} {
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			if strings.Contains(err.Error(), "ORA-01031:") {
				t.Log("Please issue this:\nGRANT CREATE USER, DROP USER, ALTER USER TO " + username + ";\n" +
					"GRANT CREATE SESSION TO " + username + " WITH ADMIN OPTION;\n")
			}
			t.Skip(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer func() {
		testHeterogeneousDB.ExecContext(testContext("HeterogeneousPoolIntegration-drop"), "DROP USER "+proxyUser)
	}()

	testCases := map[string]struct {
		In   context.Context
		Want string
	}{
		"noContext":             {In: ctx, Want: username},
		"proxyUser":             {In: godror.ContextWithUserPassw(ctx, proxyUser, proxyPassword, ""), Want: proxyUser},
		"proxyUserNoPass":       {In: godror.ContextWithUserPassw(ctx, proxyUser, "", ""), Want: proxyUser},
		"proxyUserwithBrackets": {In: godror.ContextWithUserPassw(ctx, "["+proxyUser+"]", "", ""), Want: proxyUser},
	}
	if cs.IsStandalone() {
		delete(testCases, "proxyUser")
		delete(testCases, "proxyUserNoPass")
		delete(testCases, "proxyUserwithBrackets")
	}
	for tName, tCase := range testCases {
		t.Run(tName, func(t *testing.T) {
			var result string
			if err = testHeterogeneousDB.QueryRowContext(tCase.In, "SELECT user FROM dual").Scan(&result); err != nil {
				if tName == "proxyUserwithBrackets" {
					if !strings.Contains(err.Error(), "ORA-00987:") {
						t.Errorf("%s: unexpected Error %s", tName, err.Error())
					}
				} else {
					t.Fatalf("%s: %+v", tName, err)
				}
			}
			if tName != "proxyUserwithBrackets" {
				if !strings.EqualFold(tCase.Want, result) {
					t.Errorf("%s: currentUser got %s, wanted %s", tName, result, tCase.Want)
				}
			}
		})

	}

}

// passing Proxyuser at the time of Go pool creation
// user = proxyusername[sessionusername], password for proxyusername
func TestHeterogeneousConnCreationWithProxy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("HeterogeneousConnCreationWithProxy"), 30*time.Second)
	defer cancel()

	const sessionUserPassword = "myPassword666myPassword"
	const sessionUser = "test_hetero_create_sessionUser"

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	username := cs.Username
	if !cs.IsStandalone() {
		cs.Heterogeneous = godror.Bool(true)
	}
	cs.Username += "[" + sessionUser + "]"
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("godror", testHeterogeneousConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testHeterogeneousConStr, err))
	}
	defer testHeterogeneousDB.Close()
	testHeterogeneousDB.SetMaxIdleConns(0)

	testDb.ExecContext(ctx, fmt.Sprintf("DROP USER %s", sessionUser))

	for _, qry := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY "+sessionUserPassword, sessionUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", sessionUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", sessionUser, username),
	} {
		if _, err = testDb.ExecContext(ctx, qry); err != nil {
			if strings.Contains(err.Error(), "ORA-01031:") {
				t.Log("Please issue this:\nGRANT CREATE USER, DROP USER, ALTER USER TO " + username + ";\n" +
					"GRANT CREATE SESSION TO " + username + " WITH ADMIN OPTION;\n")
			}
			t.Skip(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer func() {
		testDb.ExecContext(testContext("HeterogeneousConnCreationWithProxy-drop"), "DROP USER "+sessionUser)
	}()
	var result string
	if err = testHeterogeneousDB.QueryRowContext(ctx, "SELECT user FROM dual").Scan(&result); err != nil {
		t.Fatalf("%+v", err)
	}
	if !strings.EqualFold(sessionUser, result) {
		t.Errorf("currentUser got %s, wanted %s", result, sessionUser)
	}
}

func TestContextWithUserPassw(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ContextWithUserPassw"), 30*time.Second)
	defer cancel()

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if !cs.IsStandalone() {
		cs.Heterogeneous = godror.Bool(true)
	}
	username, password := cs.Username, cs.Password
	cs.Username = ""
	cs.Password.Reset()
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testConStr, " -> ", testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("godror", testHeterogeneousConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testHeterogeneousConStr, err))
	}
	defer testHeterogeneousDB.Close()
	testHeterogeneousDB.SetMaxIdleConns(0)

	{
		ctx := godror.ContextWithUserPassw(ctx, username, password.Secret(), "")
		if err := testHeterogeneousDB.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		t.Log(ctx)
	}

	{
		ctx := godror.ContextWithUserPassw(ctx, username, password.String(), "")
		err := testHeterogeneousDB.PingContext(ctx)
		t.Logf("Ping with wrong password (%q): %+v", password.String(), err)
		if err == nil {
			t.Log(ctx)
			t.Fatal("success with wrong password")
		}
	}
}
