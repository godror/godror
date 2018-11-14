// Copyright 2017 Tamás Gulácsi
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
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	goracle "gopkg.in/goracle.v2"
)

func TestHeterogeneousPoolIntegration(t *testing.T) {

	username := os.Getenv("GORACLE_DRV_TEST_USERNAME")
	proxyUser := "proxyUser"

	testHeterogeneousConStr := fmt.Sprintf("oracle://%s:%s@%s/?poolMinSessions=1&poolMaxSessions=%d&poolIncrement=1&connectionClass=POOLED&noConnectionPooling=0&heterogeneousPool=1",
		username,
		os.Getenv("GORACLE_DRV_TEST_PASSWORD"),
		os.Getenv("GORACLE_DRV_TEST_DB"),
		maxSessions,
	)

	var err error
	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("goracle", testHeterogeneousConStr); err != nil {
		fmt.Printf("ERROR: %+v\n", err)
		return
		//panic(err)
	}
	defer testHeterogeneousDB.Close()

	testHeterogeneousDB.Exec(fmt.Sprintf("DROP USER %s", proxyUser))

	if _, err = testHeterogeneousDB.Exec(fmt.Sprintf("CREATE USER %s IDENTIFIED BY myPassword", proxyUser)); err != nil {
		t.Fatal(err)
	}
	if _, err = testHeterogeneousDB.Exec(fmt.Sprintf("GRANT CONNECT TO %s", proxyUser)); err != nil {
		t.Fatal(err)
	}
	if _, err = testHeterogeneousDB.Exec(fmt.Sprintf("GRANT CREATE SESSION TO %s", proxyUser)); err != nil {
		t.Fatal(err)
	}
	if _, err = testHeterogeneousDB.Exec(fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", proxyUser, username)); err != nil {
		t.Fatal(err)
	}

	for tName, tCase := range map[string]struct {
		In   context.Context
		Want string
	}{
		"noContext": {In: context.TODO(), Want: username},
		"proxyUser": {In: context.WithValue(context.TODO(), goracle.CurrentUser, proxyUser), Want: proxyUser},
	} {

		var result string
		if err = testHeterogeneousDB.QueryRowContext(tCase.In, "SELECT user FROM dual").Scan(&result); err != nil {
			t.Fatal(err)
		}

		if strings.ToUpper(tCase.Want) != strings.ToUpper(result) {
			t.Errorf("%s: currentUser got %s, wanted %s", tName, result, tCase.Want)
		}

	}

}
