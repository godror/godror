// Copyright 2018 @wwanderley
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
	"strings"
	"testing"

	"github.com/pkg/errors"

	goracle "gopkg.in/goracle.v2"
)

func TestHeterogeneousPoolIntegration(t *testing.T) {

	const proxyPassword = "myPassword"
	const proxyUser = "proxyUser"

	cs, err := goracle.ParseConnString(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.HeterogeneousPool = true
	username := cs.Username
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("goracle", testHeterogeneousConStr); err != nil {
		t.Fatal(errors.Wrap(err, testHeterogeneousConStr))
	}
	defer testHeterogeneousDB.Close()

	testHeterogeneousDB.Exec(fmt.Sprintf("DROP USER %s", proxyUser))

	for _, qry := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY "+proxyPassword, proxyUser),
		fmt.Sprintf("GRANT CONNECT TO %s", proxyUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", proxyUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", proxyUser, username),
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", proxyUser, proxyPassword),
		fmt.Sprintf("GRANT CONNECT TO %s", proxyUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", proxyUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", proxyUser, username),
	} {
		if _, err := testHeterogeneousDB.Exec(qry); err != nil {
			t.Skip(errors.Wrap(err, qry))
		}
	}

	for tName, tCase := range map[string]struct {
		In   context.Context
		Want string
	}{
		"noContext": {In: context.TODO(), Want: username},
		"proxyUser": {In: goracle.ContextWithUserPassw(context.TODO(), proxyUser, proxyPassword), Want: proxyUser},
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
