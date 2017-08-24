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

package goracle

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
)

func TestSubscr(t *testing.T) {
	_, testCon, err := initConn()
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan Event, 3)
	s, err := testCon.NewSubscription(events, "subscr")
	if err != nil {
		if strings.Contains(errors.Cause(err).Error(), "ORA-29970:") {
			t.Skip(err.Error())
		}
		t.Fatalf("%+v", err)
	}
	defer s.Close()
	if err := s.Register("SELECT object_name, TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') last_ddl_time FROM user_objects"); err != nil {
		t.Fatalf("%+v", err)
	}
}
