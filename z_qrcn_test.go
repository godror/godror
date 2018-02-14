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
	"strings"
	"testing"

	"github.com/pkg/errors"
	goracle "gopkg.in/goracle.v2"
)

func TestQRCN(t *testing.T) {
	c, err := goracle.DriverConn(testDb)
	if err != nil {
		t.Fatal(err)
	}
	cb := func(e goracle.Event) {
		t.Log(e)
	}
	subscr, err := c.NewSubscription("test", cb)
	if err != nil {
		if strings.Contains(errors.Cause(err).Error(), "ORA-29970:") {
			t.Skip(err.Error())
		}
		t.Fatal(err)
	}
	defer subscr.Close()
	if err := subscr.Register("SELECT object_name FROM user_objects"); err != nil {
		t.Fatalf("%+v", err)
	}
}
