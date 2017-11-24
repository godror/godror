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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseConnString(t *testing.T) {
	wantAt := ConnectionParams{
		Username: "cc", Password: "c@c*1", SID: "192.168.1.1/cc",
	}
	wantDefault := ConnectionParams{
		Username: "user", Password: "pass", SID: "sid",
		ConnClass:   DefaultConnectionClass,
		MinSessions: DefaultPoolMinSessions, MaxSessions: DefaultPoolMaxSessions,
		PoolIncrement: DefaultPoolIncrement}

	wantXO := wantDefault
	wantXO.SID = "localhost/sid"

	for tName, tCase := range map[string]struct {
		In   string
		Want ConnectionParams
	}{
		"simple": {In: "user/pass@sid", Want: wantDefault},
		"full": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=POOLED&sysoper=1&sysdba=0",
			Want: ConnectionParams{Username: "user", Password: "pass", SID: "sid",
				ConnClass: "POOLED", IsSysOper: true,
				MinSessions: 3, MaxSessions: 9, PoolIncrement: 3}},

		"@": {In: wantAt.String(), Want: wantAt},

		"xo": {In: "oracle://user:pass@localhost/sid", Want: wantXO},
	} {
		t.Log(tCase.In)
		P, err := ParseConnString(tCase.In)
		if err != nil {
			t.Errorf("%s: %v", tName, err)
			continue
		}
		if P != tCase.Want {
			t.Errorf("%s: got %+v, wanted %+v\n%s", tName, P, tCase.Want, cmp.Diff(tCase.Want, P))
			continue
		}
		s := P.String()
		Q, err := ParseConnString(s)
		if err != nil {
			t.Errorf("%s: %v", tName, err)
			continue
		}
		if P != Q {
			t.Errorf("%s: got %+v, wanted %+v\n%s", tName, P, Q, cmp.Diff(P, Q))
			continue
		}
		if got := Q.String(); s != got {
			t.Errorf("%s: got %q, wanted %q", tName, got, s)
		}
	}
}
