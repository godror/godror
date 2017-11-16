// +build go1.10

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

import "C"

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (st *statement) NumInput() int {
	if st.dpiStmt == nil {
		if st.query == getConnection {
			return 1
		}
		return 0
	}

	st.Lock()
	defer st.Unlock()
	var cnt C.uint32_t
	//defer func() { fmt.Printf("%p.NumInput=%d (%q)\n", st, cnt, st.query) }()
	if C.dpiStmt_getBindCount(st.dpiStmt, &cnt) == C.DPI_FAILURE {
		return -1
	}
	if cnt < 2 { // 1 can't decrease...
		return int(cnt)
	}
	names := make([]*C.char, int(cnt))
	lengths := make([]C.uint32_t, int(cnt))
	if C.dpiStmt_getBindNames(st.dpiStmt, &cnt, &names[0], &lengths[0]) == C.DPI_FAILURE {
		return -1
	}
	//fmt.Printf("%p.NumInput=%d\n", st, cnt)

	// return the number of *unique* arguments
	return int(cnt)
}
