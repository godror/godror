// +build !go1.10

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

/*
#include <stdlib.h>
#include "dpiImpl.h"
*/
import "C"
import "unsafe"

const go10 = false

func dpi_setFromString(dv *C.dpiVar, pos C.uint32_t, x string) {
	b := []byte(x)
	p = (*C.char)(unsafe.Pointer(&b[0]))
	//if Log != nil {Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(b)) }
	C.dpiVar_setFromBytes(dv, pos, p, C.uint32_t(len(b)))
}
