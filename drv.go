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

// Package goracle is a database/sql/driver for Oracle DB.
package goracle

//go:generate git submodule update --init --recursive
//go:generate sh -c "cd odpi && make"
//go:generate echo "sudo cp -a odpi/lib/libodpic.so /usr/local/lib/"
//go:generate echo "sudo ldconfig /usr/local/lib"

/*
//#cgo pkg-config: --define-variable=GOPATH=$GOPATH odpi
#cgo CFLAGS: -I./odpi/include
#cgo LDFLAGS: -Lodpi/lib -lodpic -ldl -s

#include <stdlib.h>
#include <dpi.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
)

// Version of this driver
const Version = "v5.0.0"

const (
	// DpiMajorVersion is the wanted major version of the underlying ODPI-C library.
	DpiMajorVersion = 2
	// DpiMinorVersion is the wanted minor version of the underlying ODPI-C library.
	DpiMinorVersion = 0

	// DriverName is set on the connection to be seen in the DB
	DriverName = "gopkg.in/rana/ora.v5 : " + Version
)

func init() {
	var d drv
	err := &oraErr{}
	if C.dpiContext_create(C.uint(DpiMajorVersion), C.uint(DpiMinorVersion),
		(**C.dpiContext)(unsafe.Pointer(&d.dpiContext)), &err.errInfo,
	) == C.DPI_FAILURE {
		panic(err)
	}

	sql.Register("goracle", &d)
}

var _ = driver.Driver((*drv)(nil))

type drv struct {
	dpiContext *C.dpiContext
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *drv) Open(connString string) (driver.Conn, error) {
	var username, password, sid, connClass string
	var isSysDBA, isSysOper bool
	if strings.HasPrefix(connString, "ora://") {
		u, err := url.Parse(connString)
		if err != nil {
			return nil, err
		}
		if usr := u.User; usr != nil {
			username = usr.Username()
			password, _ = usr.Password()
		}
		sid = u.Hostname()
		if u.Port() != "" {
			sid += ":" + u.Port()
		}
		q := u.Query()
		if isSysDBA = q.Get("sysdba") == "1"; !isSysDBA {
			isSysOper = q.Get("sysoper") == "1"
		}
	} else {
		i := strings.IndexByte(connString, '/')
		if i < 0 {
			return nil, errors.Errorf("no / in %q", connString)
		}
		username, connString = connString[:i], connString[i+1:]
		if i = strings.IndexByte(connString, '@'); i < 0 {
			return nil, errors.Errorf("no @ in %q", connString)
		}
		password, sid = connString[:i], connString[i+1:]
		uSid := strings.ToUpper(sid)
		if isSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); isSysDBA {
			sid = sid[:len(sid)-10]
		} else if isSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); isSysOper {
			sid = sid[:len(sid)-11]
		}
		if strings.HasSuffix(sid, ":POOLED") {
			connClass, sid = "POOLED", sid[:len(sid)-7]
		}
	}

	authMode := C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	if isSysDBA {
		authMode |= C.DPI_MODE_AUTH_SYSDBA
	} else if isSysOper {
		authMode |= C.DPI_MODE_AUTH_SYSOPER
	}

	c := conn{drv: d, connString: connString}
	cUserName, cPassword, cSid := C.CString(username), C.CString(password), C.CString(sid)
	cUTF8, cConnClass := C.CString("AL32UTF8"), C.CString(connClass)
	cDriverName := C.CString(DriverName)
	defer func() {
		C.free(unsafe.Pointer(cUserName))
		C.free(unsafe.Pointer(cPassword))
		C.free(unsafe.Pointer(cSid))
		C.free(unsafe.Pointer(cUTF8))
		C.free(unsafe.Pointer(cConnClass))
		C.free(unsafe.Pointer(cDriverName))
	}()
	var extAuth C.int
	if username == "" && password == "" {
		extAuth = 1
	}
	_ = extAuth
	dc := C.malloc(C.sizeof_void)
	if C.dpiConn_create(
		d.dpiContext,
		cUserName, C.uint32_t(len(username)),
		cPassword, C.uint32_t(len(password)),
		cSid, C.uint32_t(len(sid)),
		&C.dpiCommonCreateParams{
			createMode: C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED | C.DPI_MODE_CREATE_EVENTS,
			encoding:   cUTF8, nencoding: cUTF8,
			driverName: cDriverName, driverNameLength: C.uint32_t(len(DriverName)),
		},
		&C.dpiConnCreateParams{
			authMode:        authMode,
			connectionClass: cConnClass, connectionClassLength: C.uint32_t(len(connClass)),
			externalAuth: extAuth,
		},
		(**C.dpiConn)(unsafe.Pointer(&dc)),
	) == C.DPI_FAILURE {
		return nil, d.getError()
	}
	c.dpiConn = (*C.dpiConn)(dc)
	return &c, nil
}

type oraErr struct {
	errInfo C.dpiErrorInfo
}

func (oe *oraErr) Code() int       { return int(oe.errInfo.code) }
func (oe *oraErr) Message() string { return C.GoString(oe.errInfo.message) }
func (oe *oraErr) Error() string {
	msg := oe.Message()
	if oe.errInfo.code == 0 && msg == "" {
		return ""
	}
	prefix := fmt.Sprintf("ORA-%05d: ", oe.Code())
	if strings.HasPrefix(msg, prefix) {
		return msg
	}
	return prefix + msg
}

func (d *drv) getError() *oraErr {
	var oe oraErr
	C.dpiContext_getError(d.dpiContext, &oe.errInfo)
	return &oe
}
