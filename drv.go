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
//
// The connection string for the sql.Open("goracle", connString) call can be
// the simple
//   loin/password@sid [AS SYSDBA|AS SYSOPER]
//
// type (with sid being the sexp returned by tnsping),
// or in the form of
//   ora://login:password@sid/? \
//     sysdba=0& \
//     sysoper=0& \
//     poolMinSessions=1& \
//     poolMaxSessions=1000& \
//     poolIncrement=1& \
//     connectionClass=POOLED
//
// These are the defaults. Many advocate that a static session pool (min=max, incr=0)
// is better, with 1-10 sessions per CPU thread.
// See http://docs.oracle.com/cd/E82638_01/JJUCP/optimizing-real-world-performance.htm#JJUCP-GUID-BC09F045-5D80-4AF5-93F5-FEF0531E0E1D
//
// If you specify connectionClass, that'll reuse the same session pool
// without the connectionClass, but will specify it on each session acquire.
// Thus you can cluster the session pool with classes, or ose POOLED for DRCP.
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
	"strconv"
	"strings"
	"sync"
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

	// DefaultPoolMinSessions specifies the default value for minSessions for pool creation.
	DefaultPoolMinSessions = 1
	// DefaultPoolMaxSessions specifies the default value for maxSessions for pool creation.
	DefaultPoolMaxSessions = 1000
	// DefaultPoolInrement specifies the default value for increment for pool creation.
	DefaultPoolIncrement = 1
)

// Number as string
type Number string

// Log function
var Log = func(...interface{}) error { return nil }

func init() {
	var d drv
	err := &oraErr{}
	if C.dpiContext_create(C.uint(DpiMajorVersion), C.uint(DpiMinorVersion),
		(**C.dpiContext)(unsafe.Pointer(&d.dpiContext)), &err.errInfo,
	) == C.DPI_FAILURE {
		panic(err)
	}
	d.pools = make(map[string]*C.dpiPool)

	sql.Register("goracle", &d)
}

var _ = driver.Driver((*drv)(nil))

type drv struct {
	dpiContext *C.dpiContext
	poolsMu    sync.Mutex
	pools      map[string]*C.dpiPool
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *drv) Open(connString string) (driver.Conn, error) {
	P, err := ParseConnString(connString)
	if err != nil {
		return nil, err
	}
	return d.openConn(P)
}

func (d *drv) openConn(P connectionParams) (*conn, error) {
	c := conn{drv: d, connParams: P}
	connString := P.StringNoClass()

	defer func() { d.poolsMu.Lock(); Log("pools", d.pools, "conn", P.String()); d.poolsMu.Unlock() }()
	authMode := C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	if P.IsSysDBA {
		authMode |= C.DPI_MODE_AUTH_SYSDBA
	} else if P.IsSysOper {
		authMode |= C.DPI_MODE_AUTH_SYSOPER
	}

	extAuth := C.int(b2i(P.Username == "" && P.Password == ""))
	connCreateParams := C.dpiConnCreateParams{
		authMode:     authMode,
		externalAuth: extAuth,
	}
	if P.ConnClass != "" {
		cConnClass := C.CString(P.ConnClass)
		defer C.free(unsafe.Pointer(cConnClass))
		connCreateParams.connectionClass = cConnClass
		connCreateParams.connectionClassLength = C.uint32_t(len(P.ConnClass))
	}
	if !(P.IsSysDBA || P.IsSysOper) {
		d.poolsMu.Lock()
		dp := d.pools[connString]
		d.poolsMu.Unlock()
		if dp != nil {
			dc := C.malloc(C.sizeof_void)
			if C.dpiPool_acquireConnection(
				dp,
				nil, 0, nil, 0, &connCreateParams,
				(**C.dpiConn)(unsafe.Pointer(&dc)),
			) == C.DPI_FAILURE {
				return nil, errors.Wrapf(d.getError(), "acquireConnection[%s]", P)
			}
			c.dpiConn = (*C.dpiConn)(dc)
			return &c, nil
		}
	}

	cUserName, cPassword, cSid := C.CString(P.Username), C.CString(P.Password), C.CString(P.SID)
	cUTF8, cConnClass := C.CString("AL32UTF8"), C.CString(P.ConnClass)
	cDriverName := C.CString(DriverName)
	defer func() {
		C.free(unsafe.Pointer(cUserName))
		C.free(unsafe.Pointer(cPassword))
		C.free(unsafe.Pointer(cSid))
		C.free(unsafe.Pointer(cUTF8))
		C.free(unsafe.Pointer(cConnClass))
		C.free(unsafe.Pointer(cDriverName))
	}()

	if P.IsSysDBA || P.IsSysOper {
		dc := C.malloc(C.sizeof_void)
		if C.dpiConn_create(
			d.dpiContext,
			cUserName, C.uint32_t(len(P.Username)),
			cPassword, C.uint32_t(len(P.Password)),
			cSid, C.uint32_t(len(P.SID)),
			&C.dpiCommonCreateParams{
				createMode: C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED | C.DPI_MODE_CREATE_EVENTS,
				encoding:   cUTF8, nencoding: cUTF8,
				driverName: cDriverName, driverNameLength: C.uint32_t(len(DriverName)),
			},
			&connCreateParams,
			(**C.dpiConn)(unsafe.Pointer(&dc)),
		) == C.DPI_FAILURE {
			return nil, errors.Wrapf(d.getError(), "username=%q password=%q sid=%q params=%+v", P.Username, P.Password, P.SID, connCreateParams)
		}
		c.dpiConn = (*C.dpiConn)(dc)
		return &c, nil
	}

	var dp *C.dpiPool
	if C.dpiPool_create(
		d.dpiContext,
		cUserName, C.uint32_t(len(P.Username)),
		cPassword, C.uint32_t(len(P.Password)),
		cSid, C.uint32_t(len(P.SID)),
		&C.dpiCommonCreateParams{
			createMode: C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED | C.DPI_MODE_CREATE_EVENTS,
			encoding:   cUTF8, nencoding: cUTF8,
			driverName: cDriverName, driverNameLength: C.uint32_t(len(DriverName)),
		},
		&C.dpiPoolCreateParams{
			minSessions:      C.uint32_t(P.MinSessions),
			maxSessions:      C.uint32_t(P.MaxSessions),
			sessionIncrement: C.uint32_t(P.PoolIncrement),
			homogeneous:      1,
			externalAuth:     extAuth,
			getMode:          C.DPI_MODE_POOL_GET_NOWAIT,
		},
		(**C.dpiPool)(unsafe.Pointer(&dp)),
	) == C.DPI_FAILURE {
		return nil, errors.Wrapf(d.getError(), "minSessions=%d maxSessions=%d poolIncrement=%d extAuth=%d", P.MinSessions, P.MaxSessions, P.PoolIncrement, extAuth)
	}
	C.dpiPool_setTimeout(dp, 300)
	//C.dpiPool_setMaxLifetimeSession(dp, 3600)
	C.dpiPool_setStmtCacheSize(dp, 1<<20)
	d.poolsMu.Lock()
	d.pools[connString] = dp
	d.poolsMu.Unlock()

	return d.openConn(P)
}

type connectionParams struct {
	Username, Password, SID, ConnClass      string
	IsSysDBA, IsSysOper                     bool
	MinSessions, MaxSessions, PoolIncrement int
}

func (P connectionParams) StringNoClass() string {
	return P.string(false)
}
func (P connectionParams) String() string {
	return P.string(true)
}

func (P connectionParams) string(class bool) string {
	cc := ""
	if class {
		cc = fmt.Sprintf("connectionClass=%s&", url.QueryEscape(P.ConnClass))
	}
	// params should be sorted lexicographically
	return fmt.Sprintf("oracle://%s:%s@%s/?"+
		cc+
		"poolIncrement=%d&poolMaxSessions=%d&poolMinSessions=%d&"+
		"sysdba=%d&sysoper=%d",
		P.Username, P.Password, P.SID,
		P.PoolIncrement, P.MaxSessions, P.MinSessions,
		b2i(P.IsSysDBA), b2i(P.IsSysOper),
	)
}

func ParseConnString(connString string) (connectionParams, error) {
	P := connectionParams{
		MinSessions:   DefaultPoolMinSessions,
		MaxSessions:   DefaultPoolMaxSessions,
		PoolIncrement: DefaultPoolIncrement,
	}
	if !strings.HasPrefix(connString, "oracle://") {
		i := strings.IndexByte(connString, '/')
		if i < 0 {
			return P, errors.Errorf("no / in %q", connString)
		}
		P.Username, connString = connString[:i], connString[i+1:]
		if i = strings.IndexByte(connString, '@'); i < 0 {
			return P, errors.Errorf("no @ in %q", connString)
		}
		P.Password, P.SID = connString[:i], connString[i+1:]
		uSid := strings.ToUpper(P.SID)
		if P.IsSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); P.IsSysDBA {
			P.SID = P.SID[:len(P.SID)-10]
		} else if P.IsSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); P.IsSysOper {
			P.SID = P.SID[:len(P.SID)-11]
		}
		if strings.HasSuffix(P.SID, ":POOLED") {
			P.ConnClass, P.SID = "POOLED", P.SID[:len(P.SID)-7]
		}
		return P, nil
	}
	u, err := url.Parse(connString)
	if err != nil {
		return P, err
	}
	if usr := u.User; usr != nil {
		P.Username = usr.Username()
		P.Password, _ = usr.Password()
	}
	P.SID = u.Hostname()
	if u.Port() != "" {
		P.SID += ":" + u.Port()
	}
	q := u.Query()
	if vv, ok := q["connectionClass"]; ok {
		P.ConnClass = vv[0]
	}
	if P.IsSysDBA = q.Get("sysdba") == "1"; !P.IsSysDBA {
		P.IsSysOper = q.Get("sysoper") == "1"
	}

	for _, task := range []struct {
		Dest *int
		Key  string
	}{
		{&P.MinSessions, "poolMinSessions"},
		{&P.MaxSessions, "poolMaxSessions"},
		{&P.PoolIncrement, "poolIncrement"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = strconv.Atoi(s)
		if err != nil {
			return P, errors.Wrap(err, task.Key+"="+s)
		}
	}
	if P.MinSessions > P.MaxSessions {
		P.MinSessions = P.MaxSessions
	}
	if P.MinSessions == P.MaxSessions {
		P.PoolIncrement = 0
	} else if P.PoolIncrement < 1 {
		P.PoolIncrement = 1
	}

	return P, nil
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

func b2i(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
