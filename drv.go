// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

// Package godror is a database/sql/driver for Oracle DB.
//
// The connection string for the sql.Open("godror", connString) call can be
// the simple
//   login/password@sid [AS SYSDBA|AS SYSOPER]
//
// type (with sid being the sexp returned by tnsping),
// or in the form of
//   ora://login:password@sid/? \
//     sysdba=0& \
//     sysoper=0& \
//     poolMinSessions=1& \
//     poolMaxSessions=1000& \
//     poolIncrement=1& \
//     connectionClass=POOLED& \
//     standaloneConnection=0& \
//     enableEvents=0& \
//     heterogeneousPool=0& \
//     prelim=0& \
//     poolWaitTimeout=5m& \
//     poolSessionMaxLifetime=1h& \
//     poolSessionTimeout=30s& \
//     timezone=Local& \
//     newPassword= \
//     onInit=ALTER+SESSION+SET+current_schema%3Dmy_schema
//
// These are the defaults. Many advocate that a static session pool (min=max, incr=0)
// is better, with 1-10 sessions per CPU thread.
// See http://docs.oracle.com/cd/E82638_01/JJUCP/optimizing-real-world-performance.htm#JJUCP-GUID-BC09F045-5D80-4AF5-93F5-FEF0531E0E1D
// You may also use ConnectionParams to configure a connection.
//
// If you specify connectionClass, that'll reuse the same session pool
// without the connectionClass, but will specify it on each session acquire.
// Thus you can cluster the session pool with classes, or use POOLED for DRCP.
//
// For what can be used as "sid", see https://docs.oracle.com/en/database/oracle/oracle-database/19/netag/configuring-naming-methods.html#GUID-E5358DEA-D619-4B7B-A799-3D2F802500F1
package godror

/*
#cgo CFLAGS: -I./odpi/include -I./odpi/src -I./odpi/embed

#include <stdlib.h>

#include "dpi.c"
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	errors "golang.org/x/xerrors"
)

const (
	// DefaultFetchRowCount is the number of prefetched rows by default (if not changed through FetchRowCount statement option).
	DefaultFetchRowCount = 1 << 8

	// DefaultArraySize is the length of the maximum PL/SQL array by default (if not changed through ArraySize statement option).
	DefaultArraySize = 1 << 10
)

const (
	// DpiMajorVersion is the wanted major version of the underlying ODPI-C library.
	DpiMajorVersion = C.DPI_MAJOR_VERSION
	// DpiMinorVersion is the wanted minor version of the underlying ODPI-C library.
	DpiMinorVersion = C.DPI_MINOR_VERSION
	// DpiPatchLevel is the patch level version of the underlying ODPI-C library
	DpiPatchLevel = C.DPI_PATCH_LEVEL
	// DpiVersionNumber is the underlying ODPI-C version as one number (Major * 10000 + Minor * 100 + Patch)
	DpiVersionNumber = C.DPI_VERSION_NUMBER

	// DriverName is set on the connection to be seen in the DB
	//
	// It cannot be longer than 30 bytes !
	DriverName = "godror : " + Version

	// DefaultPoolMinSessions specifies the default value for minSessions for pool creation.
	DefaultPoolMinSessions = 1
	// DefaultPoolMaxSessions specifies the default value for maxSessions for pool creation.
	DefaultPoolMaxSessions = 1000
	// DefaultSessionIncrement specifies the default value for increment for pool creation.
	DefaultSessionIncrement = 1
	// DefaultPoolIncrement is a deprecated name for DefaultSessionIncrement.
	DefaultPoolIncrement = DefaultSessionIncrement
	// DefaultConnectionClass is the default connectionClass
	DefaultConnectionClass = "GODROR"
	// NoConnectionPoolingConnectionClass is a special connection class name to indicate no connection pooling.
	// It is the same as setting standaloneConnection=1
	NoConnectionPoolingConnectionClass = "NO-CONNECTION-POOLING"
	// DefaultSessionTimeout is the seconds before idle pool sessions get evicted
	DefaultSessionTimeout = 5 * time.Minute
	// DefaultWaitTimeout is the milliseconds to wait for a session to become available
	DefaultWaitTimeout = 30 * time.Second
	// DefaultMaxLifeTime is the maximum time in seconds till a pooled session may exist
	DefaultMaxLifeTime = 1 * time.Hour
)

// Log function. By default, it's nil, and thus logs nothing.
// If you want to change this, change it to a github.com/go-kit/kit/log.Swapper.Log
// or analog to be race-free.
var Log func(...interface{}) error

var defaultDrv = &drv{}

func init() {
	sql.Register("godror", defaultDrv)
}

var _ = driver.Driver((*drv)(nil))

type drv struct {
	mu            sync.Mutex
	dpiContext    *C.dpiContext
	pools         map[string]*connPool
	clientVersion VersionInfo
}

type connPool struct {
	dpiPool       *C.dpiPool
	params        PoolParams
	timeZone      *time.Location
	tzOffSecs     int
	serverVersion VersionInfo
}

func (d *drv) init() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pools == nil {
		d.pools = make(map[string]*connPool)
	}
	if d.dpiContext != nil {
		return nil
	}
	var errInfo C.dpiErrorInfo
	var dpiCtx *C.dpiContext
	if C.dpiContext_create(C.uint(DpiMajorVersion), C.uint(DpiMinorVersion),
		(**C.dpiContext)(unsafe.Pointer(&dpiCtx)), &errInfo,
	) == C.DPI_FAILURE {
		return fromErrorInfo(errInfo)
	}
	d.dpiContext = dpiCtx

	var v C.dpiVersionInfo
	if C.dpiContext_getClientVersion(d.dpiContext, &v) == C.DPI_FAILURE {
		return errors.Errorf("%s: %w", "getClientVersion", d.getError())
	}
	d.clientVersion.set(&v)
	return nil
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *drv) Open(connString string) (driver.Conn, error) {
	c, err := d.OpenConnector(connString)
	if err != nil {
		return nil, err
	}
	cx := c.(connector)
	return d.createConnFromParams(cx.PoolParams, cx.ConnParams)
}

func (d *drv) ClientVersion() (VersionInfo, error) {
	return d.clientVersion, nil
}

var cUTF8, cDriverName = C.CString("UTF-8"), C.CString(DriverName)

//-----------------------------------------------------------------------------
// initCommonCreateParams()
//   Initializes ODPI-C common creation parameters used for creating pools and
// standalone connections. The C strings for the encoding and driver name are
// defined at the package level for convenience.
//-----------------------------------------------------------------------------
func (d *drv) initCommonCreateParams(P *C.dpiCommonCreateParams, enableEvents bool) error {

	// initialize ODPI-C structure for common creation parameters
	if C.dpiContext_initCommonCreateParams(d.dpiContext, P) == C.DPI_FAILURE {
		return errors.Errorf("initCommonCreateParams: %w", d.getError())
	}

	// assign encoding and national encoding (always use UTF-8)
	P.encoding = cUTF8
	P.nencoding = cUTF8

	// assign driver name
	P.driverName = cDriverName
	P.driverNameLength = C.uint32_t(len(DriverName))

	// assign creation mode; always use threaded mode in order to allow
	// goroutines to function without mutexing; enable events mode, if
	// requested
	P.createMode = C.DPI_MODE_CREATE_DEFAULT | C.DPI_MODE_CREATE_THREADED
	if enableEvents {
		P.createMode |= C.DPI_MODE_CREATE_EVENTS
	}

	return nil
}

//-----------------------------------------------------------------------------
// createConn()
//   Creates an ODPI-C connection with the specified parameters. If a pool is
// provided, the connection is acquired from the pool; otherwise, a standalone
// connection is created.
//-----------------------------------------------------------------------------
func (d *drv) createConn(pool *connPool, P ConnParams) (*conn, error) {

	// initialize driver, if necessary
	if err := d.init(); err != nil {
		return nil, err
	}

	// initialize ODPI-C structure for common creation parameters; this is only
	// used when a standalone connection is being created; when a connection is
	// being acquired from the pool this structure is not needed
	var commonCreateParamsPtr *C.dpiCommonCreateParams
	var commonCreateParams C.dpiCommonCreateParams
	if pool == nil {
		if err := d.initCommonCreateParams(&commonCreateParams,
			P.EnableEvents); err != nil {
			return nil, err
		}
		commonCreateParamsPtr = &commonCreateParams
	}

	// manage strings
	var cUserName, cPassword, cNewPassword, cDSN, cConnClass *C.char
	defer func() {
		if cUserName != nil {
			C.free(unsafe.Pointer(cUserName))
		}
		if cPassword != nil {
			C.free(unsafe.Pointer(cPassword))
		}
		if cNewPassword != nil {
			C.free(unsafe.Pointer(cNewPassword))
		}
		if cDSN != nil {
			C.free(unsafe.Pointer(cDSN))
		}
		if cConnClass != nil {
			C.free(unsafe.Pointer(cConnClass))
		}
	}()

	// initialize ODPI-C structure for connection creation parameters
	var connCreateParams C.dpiConnCreateParams
	if C.dpiContext_initConnCreateParams(d.dpiContext,
		&connCreateParams) == C.DPI_FAILURE {
		return nil, errors.Errorf("initConnCreateParams: %w", d.getError())
	}

	// assign connection class
	if P.ConnClass != "" {
		cConnClass := C.CString(P.ConnClass)
		connCreateParams.connectionClass = cConnClass
		connCreateParams.connectionClassLength = C.uint32_t(len(P.ConnClass))
	}

	// assign new password (only relevant for standalone connections)
	if pool == nil && P.NewPassword != "" {
		cNewPassword = C.CString(P.NewPassword)
		connCreateParams.newPassword = cNewPassword
		connCreateParams.newPasswordLength = C.uint32_t(len(P.NewPassword))
	}

	// assign external authentication flag (only relevant for standalone
	// connections)
	if pool == nil && P.UserName == "" && P.Password == "" {
		connCreateParams.externalAuth = 1
	}

	// assign authorization mode
	connCreateParams.authMode = C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	if P.IsSysDBA {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSDBA
	}
	if P.IsSysOper {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSOPER
	}
	if P.IsSysASM {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_SYSASM
	}
	if P.IsPrelim {
		connCreateParams.authMode |= C.DPI_MODE_AUTH_PRELIM
	}

	// assign sharding keys, if applicable
	if len(P.ShardingKey) > 0 {
		var tempData C.dpiData
		mem := C.malloc(C.sizeof_dpiShardingKeyColumn *
			C.size_t(len(P.ShardingKey)))
		defer C.free(mem)
		columns := (*[1 << 30]C.dpiShardingKeyColumn)(mem)
		for i, value := range P.ShardingKey {
			switch value.(type) {
			case int:
				columns[i].oracleTypeNum = C.DPI_ORACLE_TYPE_NUMBER
				columns[i].nativeTypeNum = C.DPI_NATIVE_TYPE_INT64
				C.dpiData_setInt64(&tempData, C.int64_t(value.(int)))
			case string:
				columns[i].oracleTypeNum = C.DPI_ORACLE_TYPE_VARCHAR
				columns[i].nativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
				strVal := value.(string)
				cs := C.CString(strVal)
				defer C.free(unsafe.Pointer(cs))
				C.dpiData_setBytes(&tempData, cs, C.uint32_t(len(strVal)))
			default:
				return nil, errors.New("Unsupported data type for sharding")
			}
			columns[i].value = tempData.value
		}
		connCreateParams.shardingKeyColumns = &columns[0]
		connCreateParams.numShardingKeyColumns = C.uint8_t(len(P.ShardingKey))
	}

	// if a pool was provided, assign the pool
	if pool != nil {
		connCreateParams.pool = pool.dpiPool
	}

	// setup credentials
	if P.UserName != "" {
		cUserName = C.CString(P.UserName)
	}
	if P.Password != "" {
		cPassword = C.CString(P.Password)
	}
	if P.DSN != "" {
		cDSN = C.CString(P.DSN)
	}

	// create ODPI-C connection
	var dc *C.dpiConn
	if C.dpiConn_create(
		d.dpiContext,
		cUserName, C.uint32_t(len(P.UserName)),
		cPassword, C.uint32_t(len(P.Password)),
		cDSN, C.uint32_t(len(P.DSN)),
		commonCreateParamsPtr,
		&connCreateParams, &dc,
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("username=%q dsn=%q params=%+v: %w",
			P.UserName, P.DSN, connCreateParams, d.getError())
	}

	// create connection and initialize it, if needed
	var poolParams PoolParams
	currentUser := P.UserName
	if pool != nil {
		poolParams = pool.params
		if currentUser == "" {
			currentUser = poolParams.UserName
		}
	}
	c := conn{drv: d, Client: d.clientVersion, dpiConn: dc,
		currentUser: currentUser, timeZone: P.Timezone,
		poolParams: poolParams, connParams: P,
		newSession: pool == nil || connCreateParams.outNewSession == 1}
	c.init(P.OnInit)
	return &c, nil
}

//-----------------------------------------------------------------------------
// createConnFromParams()
//   Creates a driver connection given pool parameters and connection
// parameters. The pool parameters are used to either create a pool or use an
// existing cached pool. If the pool parameters are nil, no pool is used and a
// standalone connection is created instead. The connection parameters are used
// to acquire a connection from the pool specified by the pool parameters or
// are used to create a standalone connection.
//-----------------------------------------------------------------------------
func (d *drv) createConnFromParams(PP PoolParams, CP ConnParams) (*conn, error) {

	var err error
	var pool *connPool
	if !PP.IsZero() {
		pool, err = d.getPool(PP)
		if err != nil {
			return nil, err
		}
	}
	conn, err := d.createConn(pool, CP)
	if err != nil || PP.IsZero() || PP.OnInit == nil || !conn.newSession {
		return conn, err
	}
	if err = PP.OnInit(conn); err != nil {
		conn.close(true)
		return nil, err
	}
	return conn, nil
}

//-----------------------------------------------------------------------------
// getPool()
//   Get the pool to use given the set of pool parameters provided. Pools are
// stored in a map keyed by a string representation of the pool parameters. If
// no pool exists, a pool is created and stored in the map.
//-----------------------------------------------------------------------------
func (d *drv) getPool(P PoolParams) (*connPool, error) {

	// initialize driver, if necessary
	if err := d.init(); err != nil {
		return nil, err
	}

	// determine key to use for pool
	poolKey := fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%t\t%t\t%t",
		P.UserName, P.DSN, P.MinSessions, P.MaxSessions,
		P.SessionIncrement, P.WaitTimeout, P.MaxLifeTime, P.SessionTimeout,
		P.Heterogeneous, P.EnableEvents, P.ExternalAuth)
	if Log != nil {
		Log("pool key:", poolKey)
	}

	// if pool already exists, return it immediately; otherwise, create a new
	// pool; hold the lock while the pool is looked up (and created, if needed)
	// in order to ensure that multiple goroutines do not attempt to create a
	// pool
	d.mu.Lock()
	defer d.mu.Unlock()
	pool, ok := d.pools[poolKey]
	if ok {
		return pool, nil
	}
	pool, err := d.createPool(P)
	if err != nil {
		return nil, err
	}
	d.pools[poolKey] = pool
	return pool, nil
}

//-----------------------------------------------------------------------------
// createPool()
//   Creates an ODPI-C pool with the specified parameters. This is done while
// holding the mutex in order to ensure that multiple goroutines do not attempt
// to create the pool at the same time.
//-----------------------------------------------------------------------------
func (d *drv) createPool(P PoolParams) (*connPool, error) {

	// set up common creation parameters
	var commonCreateParams C.dpiCommonCreateParams
	if err := d.initCommonCreateParams(&commonCreateParams,
		P.EnableEvents); err != nil {
		return nil, err
	}

	// initialize ODPI-C structure for pool creation parameters
	var poolCreateParams C.dpiPoolCreateParams
	if C.dpiContext_initPoolCreateParams(d.dpiContext,
		&poolCreateParams) == C.DPI_FAILURE {
		return nil, errors.Errorf("initPoolCreateParams: %w", d.getError())
	}

	// assign minimum number of sessions permitted in the pool
	poolCreateParams.minSessions = DefaultPoolMinSessions
	if P.MinSessions >= 0 {
		poolCreateParams.minSessions = C.uint32_t(P.MinSessions)
	}

	// assign maximum number of sessions permitted in the pool
	poolCreateParams.maxSessions = DefaultPoolMaxSessions
	if P.MaxSessions > 0 {
		poolCreateParams.maxSessions = C.uint32_t(P.MaxSessions)
	}

	// assign the number of sessions to create each time more is needed
	poolCreateParams.sessionIncrement = DefaultPoolIncrement
	if P.SessionIncrement > 0 {
		poolCreateParams.sessionIncrement = C.uint32_t(P.SessionIncrement)
	}

	// assign "get" mode (always used timed wait)
	poolCreateParams.getMode = C.DPI_MODE_POOL_GET_TIMEDWAIT

	// assign wait timeout (number of milliseconds to wait for a session to
	// become available
	poolCreateParams.waitTimeout =
		C.uint32_t(DefaultWaitTimeout / time.Millisecond)
	if P.WaitTimeout > 0 {
		poolCreateParams.waitTimeout =
			C.uint32_t(P.WaitTimeout / time.Millisecond)
	}

	// assign timeout (number of seconds before idle pool session are evicted
	// from the pool
	poolCreateParams.timeout = C.uint32_t(DefaultSessionTimeout / time.Second)
	if P.SessionTimeout > 0 {
		poolCreateParams.timeout = C.uint32_t(P.SessionTimeout / time.Second)
	}

	// assign maximum lifetime (number of seconds a pooled session may exist)
	poolCreateParams.maxLifetimeSession =
		C.uint32_t(DefaultMaxLifeTime / time.Second)
	if P.MaxLifeTime > 0 {
		poolCreateParams.maxLifetimeSession =
			C.uint32_t(P.MaxLifeTime / time.Second)
	}

	// assign external authentication flag
	poolCreateParams.externalAuth = C.int(b2i(P.ExternalAuth))

	// assign homogeneous pool flag; default is true so need to clear the flag
	// if specifically reqeuested or if external authentication is desirable
	if poolCreateParams.externalAuth == 1 || P.Heterogeneous {
		poolCreateParams.homogeneous = 0
	}

	// setup credentials
	var cUserName, cPassword, cDSN *C.char
	if P.UserName != "" {
		cUserName = C.CString(P.UserName)
		defer C.free(unsafe.Pointer(cUserName))
	}
	if P.Password != "" {
		cPassword = C.CString(P.Password)
		defer C.free(unsafe.Pointer(cPassword))
	}
	if P.DSN != "" {
		cDSN = C.CString(P.DSN)
		defer C.free(unsafe.Pointer(cDSN))
	}

	// create pool
	var dp *C.dpiPool
	if Log != nil {
		Log("C", "dpiPool_create", "username", P.UserName, "DSN", P.DSN,
			"common", commonCreateParams, "pool",
			fmt.Sprintf("%#v", poolCreateParams))
	}
	if C.dpiPool_create(
		d.dpiContext,
		cUserName, C.uint32_t(len(P.UserName)),
		cPassword, C.uint32_t(len(P.Password)),
		cDSN, C.uint32_t(len(P.DSN)),
		&commonCreateParams,
		&poolCreateParams,
		(**C.dpiPool)(unsafe.Pointer(&dp)),
	) == C.DPI_FAILURE {
		return nil, errors.Errorf("params=%s extAuth=%v: %w", P,
			poolCreateParams.externalAuth, d.getError())
	}

	// set statement cache
	C.dpiPool_setStmtCacheSize(dp, 40)

	return &connPool{dpiPool: dp, params: P, timeZone: P.Timezone}, nil
}

// ConnectionParams holds the params for a connection (pool).
// You can use ConnectionParams{...}.StringWithPassword()
// as a connection string in sql.Open.
type ConnectionParams struct {
	ConnParams
	PoolParams
	// NewPassword is used iff StandaloneConnection is true!
	NewPassword          string
	StandaloneConnection bool
}

// String returns the string representation of ConnectionParams.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) String() string {
	return P.string(true, false)
}

// StringNoClass returns the string representation of ConnectionParams, without class info.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) StringNoClass() string {
	return P.string(false, false)
}

// StringWithPassword returns the string representation of ConnectionParams (as String() does),
// but does NOT obfuscate the password, just prints it as is.
func (P ConnectionParams) StringWithPassword() string {
	return P.string(true, true)
}

func (P ConnectionParams) string(class, withPassword bool) string {
	host, path := P.ConnParams.DSN, ""
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host, path = host[:i], host[i:]
	}
	q := make(url.Values, 32)
	s := P.ConnClass
	if !class {
		s = ""
	}
	q.Add("connectionClass", s)

	password := P.ConnParams.Password
	if withPassword {
		q.Add("newPassword", P.NewPassword)
	} else {
		hsh := fnv.New64()
		io.WriteString(hsh, P.ConnParams.Password)
		password = "SECRET-" + base64.URLEncoding.EncodeToString(hsh.Sum(nil))
		if P.NewPassword != "" {
			hsh.Reset()
			io.WriteString(hsh, P.NewPassword)
			q.Add("newPassword", "SECRET-"+base64.URLEncoding.EncodeToString(hsh.Sum(nil)))
		}
	}
	s = "local"
	if P.ConnParams.Timezone != nil && P.ConnParams.Timezone != time.Local {
		s = P.ConnParams.Timezone.String()
	}
	q.Add("timezone", s)
	B := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	q.Add("poolMinSessions", strconv.Itoa(P.MinSessions))
	q.Add("poolMaxSessions", strconv.Itoa(P.MaxSessions))
	q.Add("poolIncrement", strconv.Itoa(P.SessionIncrement))
	q.Add("sysdba", B(P.IsSysDBA))
	q.Add("sysoper", B(P.IsSysOper))
	q.Add("sysasm", B(P.IsSysASM))
	q.Add("standaloneConnection", B(P.StandaloneConnection))
	q.Add("enableEvents", B(P.ConnParams.EnableEvents))
	q.Add("heterogeneousPool", B(P.Heterogeneous))
	q.Add("prelim", B(P.IsPrelim))
	q.Add("poolWaitTimeout", P.WaitTimeout.String())
	q.Add("poolSessionMaxLifetime", P.MaxLifeTime.String())
	q.Add("poolSessionTimeout", P.SessionTimeout.String())
	q["onInit"] = P.ConnParams.OnInit
	return (&url.URL{
		Scheme:   "oracle",
		User:     url.UserPassword(P.ConnParams.UserName, password),
		Host:     host,
		Path:     path,
		RawQuery: q.Encode(),
	}).String()
}

// Comb the several contradicting settings of the ConnectionParams.
func (P *ConnectionParams) Comb() {
	P.StandaloneConnection = P.StandaloneConnection || P.ConnClass == NoConnectionPoolingConnectionClass
	if P.IsPrelim || P.StandaloneConnection {
		// Prelim: the shared memory may not exist when Oracle is shut down.
		P.ConnClass = ""
		P.Heterogeneous = false
	}
	if P.ConnParams.Timezone == nil {
		if P.PoolParams.Timezone != nil {
			P.ConnParams.Timezone = P.PoolParams.Timezone
		} else {
			P.ConnParams.Timezone = time.Local
		}
	}
}

// ParseConnString parses the given connection string into a struct.
func ParseConnString(connString string) (ConnectionParams, error) {
	P := ConnectionParams{
		ConnParams: ConnParams{
			ConnClass: DefaultConnectionClass,
		},
		PoolParams: PoolParams{
			MinSessions:      DefaultPoolMinSessions,
			MaxSessions:      DefaultPoolMaxSessions,
			SessionIncrement: DefaultPoolIncrement,
			MaxLifeTime:      DefaultMaxLifeTime,
			WaitTimeout:      DefaultWaitTimeout,
			SessionTimeout:   DefaultSessionTimeout,
			Timezone:         time.Local,
		},
	}
	var username, password, dsn string
	if !strings.HasPrefix(connString, "oracle://") {
		i := strings.IndexByte(connString, '/')
		if i < 0 {
			return P, errors.New("no '/' in connection string")
		}
		username, connString = connString[:i], connString[i+1:]

		uSid := strings.ToUpper(connString)
		//fmt.Printf("connString=%q SID=%q\n", connString, uSid)
		if strings.Contains(uSid, " AS ") {
			if P.IsSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); P.IsSysDBA {
				connString = connString[:len(connString)-10]
			} else if P.IsSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); P.IsSysOper {
				connString = connString[:len(connString)-11]
			} else if P.IsSysASM = strings.HasSuffix(uSid, " AS SYSASM"); P.IsSysASM {
				connString = connString[:len(connString)-10]
			}
		}
		if i = strings.LastIndexByte(connString, '@'); i >= 0 {
			password, dsn = connString[:i], connString[i+1:]
		} else {
			password = connString
		}
		if strings.HasSuffix(dsn, ":POOLED") {
			P.ConnClass, dsn = "POOLED", dsn[:len(dsn)-7]
		}
		//fmt.Printf("connString=%q params=%s\n", connString, P)
		P.ConnParams.UserName, P.ConnParams.Password, P.ConnParams.DSN = username, password, dsn
		P.PoolParams.UserName, P.PoolParams.Password, P.PoolParams.DSN = username, password, dsn
		return P, nil
	}
	u, err := url.Parse(connString)
	if err != nil {
		return P, errors.Errorf("%s: %w", connString, err)
	}
	if usr := u.User; usr != nil {
		username = usr.Username()
		P.ConnParams.Password, _ = usr.Password()
	}
	dsn = u.Hostname()
	// IPv6 literal address brackets are removed by u.Hostname,
	// so we have to put them back
	if strings.HasPrefix(u.Host, "[") && !strings.Contains(dsn[1:], "]") {
		dsn = "[" + dsn + "]"
	}
	if u.Port() != "" {
		dsn += ":" + u.Port()
	}
	if u.Path != "" && u.Path != "/" {
		dsn += u.Path
	}
	P.ConnParams.UserName, P.ConnParams.Password, P.ConnParams.DSN = username, password, dsn
	P.PoolParams.UserName, P.PoolParams.Password, P.PoolParams.DSN = username, password, dsn

	q := u.Query()
	if vv, ok := q["connectionClass"]; ok {
		P.ConnClass = vv[0]
	}
	for _, task := range []struct {
		Dest *bool
		Key  string
	}{
		{&P.IsSysDBA, "sysdba"},
		{&P.IsSysOper, "sysoper"},
		{&P.IsSysASM, "sysasm"},
		{&P.IsPrelim, "prelim"},

		{&P.StandaloneConnection, "standaloneConnection"},
		{&P.ConnParams.EnableEvents, "enableEvents"},
		{&P.Heterogeneous, "heterogeneousPool"},
	} {
		*task.Dest = q.Get(task.Key) == "1"
	}
	P.PoolParams.EnableEvents = P.ConnParams.EnableEvents
	if tz := q.Get("timezone"); tz != "" {
		if strings.EqualFold(tz, "local") {
			// P.Timezone = time.Local // already set
		} else if strings.Contains(tz, "/") {
			if P.ConnParams.Timezone, err = time.LoadLocation(tz); err != nil {
				return P, errors.Errorf("%s: %w", tz, err)
			}
		} else if off, err := parseTZ(tz); err == nil {
			P.ConnParams.Timezone = time.FixedZone(tz, off)
		} else {
			return P, errors.Errorf("%s: %w", tz, err)
		}
		P.PoolParams.Timezone = P.ConnParams.Timezone
	}

	for _, task := range []struct {
		Dest *int
		Key  string
	}{
		{&P.MinSessions, "poolMinSessions"},
		{&P.MaxSessions, "poolMaxSessions"},
		{&P.SessionIncrement, "poolIncrement"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = strconv.Atoi(s)
		if err != nil {
			return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
		}
	}
	for _, task := range []struct {
		Dest *time.Duration
		Key  string
	}{
		{&P.SessionTimeout, "poolSessionTimeout"},
		{&P.WaitTimeout, "poolWaitTimeout"},
		{&P.MaxLifeTime, "poolSessionMaxLifetime"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = time.ParseDuration(s)
		if err != nil {
			if !strings.Contains(err.Error(), "time: missing unit in duration") {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			i, err := strconv.Atoi(s)
			if err != nil {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			base := time.Second
			if task.Key == "poolWaitTimeout" {
				base = time.Millisecond
			}
			*task.Dest = time.Duration(i) * base
		}
	}
	if P.MinSessions > P.MaxSessions {
		P.MinSessions = P.MaxSessions
	}
	if P.MinSessions == P.MaxSessions {
		P.SessionIncrement = 0
	} else if P.SessionIncrement < 1 {
		P.SessionIncrement = 1
	}
	P.ConnParams.OnInit = q["onInit"]

	P.Comb()
	if P.StandaloneConnection {
		P.NewPassword = q.Get("newPassword")
	}

	return P, nil
}

// SetSessionParamOnInit adds an "ALTER SESSION k=v" to the OnInit task list.
func (P *ConnectionParams) SetSessionParamOnInit(k, v string) {
	P.ConnParams.OnInit = append(P.ConnParams.OnInit, fmt.Sprintf("ALTER SESSION SET %s = q'(%s)'", k, strings.Replace(v, "'", "''", -1)))
}

func (P ConnectionParams) authMode() C.dpiAuthMode {
	authMode := C.dpiAuthMode(C.DPI_MODE_AUTH_DEFAULT)
	// OR all the modes together
	for _, elt := range []struct {
		Is   bool
		Mode C.dpiAuthMode
	}{
		{P.IsSysDBA, C.DPI_MODE_AUTH_SYSDBA},
		{P.IsSysOper, C.DPI_MODE_AUTH_SYSOPER},
		{P.IsSysASM, C.DPI_MODE_AUTH_SYSASM},
		{P.IsPrelim, C.DPI_MODE_AUTH_PRELIM},
	} {
		if elt.Is {
			authMode |= elt.Mode
		}
	}
	return authMode
}

// OraErr is an error holding the ORA-01234 code and the message.
type OraErr struct {
	message, funName, action, sqlState string
	code, offset                       int
	recoverable                        bool
}

// AsOraErr returns the underlying *OraErr and whether it succeeded.
func AsOraErr(err error) (*OraErr, bool) {
	var oerr *OraErr
	ok := errors.As(err, &oerr)
	return oerr, ok
}

var _ = error((*OraErr)(nil))

// Code returns the OraErr's error code.
func (oe *OraErr) Code() int { return oe.code }

// Message returns the OraErr's message.
func (oe *OraErr) Message() string { return oe.message }
func (oe *OraErr) Error() string {
	msg := oe.Message()
	if oe.code == 0 && msg == "" {
		return ""
	}
	return fmt.Sprintf("ORA-%05d: %s", oe.code, oe.message)
}
func fromErrorInfo(errInfo C.dpiErrorInfo) *OraErr {
	oe := OraErr{
		code:        int(errInfo.code),
		message:     strings.TrimSpace(C.GoStringN(errInfo.message, C.int(errInfo.messageLength))),
		offset:      int(errInfo.offset),
		funName:     strings.TrimSpace(C.GoString(errInfo.fnName)),
		action:      strings.TrimSpace(C.GoString(errInfo.action)),
		sqlState:    strings.TrimSpace(C.GoString(errInfo.sqlState)),
		recoverable: errInfo.isRecoverable != 0,
	}
	if oe.code == 0 && strings.HasPrefix(oe.message, "ORA-") &&
		len(oe.message) > 9 && oe.message[9] == ':' {
		if i, _ := strconv.Atoi(oe.message[4:9]); i > 0 {
			oe.code, oe.message = i, strings.TrimSpace(oe.message[10:])
		}
	}
	oe.message = strings.TrimPrefix(oe.message, fmt.Sprintf("ORA-%05d: ", oe.Code()))
	return &oe
}

// Offset returns the parse error offset (in bytes) when executing a statement or the row offset when performing bulk operations or fetching batch error information. If neither of these cases are true, the value is 0.
func (oe *OraErr) Offset() int { return oe.offset }

// FunName returns the public ODPI-C function name which was called in which the error took place. This is a null-terminated ASCII string.
func (oe *OraErr) FunName() string { return oe.funName }

// Action returns the internal action that was being performed when the error took place. This is a null-terminated ASCII string.
func (oe *OraErr) Action() string { return oe.action }

// SQLState returns the SQLSTATE code associated with the error. This is a 5 character null-terminated string.
func (oe *OraErr) SQLState() string { return oe.sqlState }

// Recoverable indicates if the error is recoverable. This is always false unless both client and server are at release 12.1 or higher.
func (oe *OraErr) Recoverable() bool { return oe.recoverable }

// newErrorInfo is just for testing: testing cannot use Cgo...
func newErrorInfo(code int, message string) C.dpiErrorInfo {
	return C.dpiErrorInfo{code: C.int32_t(code), message: C.CString(message), messageLength: C.uint(len(message))}
}

// against deadcode
var _ = newErrorInfo

func (d *drv) getError() *OraErr {
	if d == nil || d.dpiContext == nil {
		return &OraErr{code: -12153, message: driver.ErrBadConn.Error()}
	}
	var errInfo C.dpiErrorInfo
	C.dpiContext_getError(d.dpiContext, &errInfo)
	return fromErrorInfo(errInfo)
}

func b2i(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// VersionInfo holds version info returned by Oracle DB.
type VersionInfo struct {
	ServerRelease                                           string
	Version, Release, Update, PortRelease, PortUpdate, Full uint8
}

func (V *VersionInfo) set(v *C.dpiVersionInfo) {
	*V = VersionInfo{
		Version: uint8(v.versionNum),
		Release: uint8(v.releaseNum), Update: uint8(v.updateNum),
		PortRelease: uint8(v.portReleaseNum), PortUpdate: uint8(v.portUpdateNum),
		Full: uint8(v.fullVersionNum),
	}
}
func (V VersionInfo) String() string {
	var s string
	if V.ServerRelease != "" {
		s = " [" + V.ServerRelease + "]"
	}
	return fmt.Sprintf("%d.%d.%d.%d.%d%s", V.Version, V.Release, V.Update, V.PortRelease, V.PortUpdate, s)
}

var timezones = make(map[[2]C.int8_t]*time.Location)
var timezonesMu sync.RWMutex

func timeZoneFor(hourOffset, minuteOffset C.int8_t) *time.Location {
	if hourOffset == 0 && minuteOffset == 0 {
		return time.UTC
	}
	key := [2]C.int8_t{hourOffset, minuteOffset}
	timezonesMu.RLock()
	tz := timezones[key]
	timezonesMu.RUnlock()
	if tz == nil {
		timezonesMu.Lock()
		if tz = timezones[key]; tz == nil {
			tz = time.FixedZone(
				fmt.Sprintf("%02d:%02d", hourOffset, minuteOffset),
				int(hourOffset)*3600+int(minuteOffset)*60,
			)
			timezones[key] = tz
		}
		timezonesMu.Unlock()
	}
	return tz
}

type ctxKey string

const logCtxKey = ctxKey("godror.Log")

type logFunc func(...interface{}) error

func ctxGetLog(ctx context.Context) logFunc {
	if lgr, ok := ctx.Value(logCtxKey).(func(...interface{}) error); ok {
		return lgr
	}
	return Log
}

// ContextWithLog returns a context with the given log function.
func ContextWithLog(ctx context.Context, logF func(...interface{}) error) context.Context {
	return context.WithValue(ctx, logCtxKey, logF)
}

var _ = driver.DriverContext((*drv)(nil))
var _ = driver.Connector((*connector)(nil))

type connector struct {
	drv *drv
	PoolParams
	ConnParams
}

type PoolParams struct {
	UserName, Password, DSN                    string
	MinSessions, MaxSessions, SessionIncrement int
	WaitTimeout, MaxLifeTime, SessionTimeout   time.Duration
	Heterogeneous, ExternalAuth, EnableEvents  bool
	OnInit                                     func(driver.Conn) error
	Timezone                                   *time.Location
}

func (pp PoolParams) IsZero() bool {
	return pp.UserName == "" && pp.Password == "" && pp.DSN == "" &&
		pp.MinSessions == 0 && pp.MaxSessions == 0 && pp.SessionIncrement == 0 &&
		pp.WaitTimeout == 0 && pp.MaxLifeTime == 0 && pp.SessionTimeout == 0 &&
		!pp.Heterogeneous && !pp.ExternalAuth && !pp.EnableEvents &&
		pp.OnInit == nil && pp.Timezone == nil
}

type ConnParams struct {
	UserName, Password, NewPassword, ConnClass, DSN       string
	EnableEvents, IsSysDBA, IsSysOper, IsSysASM, IsPrelim bool
	ShardingKey, SuperShardingKey                         []interface{}
	OnInit                                                []string
	Timezone                                              *time.Location
}

func NewConnector2(poolParams PoolParams, connParams ConnParams) driver.Connector {
	return connector{PoolParams: poolParams, ConnParams: connParams,
		drv: defaultDrv}
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *drv) OpenConnector(name string) (driver.Connector, error) {

	// parse connection string
	P, err := ParseConnString(name)
	if err != nil {
		return nil, err
	}

	// create connector and assign parameters
	c := connector{drv: d, ConnParams: P.ConnParams}
	if !(P.IsSysDBA || P.IsSysOper || P.IsSysASM || P.IsPrelim || P.StandaloneConnection) {
		c.PoolParams = P.PoolParams

		// only enable external authentication if we are dealing with a
		// homogeneous pool and no user name/password has been specified
		if P.PoolParams.UserName == "" && P.PoolParams.Password == "" && !P.Heterogeneous {
			c.PoolParams.ExternalAuth = true
		}
	}
	return c, nil
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes.
//
// The returned connection is only used by one goroutine at a
// time.
func (c connector) Connect(ctx context.Context) (driver.Conn, error) {

	if ctxValue := ctx.Value(paramsCtxKey); ctxValue != nil {
		params, ok := ctxValue.(ConnParams)
		if ok {
			return c.drv.createConnFromParams(c.PoolParams, params)
		}
	}

	return c.drv.createConnFromParams(c.PoolParams, c.ConnParams)
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c connector) Driver() driver.Driver { return c.drv }

// NewConnector returns a driver.Connector to be used with sql.OpenDB,
// which calls the given onInit if the connection is new.
//
// For an onInit example, see NewSessionIniter.
func (d *drv) NewConnector(name string, onInit func(driver.Conn) error) (driver.Connector, error) {
	cxr, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	cx := cxr.(connector)
	cx.PoolParams.OnInit = onInit
	return cx, err
}

// NewConnector returns a driver.Connector to be used with sql.OpenDB,
// (for the default Driver registered with godror)
// which calls the given onInit if the connection is new.
//
// For an onInit example, see NewSessionIniter.
func NewConnector(name string, onInit func(driver.Conn) error) (driver.Connector, error) {
	return defaultDrv.NewConnector(name, onInit)
}

// NewSessionIniter returns a function suitable for use in NewConnector as onInit,
// which calls "ALTER SESSION SET <key>='<value>'" for each element of the given map.
func NewSessionIniter(m map[string]string) func(driver.Conn) error {
	return func(cx driver.Conn) error {
		for k, v := range m {
			qry := fmt.Sprintf("ALTER SESSION SET %s = q'(%s)'", k, strings.Replace(v, "'", "''", -1))
			st, err := cx.Prepare(qry)
			if err != nil {
				return errors.Errorf("%s: %w", qry, err)
			}
			_, err = st.Exec(nil) //lint:ignore SA1019 it's hard to use ExecContext here
			st.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}
}
