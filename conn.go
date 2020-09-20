// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"
*/
import "C"

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/godror/godror/dsn"
)

const getConnection = "--GET_CONNECTION--"
const wrapResultset = "--WRAP_RESULTSET--"

// The maximum capacity is limited to (2^32 / sizeof(dpiData))-1 to remain compatible
// with 32-bit platforms. The size of a `C.dpiData` is 32 Byte on a 64-bit system, `C.dpiSubscrMessageTable` is 40 bytes.
const maxArraySize = (1<<30)/C.sizeof_dpiSubscrMessageTable - 1

var _ driver.Conn = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)

//
//var _ driver.ExecerContext = (*conn)(nil)
//var _ driver.QueryerContext = (*conn)(nil)
//var _ driver.NamedValueChecker = (*conn)(nil)

type conn struct {
	currentTT     TraceTag
	params        dsn.ConnectionParams
	Server        VersionInfo
	tranParams    tranParams
	mu            sync.RWMutex
	poolKey       string
	drv           *drv
	dpiConn       *C.dpiConn
	tzOffSecs     int
	inTransaction bool
	newSession    bool
	released      bool
	tzValid       bool
}

func (c *conn) getError() error {
	if c == nil || c.drv == nil {
		return driver.ErrBadConn
	}
	return c.drv.getError()
}

// Break signals the server to stop the execution on the connection.
//
// The execution should fail with ORA-1013: "user requested cancel of current operation".
// You then need to wait for the originally executing call to to complete with the error before proceeding.
//
// So, after the Break, the connection MUST NOT be used till the executing thread finishes!
func (c *conn) Break() error {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if Log != nil {
		Log("msg", "Break", "dpiConn", c.dpiConn)
	}
	if c.dpiConn == nil {
		return nil
	}
	if C.dpiConn_breakExecution(c.dpiConn) == C.DPI_FAILURE {
		return maybeBadConn(fmt.Errorf("Break: %w", c.getError()), c)
	}
	return nil
}

func (c *conn) ClientVersion() (VersionInfo, error) { return c.drv.ClientVersion() }

// Ping checks the connection's state.
//
// WARNING: as database/sql calls database/sql/driver.Open when it needs
// a new connection, but does not provide this Context,
// if the Open stalls (unreachable / firewalled host), the
// database/sql.Ping may return way after the Context.Deadline!
func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
			// select again to avoid race condition if both are done
			select {
			case <-done:
				return
			default:
				_ = c.Break()
				return
			}
		}
	}()

	dl, hasDeadline := ctx.Deadline()
	if hasDeadline {
		c.setCallTimeout(time.Until(dl))
	}
	failure := C.dpiConn_ping(c.dpiConn) == C.DPI_FAILURE
	close(done)
	if hasDeadline {
		c.setCallTimeout(0)
	}
	if failure {
		return maybeBadConn(fmt.Errorf("Ping: %w", c.getError()), c)
	}
	return nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (c *conn) CheckNamedValueX(nv *driver.NamedValue) error {
	return driver.ErrSkip
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *conn) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeNotLocking()
}

func (c *conn) closeNotLocking() error {
	if c == nil {
		return nil
	}
	c.currentTT = TraceTag{}
	dpiConn := c.dpiConn
	if dpiConn == nil {
		return nil
	}
	c.dpiConn = nil
	if dpiConn.refCount <= 1 {
		c.tzOffSecs, c.tzValid, c.params.Timezone = 0, false, nil
	}

	// dpiConn_release decrements dpiConn's reference counting,
	// and closes it when it reaches zero.
	//
	// To track reference counting, use DPI_DEBUG_LEVEL=2
	C.dpiConn_release(dpiConn)
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	const (
		trRO = "READ ONLY"
		trRW = "READ WRITE"
		trLC = "ISOLATION LEVEL READ COMMIT" + "TED" // against misspell check
		trLS = "ISOLATION LEVEL SERIALIZABLE"
	)

	var todo tranParams
	if opts.ReadOnly {
		todo.RW = trRO
	} else {
		todo.RW = trRW
	}
	switch level := sql.IsolationLevel(opts.Isolation); level {
	case sql.LevelDefault:
	case sql.LevelReadCommitted:
		todo.Level = trLC
	case sql.LevelSerializable:
		todo.Level = trLS
	default:
		return nil, fmt.Errorf("isolation level is not supported: %s", sql.IsolationLevel(opts.Isolation))
	}

	if todo != c.tranParams {
		for _, qry := range []string{todo.RW, todo.Level} {
			if qry == "" {
				continue
			}
			qry = "SET TRANSACTION " + qry
			stmt, err := c.PrepareContext(ctx, qry)
			if err == nil {
				if stc, ok := stmt.(driver.StmtExecContext); ok {
					_, err = stc.ExecContext(ctx, nil)
				} else {
					_, err = stmt.Exec(nil) //lint:ignore SA1019 as that comment is not relevant here
				}
				stmt.Close()
			}
			if err != nil {
				return nil, maybeBadConn(fmt.Errorf("%s: %w", qry, err), c)
			}
		}
		c.tranParams = todo
	}

	c.mu.RLock()
	inTran := c.inTransaction
	c.mu.RUnlock()
	if inTran {
		return nil, errors.New("already in transaction")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inTransaction = true
	if tt, ok := ctx.Value(traceTagCtxKey).(TraceTag); ok {
		c.setTraceTag(tt)
	}
	return c, nil
}

type tranParams struct {
	RW, Level string
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if tt, ok := ctx.Value(traceTagCtxKey).(TraceTag); ok {
		c.mu.Lock()
		c.setTraceTag(tt)
		c.mu.Unlock()
	}
	// TODO: get rid of this hack
	if query == getConnection {
		if Log != nil {
			Log("msg", "PrepareContext", "shortcut", query)
		}
		return &statement{conn: c, query: query}, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.prepareContextNotLocked(ctx, query)
}
func (c *conn) prepareContextNotLocked(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cSQL := C.CString(query)
	defer func() {
		C.free(unsafe.Pointer(cSQL))
	}()
	st := statement{conn: c, query: query}
	if C.dpiConn_prepareStmt(c.dpiConn, 0, cSQL, C.uint32_t(len(query)), nil, 0,
		(**C.dpiStmt)(unsafe.Pointer(&st.dpiStmt)),
	) == C.DPI_FAILURE {
		return nil, maybeBadConn(fmt.Errorf("Prepare: %s: %w", query, c.getError()), c)
	}
	if C.dpiStmt_getInfo(st.dpiStmt, &st.dpiStmtInfo) == C.DPI_FAILURE {
		err := maybeBadConn(fmt.Errorf("getStmtInfo: %w", c.getError()), c)
		st.Close()
		return nil, err
	}
	stmtSetFinalizer(&st, "prepareContext")
	return &st, nil
}
func (c *conn) Commit() error {
	return c.endTran(true)
}
func (c *conn) Rollback() error {
	return c.endTran(false)
}
func (c *conn) endTran(isCommit bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inTransaction = false
	c.tranParams = tranParams{}

	var err error
	//msg := "Commit"
	if isCommit {
		if C.dpiConn_commit(c.dpiConn) == C.DPI_FAILURE {
			err = maybeBadConn(fmt.Errorf("Commit: %w", c.getError()), c)
		}
	} else {
		//msg = "Rollback"
		if C.dpiConn_rollback(c.dpiConn) == C.DPI_FAILURE {
			err = maybeBadConn(fmt.Errorf("Rollback: %w", c.getError()), c)
		}
	}
	//fmt.Printf("%p.%s\n", c, msg)
	return err
}

type varInfo struct {
	SliceLen, BufSize int
	ObjectType        *C.dpiObjectType
	NatTyp            C.dpiNativeTypeNum
	Typ               C.dpiOracleTypeNum
	IsPLSArray        bool
}

func (c *conn) newVar(vi varInfo) (*C.dpiVar, []C.dpiData, error) {
	if c == nil || c.dpiConn == nil {
		return nil, nil, errors.New("connection is nil")
	}
	isArray := C.int(0)
	if vi.IsPLSArray {
		isArray = 1
	}
	if vi.SliceLen < 1 {
		vi.SliceLen = 1
	}
	var dataArr *C.dpiData
	var v *C.dpiVar
	if Log != nil {
		Log("C", "dpiConn_newVar", "conn", c.dpiConn, "typ", int(vi.Typ), "natTyp", int(vi.NatTyp), "sliceLen", vi.SliceLen, "bufSize", vi.BufSize, "isArray", isArray, "objType", vi.ObjectType, "v", v)
	}
	if C.dpiConn_newVar(
		c.dpiConn, vi.Typ, vi.NatTyp, C.uint32_t(vi.SliceLen),
		C.uint32_t(vi.BufSize), 1,
		isArray, vi.ObjectType,
		&v, &dataArr,
	) == C.DPI_FAILURE {
		return nil, nil, fmt.Errorf("newVar(typ=%d, natTyp=%d, sliceLen=%d, bufSize=%d): %w", vi.Typ, vi.NatTyp, vi.SliceLen, vi.BufSize, c.getError())
	}
	// https://github.com/golang/go/wiki/cgo#Turning_C_arrays_into_Go_slices
	/*
		var theCArray *C.YourType = C.getTheArray()
		length := C.getTheArrayLength()
		slice := (*[maxArraySize]C.YourType)(unsafe.Pointer(theCArray))[:length:length]
	*/
	data := ((*[maxArraySize]C.dpiData)(unsafe.Pointer(dataArr)))[:vi.SliceLen:vi.SliceLen]
	return v, data, nil
}

var _ = driver.Tx((*conn)(nil))

func (c *conn) ServerVersion() (VersionInfo, error) {
	if c.Server.Version != 0 {
		return c.Server, nil
	}
	var v C.dpiVersionInfo
	var release *C.char
	var releaseLen C.uint32_t
	if C.dpiConn_getServerVersion(c.dpiConn, &release, &releaseLen, &v) == C.DPI_FAILURE {
		if c.params.IsPrelim {
			return c.Server, nil
		}
		return c.Server, fmt.Errorf("getServerVersion: %w", c.getError())
	}
	c.Server.set(&v)
	c.Server.ServerRelease = string(bytes.Replace(
		((*[maxArraySize]byte)(unsafe.Pointer(release)))[:releaseLen:releaseLen],
		[]byte{'\n'}, []byte{';', ' '}, -1))

	return c.Server, nil
}

func (c *conn) init(onInit func(conn driver.Conn) error) error {
	c.released = false
	if Log != nil {
		Log("msg", "init connection", "conn", c, "onInit", onInit)
	}

	if err := c.initTZ(); err != nil || onInit == nil || !c.newSession {
		return err
	}
	return onInit(c)
}

func (c *conn) initTZ() error {
	if c.tzValid {
		return nil
	}
	c.params.Timezone = time.Local

	key := time.Local.String() + "\t" + c.params.String()
	c.drv.mu.RLock()
	tz, ok := c.drv.timezones[key]
	c.drv.mu.RUnlock()
	if !ok {
		c.drv.mu.Lock()
		defer c.drv.mu.Unlock()
		tz, ok = c.drv.timezones[key]
	}
	if ok {
		c.params.Timezone, c.tzOffSecs = tz.Location, tz.offSecs
		return nil
	}
	if Log != nil {
		Log("msg", "initTZ", "key", key)
	}
	//fmt.Printf("initTZ BEG key=%q drv=%p timezones=%v\n", key, c.drv, c.drv.timezones)
	// DBTIMEZONE is useless, false, and misdirecting!
	// https://stackoverflow.com/questions/52531137/sysdate-and-dbtimezone-different-in-oracle-database
	const qry = "SELECT DBTIMEZONE, LTRIM(REGEXP_SUBSTR(TO_CHAR(SYSTIMESTAMP), ' [^ ]+$')) FROM DUAL"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st, err := c.prepareContextNotLocked(ctx, qry)
	if err != nil {
		//fmt.Printf("initTZ END key=%q drv=%p timezones=%v err=%v\n", key, c.drv, c.drv.timezones, err)
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer st.Close()
	rows, err := st.(*statement).queryContextNotLocked(ctx, nil)
	if err != nil {
		if Log != nil {
			Log("qry", qry, "error", err)
		}
		//fmt.Printf("initTZ END key=%q drv=%p timezones=%v err=%v\n", key, c.drv, c.drv.timezones, err)
		return err
	}
	defer rows.Close()
	var dbTZ, timezone string
	vals := []driver.Value{dbTZ, timezone}
	if err = rows.Next(vals); err != nil && err != io.EOF {
		//fmt.Printf("initTZ END key=%q drv=%p timezones=%v err=%v\n", key, c.drv, c.drv.timezones, err)
		return fmt.Errorf("%s: %w", qry, err)
	}
	dbTZ = vals[0].(string)
	timezone = vals[1].(string)

	tz.Location, tz.offSecs, err = calculateTZ(dbTZ, timezone)
	//fmt.Printf("calculateTZ(%q, %q): %p=%v, %v, %v\n", dbTZ, timezone, tz.Location, tz.Location, tz.offSecs, err)
	if Log != nil {
		Log("timezone", timezone, "tz", tz, "error", err)
	}
	if err == nil && tz.Location == nil {
		if tz.offSecs != 0 {
			err = fmt.Errorf("nil timezone from %q,%q", dbTZ, timezone)
		} else {
			tz.Location = time.UTC
		}
	}
	if err != nil {
		if Log != nil {
			Log("msg", "initTZ", "error", err)
		}
		//fmt.Printf("initTZ END key=%q drv=%p timezones=%v err=%v\n", key, c.drv, c.drv.timezones, err)
		panic(err)
	}

	c.params.Timezone, c.tzOffSecs, c.tzValid = tz.Location, tz.offSecs, tz.Location != nil
	if Log != nil {
		Log("tz", c.params.Timezone, "offSecs", c.tzOffSecs)
	}

	if c.tzValid {
		c.drv.timezones[key] = tz
	}
	//fmt.Printf("initTZ END key=%q drv=%p timezones=%v err=%v\n", key, c.drv, c.drv.timezones, err)
	return nil
}

func calculateTZ(dbTZ, timezone string) (*time.Location, int, error) {
	if dbTZ == "+00:00" && timezone == "+00:00" {
		return time.UTC, 0, nil
	}
	if Log != nil {
		Log("dbTZ", dbTZ, "timezone", timezone)
	}
	var tz *time.Location
	var off int
	now := time.Now()
	_, localOff := now.Local().Zone()
	// If it's a name, try to use it.
	if dbTZ != "" && strings.Contains(dbTZ, "/") {
		var err error
		if tz, err = time.LoadLocation(dbTZ); err == nil {
			_, off = now.In(tz).Zone()
			return tz, off, nil
		}
		if Log != nil {
			Log("LoadLocation", dbTZ, "error", err)
		}
	}
	// If not, use the numbers.
	var err error
	if timezone != "" {
		if off, err = dsn.ParseTZ(timezone); err != nil {
			return tz, off, fmt.Errorf("%s: %w", timezone, err)
		}
	} else if off, err = dsn.ParseTZ(dbTZ); err != nil {
		return tz, off, fmt.Errorf("%s: %w", dbTZ, err)
	}
	// This is dangerous, but I just cannot get whether the DB time zone
	// setting has DST or not - DBTIMEZONE returns just a fixed offset.
	//
	// So if the given offset is the same as with the Local time zone,
	// then keep the local.
	//fmt.Printf("off=%d localOff=%d tz=%p\n", off, localOff, tz)
	if off == localOff {
		return time.Local, off, nil
	}
	if off == 0 {
		tz = time.UTC
	} else {
		tz = time.FixedZone(timezone, off)
	}
	return tz, off, nil
}
func (c *conn) setCallTimeout(dur time.Duration) {
	if c.drv.clientVersion.Version < 18 {
		return
	}
	ms := C.uint32_t(dur / time.Millisecond)
	if Log != nil {
		Log("msg", "setCallTimeout", "ms", ms)
	}
	C.dpiConn_setCallTimeout(c.dpiConn, ms)
}

// maybeBadConn checks whether the error is because of a bad connection,
// CLOSES the connection and returns driver.ErrBadConn,
// as database/sql requires.
func maybeBadConn(err error, c *conn) error {
	if err == nil {
		return nil
	}
	cl := func() {
		if Log != nil {
			Log("msg", "maybeBadConn", "error", err)
		}
	}
	if c != nil {
		cl = func() {
			if Log != nil {
				Log("msg", "maybeBadConn close", "conn", c, "error", err)
			}
			c.closeNotLocking()
		}
	}
	if errors.Is(err, driver.ErrBadConn) {
		cl()
		return driver.ErrBadConn
	}
	var cd interface{ Code() int }
	if errors.As(err, &cd) {
		// Yes, this is copied from rana/ora, but I've put it there, so it's mine. @tgulacsi
		switch cd.Code() {
		case 0:
			if strings.Contains(err.Error(), " DPI-1002: ") {
				cl()
				return driver.ErrBadConn
			}
			// cases by experience:
			// ORA-12170: TNS:Connect timeout occurred
			// ORA-12528: TNS:listener: all appropriate instances are blocking new connections
			// ORA-12545: Connect failed because target host or object does not exist
		case 12170, 12528, 12545:
			fallthrough
			//cases from https://github.com/oracle/odpi/blob/master/src/dpiError.c#L61-L94
		case 22, // invalid session ID; access denied
			28,    // your session has been killed
			31,    // your session has been marked for kill
			45,    // your session has been terminated with no replay
			378,   // buffer pools cannot be created as specified
			602,   // internal programming exception
			603,   // ORACLE server session terminated by fatal error
			609,   // could not attach to incoming connection
			1012,  // not logged on
			1041,  // internal error. hostdef extension doesn't exist
			1043,  // user side memory corruption
			1089,  // immediate shutdown or close in progress
			1092,  // ORACLE instance terminated. Disconnection forced
			2396,  // exceeded maximum idle time, please connect again
			3113,  // end-of-file on communication channel
			3114,  // not connected to ORACLE
			3122,  // attempt to close ORACLE-side window on user side
			3135,  // connection lost contact
			3136,  // inbound connection timed out
			12153, // TNS:not connected
			12537, // TNS:connection closed
			12547, // TNS:lost contact
			12570, // TNS:packet reader failure
			12583, // TNS:no reader
			27146, // post/wait initialization failed
			28511, // lost RPC connection
			28547, // connection to server failed, probable Oracle Net admin error
			56600: // an illegal OCI function call was issued
			cl()
			return driver.ErrBadConn
		}
	}
	return err
}

func (c *conn) setTraceTag(tt TraceTag) error {
	if c == nil || c.dpiConn == nil {
		return nil
	}
	for nm, vv := range map[string][2]string{
		"action":     {c.currentTT.Action, tt.Action},
		"module":     {c.currentTT.Module, tt.Module},
		"info":       {c.currentTT.ClientInfo, tt.ClientInfo},
		"identifier": {c.currentTT.ClientIdentifier, tt.ClientIdentifier},
		"op":         {c.currentTT.DbOp, tt.DbOp},
	} {
		if vv[0] == vv[1] {
			continue
		}
		v := vv[1]
		var s *C.char
		if v != "" {
			s = C.CString(v)
		}
		var rc C.int
		switch nm {
		case "action":
			rc = C.dpiConn_setAction(c.dpiConn, s, C.uint32_t(len(v)))
		case "module":
			rc = C.dpiConn_setModule(c.dpiConn, s, C.uint32_t(len(v)))
		case "info":
			rc = C.dpiConn_setClientInfo(c.dpiConn, s, C.uint32_t(len(v)))
		case "identifier":
			rc = C.dpiConn_setClientIdentifier(c.dpiConn, s, C.uint32_t(len(v)))
		case "op":
			rc = C.dpiConn_setDbOp(c.dpiConn, s, C.uint32_t(len(v)))
		}
		if s != nil {
			C.free(unsafe.Pointer(s))
		}
		if rc == C.DPI_FAILURE {
			return fmt.Errorf("%s: %w", nm, c.getError())
		}
	}
	c.currentTT = tt
	return nil
}
func (c *conn) GetPoolStats() (stats PoolStats, err error) {
	if c == nil {
		return stats, nil
	}
	c.mu.RLock()
	key, drv := c.poolKey, c.drv
	c.mu.RUnlock()
	if key == "" {
		// not pooled connection
		return stats, nil
	}

	drv.mu.RLock()
	pool := drv.pools[key]
	drv.mu.RUnlock()
	if pool == nil {
		return stats, nil
	}
	return drv.getPoolStats(pool)
}

const traceTagCtxKey = ctxKey("tracetag")

// ContextWithTraceTag returns a context with the specified TraceTag, which will
// be set on the session used.
func ContextWithTraceTag(ctx context.Context, tt TraceTag) context.Context {
	return context.WithValue(ctx, traceTagCtxKey, tt)
}

// TraceTag holds tracing information for the session. It can be set on the session
// with ContextWithTraceTag.
type TraceTag struct {
	// ClientIdentifier - specifies an end user based on the logon ID, such as HR.HR
	ClientIdentifier string
	// ClientInfo - client-specific info
	ClientInfo string
	// DbOp - database operation
	DbOp string
	// Module - specifies a functional block, such as Accounts Receivable or General Ledger, of an application
	Module string
	// Action - specifies an action, such as an INSERT or UPDATE operation, in a module
	Action string
}

func (tt TraceTag) String() string {
	q := make(url.Values, 5)
	if tt.ClientIdentifier != "" {
		q.Add("clientIdentifier", tt.ClientIdentifier)
	}
	if tt.ClientInfo != "" {
		q.Add("clientInfo", tt.ClientInfo)
	}
	if tt.DbOp != "" {
		q.Add("dbOp", tt.DbOp)
	}
	if tt.Module != "" {
		q.Add("module", tt.Module)
	}
	if tt.Action != "" {
		q.Add("action", tt.Action)
	}
	return q.Encode()
}

const paramsCtxKey = ctxKey("params")

// ContextWithParams returns a context with the specified parameters. These parameters are used
// to modify the session acquired from the pool.
//
// If a standalone connection is being used this will have no effect.
//
// Also, you should disable the Go connection pool with DB.SetMaxIdleConns(0).
func ContextWithParams(ctx context.Context, commonParams dsn.CommonParams, connParams dsn.ConnParams) context.Context {
	return context.WithValue(ctx, paramsCtxKey,
		commonAndConnParams{CommonParams: commonParams, ConnParams: connParams})
}

// ContextWithUserPassw returns a context with the specified user and password,
// to be used with heterogeneous pools.
//
// If a standalone connection is being used this will have no effect.
//
// Also, you should disable the Go connection pool with DB.SetMaxIdleConns(0).
func ContextWithUserPassw(ctx context.Context, user, password, connClass string) context.Context {
	return ContextWithParams(ctx,
		dsn.CommonParams{Username: user, Password: dsn.NewPassword(password)},
		dsn.ConnParams{ConnClass: connClass},
	)
}

// StartupMode for the database.
type StartupMode C.dpiStartupMode

const (
	// StartupDefault is the default mode for startup which permits database access to all users.
	StartupDefault = StartupMode(C.DPI_MODE_STARTUP_DEFAULT)
	// StartupForce shuts down a running instance (using ABORT) before starting a new one. This mode should only be used in unusual circumstances.
	StartupForce = StartupMode(C.DPI_MODE_STARTUP_FORCE)
	// StartupRestrict only allows database access to users with both the CREATE SESSION and RESTRICTED SESSION privileges (normally the DBA).
	StartupRestrict = StartupMode(C.DPI_MODE_STARTUP_RESTRICT)
)

// Startup the database, equivalent to "startup nomount" in SQL*Plus.
// This should be called on PRELIM_AUTH (prelim=1) connection!
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *conn) Startup(mode StartupMode) error {
	if C.dpiConn_startupDatabase(c.dpiConn, C.dpiStartupMode(mode)) == C.DPI_FAILURE {
		return fmt.Errorf("startup(%v): %w", mode, c.getError())
	}
	return nil
}

// ShutdownMode for the database.
type ShutdownMode C.dpiShutdownMode

const (
	// ShutdownDefault - further connections to the database are prohibited. Wait for users to disconnect from the database.
	ShutdownDefault = ShutdownMode(C.DPI_MODE_SHUTDOWN_DEFAULT)
	// ShutdownTransactional - further connections to the database are prohibited and no new transactions are allowed to be started. Wait for active transactions to complete.
	ShutdownTransactional = ShutdownMode(C.DPI_MODE_SHUTDOWN_TRANSACTIONAL)
	// ShutdownTransactionalLocal - behaves the same way as ShutdownTransactional but only waits for local transactions to complete.
	ShutdownTransactionalLocal = ShutdownMode(C.DPI_MODE_SHUTDOWN_TRANSACTIONAL_LOCAL)
	// ShutdownImmediate - all uncommitted transactions are terminated and rolled back and all connections to the database are closed immediately.
	ShutdownImmediate = ShutdownMode(C.DPI_MODE_SHUTDOWN_IMMEDIATE)
	// ShutdownAbort - all uncommitted transactions are terminated and are not rolled back. This is the fastest way to shut down the database but the next database startup may require instance recovery.
	ShutdownAbort = ShutdownMode(C.DPI_MODE_SHUTDOWN_ABORT)
	// ShutdownFinal shuts down the database. This mode should only be used in the second call to dpiConn_shutdownDatabase().
	ShutdownFinal = ShutdownMode(C.DPI_MODE_SHUTDOWN_FINAL)
)

// Shutdown shuts down the database.
// Note that this must be done in two phases except in the situation where the instance is aborted.
//
// See https://docs.oracle.com/en/database/oracle/oracle-database/18/lnoci/database-startup-and-shutdown.html#GUID-44B24F65-8C24-4DF3-8FBF-B896A4D6F3F3
func (c *conn) Shutdown(mode ShutdownMode) error {
	if C.dpiConn_shutdownDatabase(c.dpiConn, C.dpiShutdownMode(mode)) == C.DPI_FAILURE {
		return fmt.Errorf("shutdown(%v): %w", mode, c.getError())
	}
	return nil
}

// Timezone returns the connection's timezone.
func (c *conn) Timezone() *time.Location { return c.params.Timezone }

var _ = driver.SessionResetter((*conn)(nil))

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before. If the driver returns driver.ErrBadConn
// the connection is discarded.
//
// This implementation does nothing if the connection is not pooled,
// but reacquires a new session if it is pooled.
//
// This ensures that the session is not stale.
func (c *conn) ResetSession(ctx context.Context) error {
	if c == nil {
		return driver.ErrBadConn
	}
	c.mu.RLock()
	key, drv, params, dpiConnOK := c.poolKey, c.drv, c.params, c.dpiConn != nil
	c.mu.RUnlock()
	if key == "" {
		// not pooled connection
		if !dpiConnOK {
			return driver.ErrBadConn
		}
		return nil
	}
	// FIXME(tgulacsi): Prepared statements hold the previous session,
	// so sometimes sessions are not released, resulting in
	//
	//     ORA-24459: OCISessionGet()
	//
	// See https://github.com/godror/godror/issues/57 for example.

	drv.mu.RLock()
	pool := drv.pools[key]
	drv.mu.RUnlock()
	if pool == nil {
		if !dpiConnOK {
			return driver.ErrBadConn
		}
		return nil
	}
	P := commonAndConnParams{CommonParams: params.CommonParams, ConnParams: params.ConnParams}
	var paramsFromCtx bool
	if ctxValue := ctx.Value(paramsCtxKey); ctxValue != nil {
		if P, paramsFromCtx = ctxValue.(commonAndConnParams); paramsFromCtx {
			// ContextWithUserPassw does not fill ConnParam.ConnectString
			if P.ConnectString == "" {
				P.ConnectString = params.ConnectString
			}
			if Log != nil {
				Log("msg", "paramsFromContext", "params", P)
			}
		}
	}
	if Log != nil {
		Log("msg", "ResetSession re-acquire session", "pool", pool.key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Close and then reacquire a fresh dpiConn
	if c.dpiConn != nil {
		// Just release
		c.closeNotLocking()
	}
	var err error
	var newSession bool
	if c.dpiConn, newSession, err = c.drv.acquireConn(pool, P); err != nil {
		return fmt.Errorf("%v: %w", err, driver.ErrBadConn)
	}

	if paramsFromCtx || newSession || !c.tzValid || c.params.Timezone == nil {
		c.init(P.OnInit)
	}
	return nil
}

// Validator may be implemented by Conn to allow drivers to
// signal if a connection is valid or if it should be discarded.
//
// If implemented, drivers may return the underlying error from queries,
// even if the connection should be discarded by the connection pool.
//
// This implementation returns the underlying session to the OCI session pool,
// iff this is a pooled connection. ResetSession will reacquire it.
func (c *conn) IsValid() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	dpiConnOK, released, pooled, tzOK := c.dpiConn != nil, c.released, c.poolKey != "", c.params.Timezone != nil
	c.mu.RUnlock()
	if Log != nil {
		Log("msg", "IsValid", "connOK", dpiConnOK, "released", released, "pooled", pooled, "tzOK", tzOK)
	}
	if !dpiConnOK || !tzOK {
		return released
	}
	if !pooled {
		// not pooled connection
		return dpiConnOK
	}

	// FIXME(tgulacsi): Prepared statements hold the previous session,
	// so sometimes sessions are not released, resulting in
	//
	//     ORA-24459: OCISessionGet()
	//
	// See https://github.com/godror/godror/issues/57 for example.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeNotLocking()
	c.released = true
	return true
}

func (c *conn) String() string {
	return fmt.Sprintf("%s&%s&serverVersion=%s&tzOffSecs=%d&new=%t",
		c.currentTT, c.params, c.Server, c.tzOffSecs, c.newSession)
}
