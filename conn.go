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
#include <dpi.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"unsafe"

	"github.com/pkg/errors"
)

var _ = driver.Conn((*conn)(nil))
var _ = driver.ConnBeginTx((*conn)(nil))
var _ = driver.ConnPrepareContext((*conn)(nil))
var _ = driver.Pinger((*conn)(nil))

type conn struct {
	dpiConn       *C.dpiConn
	connParams    connectionParams
	inTransaction bool
	*drv
}

func (c *conn) Break() error {
	if C.dpiConn_breakExecution(c.dpiConn) == C.DPI_FAILURE {
		return c.getError()
	}
	return nil
}

func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan struct{}, 1)
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			_ = c.Break()
		}
	}()
	ok := C.dpiConn_ping(c.dpiConn) == C.DPI_FAILURE
	done <- struct{}{}
	if !ok {
		return c.getError()
	}
	return nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
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
	if c == nil || c.dpiConn == nil {
		return nil
	}
	if C.dpiConn_release(c.dpiConn) == C.DPI_FAILURE {
		return c.getError()
	}
	c.dpiConn = nil
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
	if opts.ReadOnly {
		return nil, errors.New("read-only transaction property is not supported")
	}
	switch level := sql.IsolationLevel(opts.Isolation); level {
	case sql.LevelDefault, sql.LevelReadCommitted:
	default:
		return nil, errors.Errorf("%v isolation level is not supported", sql.IsolationLevel(opts.Isolation))
	}

	dc, err := c.drv.openConn(c.connParams)
	if err != nil {
		return nil, err
	}
	c2 := dc
	c2.inTransaction = true
	return c2, err
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cSQL := C.CString(query)
	defer func() {
		C.free(unsafe.Pointer(cSQL))
	}()
	var dpiStmt *C.dpiStmt
	if C.dpiConn_prepareStmt(c.dpiConn, 0, cSQL, C.uint32_t(len(query)), nil, 0,
		(**C.dpiStmt)(unsafe.Pointer(&dpiStmt)),
	) == C.DPI_FAILURE {
		return nil, c.getError()
	}
	//fmt.Printf("PrepareContext(%q):%p\n", query, dpiStmt)
	return &statement{conn: c, dpiStmt: dpiStmt}, nil
}
func (c *conn) Commit() error {
	c.inTransaction = false
	if C.dpiConn_commit(c.dpiConn) == C.DPI_FAILURE {
		return c.getError()
	}
	return nil
}
func (c *conn) Rollback() error {
	c.inTransaction = false
	if C.dpiConn_rollback(c.dpiConn) == C.DPI_FAILURE {
		return c.getError()
	}
	return nil
}

func (c *conn) newVar(isPlSQLArray bool, typ C.dpiOracleTypeNum, natTyp C.dpiNativeTypeNum, arraySize int, bufSize int) (*C.dpiVar, []C.dpiData, error) {
	if c == nil || c.dpiConn == nil {
		return nil, nil, errors.New("connection is nil")
	}
	isArray := C.int(0)
	if isPlSQLArray && arraySize > 1 {
		isArray = 1
	} else if arraySize < 0 {
		arraySize = 1
	}
	var dataArr *C.dpiData
	var v *C.dpiVar
	Log("C", "dpiConn_newVar", "conn", c.dpiConn, "typ", int(typ), "natTyp", int(natTyp), "arraySize", arraySize, "bufSize", bufSize, "isArray", isArray, "v", v)
	if C.dpiConn_newVar(
		c.dpiConn, typ, natTyp, C.uint32_t(arraySize),
		C.uint32_t(bufSize), 1,
		isArray, nil,
		&v, &dataArr,
	) == C.DPI_FAILURE {
		return nil, nil, errors.Wrapf(c.getError(), "newVar(typ=%d, natTyp=%d, arraySize=%d, bufSize=%d)", typ, natTyp, arraySize, bufSize)
	}
	// https://github.com/golang/go/wiki/cgo#Turning_C_arrays_into_Go_slices
	/*
		var theCArray *C.YourType = C.getTheArray()
		length := C.getTheArrayLength()
		slice := (*[1 << 30]C.YourType)(unsafe.Pointer(theCArray))[:length:length]
	*/
	data := ((*[maxArraySize]C.dpiData)(unsafe.Pointer(dataArr)))[:arraySize:arraySize]
	return v, data, nil
}

var _ = driver.Tx((*conn)(nil))
