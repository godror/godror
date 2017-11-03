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

const int sizeof_dpiData = sizeof(void);
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// Option for NamedArgs
type Option uint8

// PlSQLArrays is to signal that the slices given in arguments of Exec to
// be left as is - the default is to treat them as arguments for ExecMany.
const PlSQLArrays = Option(1)

const minChunkSize = 1 << 16

var _ = driver.Stmt((*statement)(nil))
var _ = driver.StmtQueryContext((*statement)(nil))
var _ = driver.StmtExecContext((*statement)(nil))
var _ = driver.NamedValueChecker((*statement)(nil))

const sizeofDpiData = C.sizeof_dpiData

type statement struct {
	sync.Mutex
	*conn
	dpiStmt     *C.dpiStmt
	query       string
	data        [][]C.dpiData
	vars        []*C.dpiVar
	gets        []dataGetter
	dests       []interface{}
	isSlice     []bool
	PlSQLArrays bool
	arrLen      int
	columns     []Column
}

type dataGetter func(v interface{}, data *C.dpiData) error

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (st *statement) Close() error {
	if st == nil {
		return nil
	}
	st.Lock()
	defer st.Unlock()
	for _, v := range st.vars {
		C.dpiVar_release(v)
	}
	if st.dpiStmt != nil && C.dpiStmt_release(st.dpiStmt) == C.DPI_FAILURE {
		return errors.Wrap(st.getError(), "dpiStmt_release")
	}
	st.data = nil
	st.vars = nil
	st.gets = nil
	st.dests = nil
	st.columns = nil
	st.dpiStmt = nil
	return nil
}

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

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (st *statement) Exec(args []driver.Value) (driver.Result, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i].Ordinal = i + 1
		nargs[i].Value = arg
	}
	return st.ExecContext(context.Background(), nargs)
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (st *statement) Query(args []driver.Value) (driver.Rows, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i].Ordinal = i + 1
		nargs[i].Value = arg
	}
	return st.QueryContext(context.Background(), nargs)
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (st *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	Log := ctxGetLog(ctx)

	st.Lock()
	defer st.Unlock()
	st.conn.Lock()
	defer st.conn.Unlock()

	if st.dpiStmt == nil && st.query == getConnection {
		*(args[0].Value.(sql.Out).Dest.(*interface{})) = st.conn
		return driver.ResultNoRows, nil
	}

	// bind variables
	if err := st.bindVars(args, Log); err != nil {
		return nil, err
	}

	// execute
	ctxErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(ctxErr)
		select {
		case <-done:
			return
		case <-ctx.Done():
			ctxErr <- ctx.Err()
			_ = st.Break()
		}
	}()

	mode := C.dpiExecMode(C.DPI_MODE_EXEC_DEFAULT)
	//fmt.Printf("%p.%p: inTran? %t\n%s\n", st.conn, st, st.inTransaction, st.query)
	if !st.inTransaction {
		mode |= C.DPI_MODE_EXEC_COMMIT_ON_SUCCESS
	}
	var err error
	if !st.PlSQLArrays && st.arrLen > 0 {
		Log("C", "dpiStmt_executeMany", "mode", mode, "len", st.arrLen)
		if C.dpiStmt_executeMany(st.dpiStmt, mode, C.uint32_t(st.arrLen)) == C.DPI_FAILURE {
			err = st.getError()
		}
	} else {
		var colCount C.uint32_t
		Log("C", "dpiStmt_execute", "mode", mode, "colCount", colCount)
		if C.dpiStmt_execute(st.dpiStmt, mode, &colCount) == C.DPI_FAILURE {
			err = st.getError()
		}
	}
	close(done)
	if err != nil {
		return nil, maybeBadConn(errors.Wrapf(err, "dpiStmt_execute(mode=%d arrLen=%d)", mode, st.arrLen))
	}
	if err = <-ctxErr; err != nil {
		return nil, err
	}
	//Log("gets", st.gets)
	for i, get := range st.gets {
		if get == nil {
			continue
		}
		dest := st.dests[i]
		/*
			dest := args[i].Value
			if o, ok := dest.(sql.Out); ok {
				dest = o.Dest
			}
		*/
		if !st.isSlice[i] {
			if err := get(dest, &st.data[i][0]); err != nil {
				return nil, errors.Wrapf(err, "%d. get[%d]", i, 0)
			}
			continue
		}
		var n C.uint32_t = 1
		if C.dpiVar_getNumElementsInArray(st.vars[i], &n) == C.DPI_FAILURE {
			return nil, errors.Wrapf(st.getError(), "%d.getNumElementsInArray", i)
		}
		//fmt.Printf("i=%d dest=%T %#v\n", i, dest, dest)
		re := reflect.ValueOf(dest).Elem()
		re.Set(re.Slice(0, 0))
		if n == 0 {
			continue
		}
		if n := int(n); re.Cap() >= n {
			re.Set(re.Slice(0, n))
		} else {
			re.Set(reflect.MakeSlice(re.Type(), n, n))
		}
		for j := 0; j < int(n); j++ {
			if err := get(re.Index(j).Addr().Interface(), &st.data[i][j]); err != nil {
				return nil, errors.Wrapf(err, "%d. get[%d]", i, j)
			}
		}
	}
	var count C.uint64_t
	if C.dpiStmt_getRowCount(st.dpiStmt, &count) == C.DPI_FAILURE {
		return nil, nil
	}
	return driver.RowsAffected(count), nil
}

// QueryContext executes a query that may return rows, such as a SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (st *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	Log := ctxGetLog(ctx)

	st.Lock()
	defer st.Unlock()
	st.conn.Lock()
	defer st.conn.Unlock()

	if st.query == getConnection {
		Log("msg", "QueryContext", "args", args)
		return &directRow{conn: st.conn, query: st.query, result: []interface{}{st.conn}}, nil
	}

	//fmt.Printf("QueryContext(%+v)\n", args)
	// bind variables
	if err := st.bindVars(args, Log); err != nil {
		return nil, err
	}

	// execute
	done := make(chan struct{}, 1)
	go func() {
		select {
		case <-ctx.Done():
			_ = st.Break()
		case <-done:
			return
		}
	}()
	var colCount C.uint32_t
	res := C.dpiStmt_execute(st.dpiStmt, C.DPI_MODE_EXEC_DEFAULT, &colCount)
	done <- struct{}{}
	if res == C.DPI_FAILURE {
		return nil, maybeBadConn(errors.Wrapf(st.getError(), "dpiStmt_execute"))
	}
	return st.openRows(int(colCount))
}

// bindVars binds the given args into new variables.
func (st *statement) bindVars(args []driver.NamedValue, Log logFunc) error {
	Log("enter", "bindVars", "args", args)
	for i, v := range st.vars {
		if v == nil {
			continue
		}
		C.dpiVar_release(v)
		st.vars[i] = nil
	}
	var named bool
	if cap(st.vars) < len(args) {
		st.vars = make([]*C.dpiVar, len(args))
	} else {
		st.vars = st.vars[:len(args)]
	}
	if cap(st.data) < len(args) {
		st.data = make([][]C.dpiData, len(args))
	} else {
		st.data = st.data[:len(args)]
	}
	if cap(st.gets) < len(args) {
		st.gets = make([]dataGetter, len(args))
	} else {
		st.gets = st.gets[:len(args)]
	}
	if cap(st.dests) < len(args) {
		st.dests = make([]interface{}, len(args))
	} else {
		st.dests = st.dests[:len(args)]
	}
	if cap(st.isSlice) < len(args) {
		st.isSlice = make([]bool, len(args))
	} else {
		st.isSlice = st.isSlice[:len(args)]
	}

	rArgs := make([]reflect.Value, len(args))
	minArrLen, maxArrLen := -1, -1

	st.arrLen = minArrLen

	type argInfo struct {
		isIn, isOut bool
		typ         C.dpiOracleTypeNum
		natTyp      C.dpiNativeTypeNum
		set         dataSetter
		bufSize     int
	}
	infos := make([]argInfo, len(args))
	//fmt.Printf("bindVars %d\n", len(args))
	for i, a := range args {
		st.gets[i] = nil
		st.dests[i] = nil
		if !named {
			named = a.Name != ""
		}
		info := &(infos[i])
		info.isIn = true
		value := a.Value
		if out, ok := value.(sql.Out); ok {
			if !st.PlSQLArrays && st.arrLen > 1 {
				st.arrLen = maxArraySize
			}
			info.isIn, info.isOut = out.In, true
			value = out.Dest
		}
		st.dests[i] = value
		if info.isOut {
			//fmt.Printf("%d. v=%T %#v kind=%s\n", i, value, value, reflect.ValueOf(value).Kind())
			if rv := reflect.ValueOf(value); rv.Kind() == reflect.Ptr {
				value = rv.Elem().Interface()
			}
		}
		st.isSlice[i] = false
		rArgs[i] = reflect.ValueOf(value)
		if rArgs[i].Kind() == reflect.Ptr {
			rArgs[i] = rArgs[i].Elem()
			//value = rArgs[i].Interface() // deref pointer
		}
		if _, isByteSlice := value.([]byte); !isByteSlice {
			st.isSlice[i] = rArgs[i].Kind() == reflect.Slice
			if !st.PlSQLArrays && st.isSlice[i] {
				n := rArgs[i].Len()
				if minArrLen == -1 || n < minArrLen {
					minArrLen = n
				}
				if maxArrLen == -1 || n > maxArrLen {
					maxArrLen = n
				}
			}
		}

		Log("msg", "bindVars", "i", i, "in", info.isIn, "out", info.isOut, "value", fmt.Sprintf("%T %#v", st.dests[i], st.dests[i]))
	}

	if maxArrLen > maxArraySize {
		return errors.Errorf("slice is bigger (%d) than the maximum (%d)", maxArrLen, maxArraySize)
	}
	doManyCount := 1
	doExecMany := !st.PlSQLArrays
	if doExecMany {
		if minArrLen != -1 && minArrLen != maxArrLen {
			return errors.Errorf("PlSQLArrays is not set, but has different lengthed slices (min=%d < %d=max)", minArrLen, maxArrLen)
		}
		st.arrLen = minArrLen
		if doExecMany = st.arrLen > 1; doExecMany {
			doManyCount = st.arrLen
		}
	}
	Log("doManyCount", doManyCount, "arrLen", st.arrLen, "doExecMany", doExecMany, "minArrLen", "maxArrLen")

	for i := range args {
		info := &(infos[i])
		value := st.dests[i]
		if _, ok := value.(*driver.Rows); !ok {
			if rv := reflect.ValueOf(value); rv.Kind() == reflect.Ptr {
				value = rv.Elem().Interface()
			}
		}
		switch v := value.(type) {
		case Lob, []Lob:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_BLOB, C.DPI_NATIVE_TYPE_LOB
			var isClob bool
			switch v := v.(type) {
			case Lob:
				isClob = v.IsClob
			case []Lob:
				isClob = len(v) > 0 && v[0].IsClob
			}
			if isClob {
				info.typ = C.DPI_ORACLE_TYPE_CLOB
			}
			info.set = st.dataSetLOB
			if info.isOut {
				st.gets[i] = st.dataGetLOB
			}
		case *driver.Rows:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_STMT, C.DPI_NATIVE_TYPE_STMT
			info.set = func(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
				data.isNull = 1
				return nil
			}
			//info.set = st.dataSetStmt
			if info.isOut {
				st.gets[i] = st.dataGetStmt
			}

		case int, []int:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_INT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case int32, []int32:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_NATIVE_TYPE_INT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case int64, []int64, sql.NullInt64, []sql.NullInt64:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_INT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case uint, []uint:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_UINT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case uint32, []uint32:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_UINT, C.DPI_NATIVE_TYPE_UINT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case uint64, []uint64:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_UINT64
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case float32, []float32:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_FLOAT, C.DPI_NATIVE_TYPE_FLOAT
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case float64, []float64:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_DOUBLE, C.DPI_NATIVE_TYPE_DOUBLE
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case sql.NullFloat64:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NATIVE_DOUBLE, C.DPI_NATIVE_TYPE_DOUBLE
			info.set = dataSetNumber
			if info.isOut {
				st.gets[i] = dataGetNumber
			}
		case bool, []bool:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_BOOLEAN, C.DPI_NATIVE_TYPE_BOOLEAN
			info.set = func(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
				b := C.int(0)
				if v.(bool) {
					b = 1
				}
				C.dpiData_setBool(data, b)
				return nil
			}
			if info.isOut {
				st.gets[i] = func(v interface{}, data *C.dpiData) error {
					return nil
				}
			}

		case []byte, [][]byte:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_RAW, C.DPI_NATIVE_TYPE_BYTES
			switch v := v.(type) {
			case []byte:
				info.bufSize = len(v)
			case [][]byte:
				for _, b := range v {
					if n := len(b); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
			info.set = dataSetBytes
			if info.isOut {
				info.bufSize = 4000
				st.gets[i] = dataGetBytes
			}

		case Number, []Number:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_NUMBER, C.DPI_NATIVE_TYPE_BYTES
			switch v := v.(type) {
			case Number:
				info.bufSize = 4 * len(v)
			case []Number:
				for _, s := range v {
					if n := 4 * len(s); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
			info.set = dataSetBytes
			if info.isOut {
				info.bufSize = 32767
				st.gets[i] = dataGetBytes
			}

		case string, []string, nil:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_NATIVE_TYPE_BYTES
			switch v := v.(type) {
			case string:
				info.bufSize = 4 * len(v)
			case []string:
				for _, s := range v {
					if n := 4 * len(s); n > info.bufSize {
						info.bufSize = n
					}
				}
			}
			info.set = dataSetBytes
			if info.isOut {
				info.bufSize = 32767
				st.gets[i] = dataGetBytes
			}

		case time.Time, []time.Time:
			info.typ, info.natTyp = C.DPI_ORACLE_TYPE_DATE, C.DPI_NATIVE_TYPE_TIMESTAMP
			info.set = func(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
				t := v.(time.Time)
				if t.IsZero() {
					data.isNull = 1
					return nil
				}

				_, z := t.Zone()
				C.dpiData_setTimestamp(data,
					C.int16_t(t.Year()), C.uint8_t(t.Month()), C.uint8_t(t.Day()),
					C.uint8_t(t.Hour()), C.uint8_t(t.Minute()), C.uint8_t(t.Second()), C.uint32_t(t.Nanosecond()),
					C.int8_t(z/3600), C.int8_t((z%3600)/60),
				)
				return nil
			}
			if info.isOut {
				st.gets[i] = func(v interface{}, data *C.dpiData) error {
					ts := C.dpiData_getTimestamp(data)
					tz := time.Local
					if ts.tzHourOffset != 0 || ts.tzMinuteOffset != 0 {
						tz = time.FixedZone(
							fmt.Sprintf("%02d:%02d", ts.tzHourOffset, ts.tzMinuteOffset),
							int(ts.tzHourOffset)*3600+int(ts.tzMinuteOffset)*60,
						)
					}
					t := time.Date(
						int(ts.year), time.Month(ts.month), int(ts.day),
						int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
						tz)
					Log("msg", "get", "t", t.Format(time.RFC3339), "dest", fmt.Sprintf("%T", v), "tz", ts.tzHourOffset)
					switch x := v.(type) {
					case *time.Time:
						*x = t
					case *interface{}:
						*x = t
					default:
						return errors.Errorf("%d. arg: wanted time.Time, got %T", i+1, v)
					}
					return nil
				}
			}

		default:
			return errors.Errorf("%d. arg: unknown type %T", i+1, value)
		}

		var err error
		var rv reflect.Value
		if st.isSlice[i] {
			rv = reflect.ValueOf(value)
		}

		n := doManyCount
		if st.PlSQLArrays && st.isSlice[i] {
			if info.isOut {
				n = rv.Cap()
			} else {
				n = rv.Len()
			}
		}
		Log("msg", "newVar", "i", i, "plSQLArrays", st.PlSQLArrays, "typ", int(info.typ), "natTyp", int(info.natTyp), "sliceLen", n, "bufSize", info.bufSize, "isSlice", st.isSlice[i])
		//i, st.PlSQLArrays, info.typ, info.natTyp dataSliceLen, info.bufSize)
		if st.vars[i], st.data[i], err = st.newVar(
			st.PlSQLArrays && st.isSlice[i], info.typ, info.natTyp, n, info.bufSize,
		); err != nil {
			return errors.WithMessage(err, fmt.Sprintf("%d", i))
		}

		// Have to setNumElementsInArray for the actual lengths for PL/SQL arrays
		dv, data := st.vars[i], st.data[i]
		if !info.isIn {
			if st.PlSQLArrays {
				Log("C", "dpiVar_setNumElementsInArray", "i", i, "n", 0)
				if C.dpiVar_setNumElementsInArray(dv, C.uint32_t(0)) == C.DPI_FAILURE {
					return errors.Wrapf(st.getError(), "setNumElementsInArray[%d](%d)", i, 0)
				}
			}
			continue
		}

		if !st.isSlice[i] {
			Log("msg", "set", "i", i, "value", fmt.Sprintf("%T=%#v", value, value))
			if err := info.set(dv, 0, &data[0], value); err != nil {
				return errors.Wrapf(err, "set(data[%d][%d], %#v (%T))", i, 0, value, value)
			}
			continue
		}

		n = doManyCount
		if st.PlSQLArrays {
			n = rv.Len()

			Log("C", "dpiVar_setNumElementsInArray", "i", i, "n", n)
			if C.dpiVar_setNumElementsInArray(dv, C.uint32_t(n)) == C.DPI_FAILURE {
				return errors.Wrapf(st.getError(), "%+v.setNumElementsInArray[%d](%d)", dv, i, n)
			}
		}
		//fmt.Println("n:", len(st.data[i]))
		for j := 0; j < n; j++ {
			//fmt.Printf("d[%d]=%p\n", j, st.data[i][j])
			v := rv.Index(j).Interface()
			Log("msg", "set", "i", i, "j", j, "n", n, "v", fmt.Sprintf("%T=%#v", v, v))
			//if err := set(dv, j, &data[j], rArgs[i].Index(j).Interface()); err != nil {
			if err := info.set(dv, j, &data[j], v); err != nil {
				//v := rArgs[i].Index(j).Interface()
				return errors.Wrapf(err, "set(data[%d][%d], %#v (%T))", i, j, v, v)
			}
		}
		//fmt.Printf("data[%d]: %#v\n", i, st.data[i])
	}

	if !named {
		for i, v := range st.vars {
			Log("C", "dpiStmt_bindByPos", "dpiStmt", st.dpiStmt, "i", i, "v", v)
			if C.dpiStmt_bindByPos(st.dpiStmt, C.uint32_t(i+1), v) == C.DPI_FAILURE {
				return errors.Wrapf(st.getError(), "bindByPos[%d]", i)
			}
		}
		return nil
	}
	for i, a := range args {
		name := a.Name
		if name == "" {
			name = strconv.Itoa(a.Ordinal)
		}
		//fmt.Printf("bindByName(%q)\n", name)
		cName := C.CString(name)
		res := C.dpiStmt_bindByName(st.dpiStmt, cName, C.uint32_t(len(name)), st.vars[i])
		C.free(unsafe.Pointer(cName))
		if res == C.DPI_FAILURE {
			return errors.Wrapf(st.getError(), "bindByName[%q]", name)
		}
	}
	return nil
}

type dataSetter func(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error

func dataGetNumber(v interface{}, data *C.dpiData) error {
	switch x := v.(type) {
	case *int:
		*x = int(C.dpiData_getInt64(data))
	case *int32:
		*x = int32(C.dpiData_getInt64(data))
	case *int64:
		*x = int64(C.dpiData_getInt64(data))
	case *sql.NullInt64:
		if data.isNull == 1 {
			x.Valid = false
		} else {
			x.Valid, x.Int64 = true, int64(C.dpiData_getInt64(data))
		}
	case *sql.NullFloat64:
		if data.isNull == 1 {
			x.Valid = false
		} else {
			x.Valid, x.Float64 = true, float64(C.dpiData_getDouble(data))
		}

	case *uint:
		*x = uint(C.dpiData_getUint64(data))
	case *uint32:
		*x = uint32(C.dpiData_getUint64(data))
	case *uint64:
		*x = uint64(C.dpiData_getUint64(data))

	case *float32:
		*x = float32(C.dpiData_getFloat(data))
	case *float64:
		*x = float64(C.dpiData_getDouble(data))

	default:
		return errors.Errorf("unknown number [%T] %#v", v, v)
	}

	//fmt.Printf("setInt64(%#v, %#v)\n", data, C.int64_t(int64(v.(int))))
	return nil
}

func dataSetNumber(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
	switch x := v.(type) {
	case int:
		C.dpiData_setInt64(data, C.int64_t(x))
	case int32:
		C.dpiData_setInt64(data, C.int64_t(x))
	case int64:
		C.dpiData_setInt64(data, C.int64_t(x))
	case sql.NullInt64:
		if x.Valid {
			C.dpiData_setInt64(data, C.int64_t(x.Int64))
		} else {
			data.isNull = 1
		}
	case sql.NullFloat64:
		if x.Valid {
			C.dpiData_setDouble(data, C.double(x.Float64))
		} else {
			data.isNull = 1
		}

	case uint:
		C.dpiData_setUint64(data, C.uint64_t(x))
	case uint32:
		C.dpiData_setUint64(data, C.uint64_t(x))
	case uint64:
		C.dpiData_setUint64(data, C.uint64_t(x))

	case float32:
		C.dpiData_setFloat(data, C.float(x))
	case float64:
		C.dpiData_setDouble(data, C.double(x))

	default:
		return errors.Errorf("unknown number [%T] %#v", v, v)
	}

	//fmt.Printf("setInt64(%#v, %#v)\n", data, C.int64_t(int64(v.(int))))
	return nil
}

func dataGetBytes(v interface{}, data *C.dpiData) error {
	switch x := v.(type) {
	case *[]byte:
		if data.isNull == 1 {
			*x = nil
			return nil
		}
		b := C.dpiData_getBytes(data)

		*x = ((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length]

	case *Number:
		if data.isNull == 1 {
			*x = ""
			return nil
		}
		b := C.dpiData_getBytes(data)
		*x = Number(((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length])

	case *string:
		if data.isNull == 1 {
			*x = ""
			return nil
		}
		b := C.dpiData_getBytes(data)
		*x = string(((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length])

	case *interface{}:
		switch y := (*x).(type) {
		case []byte:
			if err := dataGetBytes(&y, data); err != nil {
				return err
			}
			*x = y
			return nil

		case Number:
			if err := dataGetBytes(&y, data); err != nil {
				return err
			}
			*x = y
			return nil

		case string:
			if err := dataGetBytes(&y, data); err != nil {
				return err
			}
			*x = y
			return nil

		default:
			return errors.Errorf("awaited []byte/string/Number, got %T (%#v)", x, x)
		}

	default:
		return errors.Errorf("awaited []byte/string/Number, got %T (%#v)", v, v)
	}
	return nil
}

func dataSetBytes(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
	var p *C.char
	switch x := v.(type) {
	case []byte:
		if len(x) > 0 {
			p = (*C.char)(unsafe.Pointer(&x[0]))
		}
		Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(x))
		C.dpiVar_setFromBytes(dv, C.uint32_t(pos), p, C.uint32_t(len(x)))

	case Number:
		b := []byte(x)
		if len(b) > 0 {
			p = (*C.char)(unsafe.Pointer(&b[0]))
		}
		Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(b))
		C.dpiVar_setFromBytes(dv, C.uint32_t(pos), p, C.uint32_t(len(b)))

	case string:
		b := []byte(x)
		if len(b) > 0 {
			p = (*C.char)(unsafe.Pointer(&b[0]))
		}
		Log("C", "dpiVar_setFromBytes", "dv", dv, "pos", pos, "p", p, "len", len(b))
		C.dpiVar_setFromBytes(dv, C.uint32_t(pos), p, C.uint32_t(len(b)))

	case nil:
		data.isNull = 1

	default:
		return errors.Errorf("awaited []byte/string/Number, got %T (%#v)", v, v)
	}
	return nil
}

func (c *conn) dataGetStmt(v interface{}, data *C.dpiData) error {
	st := &statement{conn: c, dpiStmt: C.dpiData_getStmt(data)}
	var n C.uint32_t
	if C.dpiStmt_getNumQueryColumns(st.dpiStmt, &n) == C.DPI_FAILURE {
		reflect.ValueOf(v).Elem().Set(reflect.ValueOf(&rows{
			err: errors.Wrapf(io.EOF, "getNumQueryColumns: %v", c.getError()),
		}))
		return nil
	}
	rows, err := st.openRows(int(n))
	if err != nil {
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(rows))
	return nil
}

func (c *conn) dataGetLOB(v interface{}, data *C.dpiData) error {
	L := v.(*Lob)
	lob := C.dpiData_getLOB(data)
	if lob == nil {
		L.Reader = nil
		return nil
	}
	L.Reader = &dpiLobReader{conn: c, dpiLob: lob, IsClob: L.IsClob}
	return nil
}
func (c *conn) dataSetLOB(dv *C.dpiVar, pos int, data *C.dpiData, v interface{}) error {
	L := v.(Lob)
	if v == nil || L.Reader == nil {
		data.isNull = 1
		return nil
	}
	typ := C.dpiOracleTypeNum(C.DPI_ORACLE_TYPE_BLOB)
	if L.IsClob {
		typ = C.DPI_ORACLE_TYPE_CLOB
	}
	var lob *C.dpiLob
	if C.dpiConn_newTempLob(c.dpiConn, typ, &lob) == C.DPI_FAILURE {
		return errors.Wrapf(c.getError(), "newTempLob(typ=%d)", typ)
	}
	var chunkSize C.uint32_t
	_ = C.dpiLob_getChunkSize(lob, &chunkSize)
	if chunkSize == 0 {
		chunkSize = 8192
	}
	for chunkSize < minChunkSize {
		chunkSize <<= 1
	}
	lw := &dpiLobWriter{dpiLob: lob, conn: c, isClob: L.IsClob}
	_, err := io.CopyBuffer(lw, L, make([]byte, int(chunkSize)))
	//fmt.Printf("%p written %d with chunkSize=%d\n", lob, n, chunkSize)
	if closeErr := lw.Close(); closeErr != nil {
		if err == nil {
			err = closeErr
		}
		//fmt.Printf("close %p: %+v\n", lob, closeErr)
	}
	C.dpiVar_setFromLob(dv, C.uint32_t(pos), lob)
	return err
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
//
// If CheckNamedValue returns ErrRemoveArgument, the NamedValue will not be included
// in the final query arguments.
// This may be used to pass special options to the query itself.
//
// If ErrSkip is returned the column converter error checking path is used
// for the argument.
// Drivers may wish to return ErrSkip after they have exhausted their own special cases.
func (st *statement) CheckNamedValue(nv *driver.NamedValue) error {
	if nv == nil {
		return nil
	}
	if nv.Value == PlSQLArrays {
		st.PlSQLArrays = true
		return driver.ErrRemoveArgument
	}
	return nil
}

// ColumnConverter may be optionally implemented by Stmt
// if the statement is aware of its own columns' types and
// can convert from any type to a driver Value.
func (st *statement) ColumnConverter(idx int) driver.ValueConverter {
	c := driver.ValueConverter(driver.DefaultParameterConverter)
	switch col := st.columns[idx]; col.OracleType {
	case C.DPI_ORACLE_TYPE_NUMBER:
		switch col.NativeType {
		case C.DPI_NATIVE_TYPE_INT64, C.DPI_NATIVE_TYPE_UINT64:
			c = Int64
		//case C.DPI_NATIVE_TYPE_FLOAT, C.DPI_NATIVE_TYPE_DOUBLE:
		//	c = Float64
		default:
			c = Num
		}
	}
	Log("msg", "ColumnConverter", "c", c)
	return driver.Null{Converter: c}
}

func (st *statement) openRows(colCount int) (*rows, error) {
	C.dpiStmt_setFetchArraySize(st.dpiStmt, fetchRowCount)

	r := rows{
		statement: st,
		columns:   make([]Column, colCount),
		vars:      make([]*C.dpiVar, colCount),
		data:      make([][]C.dpiData, colCount),
	}
	var info C.dpiQueryInfo
	var ti C.dpiDataTypeInfo
	for i := 0; i < colCount; i++ {
		if C.dpiStmt_getQueryInfo(st.dpiStmt, C.uint32_t(i+1), &info) == C.DPI_FAILURE {
			return nil, errors.Wrapf(st.getError(), "getQueryInfo[%d]", i)
		}
		ti = info.typeInfo
		bufSize := int(ti.clientSizeInBytes)
		//Log("msg", "openRows", "col", i, "info", ti)
		//Log("dNTN", int(ti.defaultNativeTypeNum), "number", C.DPI_ORACLE_TYPE_NUMBER)
		switch ti.oracleTypeNum {
		case C.DPI_ORACLE_TYPE_NUMBER:
			switch ti.defaultNativeTypeNum {
			case C.DPI_NATIVE_TYPE_FLOAT, C.DPI_NATIVE_TYPE_DOUBLE:
				ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_BYTES
				bufSize = 40
			}
		case C.DPI_ORACLE_TYPE_DATE:
			ti.defaultNativeTypeNum = C.DPI_NATIVE_TYPE_TIMESTAMP
		}
		r.columns[i] = Column{
			Name:       C.GoStringN(info.name, C.int(info.nameLength)),
			OracleType: ti.oracleTypeNum,
			NativeType: ti.defaultNativeTypeNum,
			Size:       ti.clientSizeInBytes,
			Precision:  ti.precision,
			Scale:      ti.scale,
			Nullable:   info.nullOk == 1,
			ObjectType: ti.objectType,
		}
		switch ti.oracleTypeNum {
		case C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_ORACLE_TYPE_NVARCHAR, C.DPI_ORACLE_TYPE_CHAR, C.DPI_ORACLE_TYPE_NCHAR:
			bufSize *= 4
		}
		var err error
		//fmt.Printf("%d. %+v\n", i, r.columns[i])
		if r.vars[i], r.data[i], err = st.newVar(
			false, ti.oracleTypeNum, ti.defaultNativeTypeNum, fetchRowCount, bufSize,
		); err != nil {
			return nil, err
		}

		if C.dpiStmt_define(st.dpiStmt, C.uint32_t(i+1), r.vars[i]) == C.DPI_FAILURE {
			return nil, errors.Wrapf(st.getError(), "define[%d]", i)
		}
	}
	if C.dpiStmt_addRef(st.dpiStmt) == C.DPI_FAILURE {
		return &r, errors.Wrap(st.getError(), "dpiStmt_addRef")
	}
	st.columns = r.columns
	return &r, nil
}

// Column holds the info from a column.
type Column struct {
	Name       string
	OracleType C.dpiOracleTypeNum
	NativeType C.dpiNativeTypeNum
	Size       C.uint32_t
	Precision  C.int16_t
	Scale      C.int8_t
	Nullable   bool
	ObjectType *C.dpiObjectType
}
