// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

//go:build go1.27

package godror

/*
#include "dpiImpl.h"
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"runtime"
	"strconv"
	"time"
	"unsafe"
)

var _ driver.RowsColumnScanner = (*rows)(nil)

// NextRow advances to the next row, to be read with ScanColumn.
func (r *rows) NextRow() error {
	if r.err != nil {
		return r.err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if r.scratch == nil {
		r.logger = getLogger(context.Background())
	}
	if err := r.fetch(context.Background(), r.logger); err != nil {
		return err
	}
	if r.scratch == nil {
		r.scratch = make([]driver.Value, len(r.columns))
		r.callsC = make([]bool, len(r.columns))
		for i, col := range r.columns {
			r.callsC[i] = !plainColumn(col.OracleType)
		}
		r.options.load(r.statement)
	}
	r.current = r.bufferRowIndex
	r.bufferRowIndex++
	r.fetched--
	return nil
}

// ScanColumn stores the column of the current row into dest.
// Common destinations are set directly, others go through sql.ConvertAssign.
func (r *rows) ScanColumn(scanCtx driver.ScanContext, i int, dest any) error {
	p, isAny := dest.(*any)
	if !isAny && r.scanColumnDirect(i, dest) {
		return nil
	}
	if r.callsC[i] {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	if err := r.setValue(context.Background(), r.logger, &r.options, i, r.current, r.scratch); err != nil {
		return err
	}
	if isAny {
		*p = r.scratch[i]
		return nil
	}
	return sql.ConvertAssign(scanCtx, dest, r.scratch[i])
}

func plainColumn(typ C.dpiOracleTypeNum) bool {
	switch typ {
	case C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_ORACLE_TYPE_NVARCHAR,
		C.DPI_ORACLE_TYPE_CHAR, C.DPI_ORACLE_TYPE_NCHAR,
		C.DPI_ORACLE_TYPE_LONG_VARCHAR, C.DPI_ORACLE_TYPE_LONG_NVARCHAR,
		C.DPI_ORACLE_TYPE_NUMBER, C.DPI_ORACLE_TYPE_RAW, C.DPI_ORACLE_TYPE_LONG_RAW,
		C.DPI_ORACLE_TYPE_NATIVE_FLOAT, C.DPI_ORACLE_TYPE_NATIVE_DOUBLE,
		C.DPI_ORACLE_TYPE_NATIVE_INT, C.DPI_ORACLE_TYPE_NATIVE_UINT,
		C.DPI_ORACLE_TYPE_DATE, C.DPI_ORACLE_TYPE_TIMESTAMP,
		C.DPI_ORACLE_TYPE_TIMESTAMP_TZ, C.DPI_ORACLE_TYPE_TIMESTAMP_LTZ,
		C.DPI_ORACLE_TYPE_INTERVAL_DS, C.DPI_ORACLE_TYPE_INTERVAL_YM:
		return true
	}
	return false
}

func (r *rows) scanColumnDirect(i int, dest any) bool {
	col := r.columns[i]
	d := &r.data[i][r.current]
	isNull := d.isNull == 1
	switch col.OracleType {
	case C.DPI_ORACLE_TYPE_VARCHAR, C.DPI_ORACLE_TYPE_NVARCHAR,
		C.DPI_ORACLE_TYPE_CHAR, C.DPI_ORACLE_TYPE_NCHAR,
		C.DPI_ORACLE_TYPE_LONG_VARCHAR, C.DPI_ORACLE_TYPE_LONG_NVARCHAR:
		switch p := dest.(type) {
		case *string:
			*p = ""
			if !isNull {
				*p = bytesString(d)
			}
			return true
		case *sql.NullString:
			*p = sql.NullString{String: "", Valid: true}
			if !isNull {
				p.String = bytesString(d)
			}
			return true
		}

	case C.DPI_ORACLE_TYPE_NUMBER:
		if r.options.naf {
			return false
		}
		switch col.NativeType {
		case C.DPI_NATIVE_TYPE_INT64:
			i64 := *((*int64)(unsafe.Pointer(&d.value)))
			switch p := dest.(type) {
			case *int64:
				if isNull {
					return false
				}
				*p = i64
				return true
			case *sql.NullInt64:
				*p = sql.NullInt64{Int64: i64, Valid: !isNull}
				return true
			case *int:
				if isNull {
					return false
				}
				*p = int(i64)
				return true
			case *sql.NullString:
				*p = sql.NullString{}
				if !isNull {
					*p = sql.NullString{String: strconv.FormatInt(i64, 10), Valid: true}
				}
				return true
			}
		case C.DPI_NATIVE_TYPE_BYTES:
			switch p := dest.(type) {
			case *Number:
				*p = ""
				if !isNull {
					*p = Number(bytesString(d))
				}
				return true
			case *string:
				if isNull {
					return false
				}
				*p = bytesString(d)
				return true
			case *sql.NullString:
				*p = sql.NullString{}
				if !isNull {
					*p = sql.NullString{String: bytesString(d), Valid: true}
				}
				return true
			}
		}

	case C.DPI_ORACLE_TYPE_DATE, C.DPI_ORACLE_TYPE_TIMESTAMP:
		switch p := dest.(type) {
		case *time.Time:
			if isNull {
				return false
			}
			*p = r.timestamp(d)
			return true
		case *sql.NullTime:
			if isNull {
				if r.options.nullDate != nil {
					return false
				}
				*p = sql.NullTime{}
				return true
			}
			*p = sql.NullTime{Time: r.timestamp(d), Valid: true}
			return true
		}
	}
	return false
}

func bytesString(d *C.dpiData) string {
	b := (*C.dpiBytes)(unsafe.Pointer(&d.value))
	if b.length == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(b.ptr)), b.length))
}

func (r *rows) timestamp(d *C.dpiData) time.Time {
	ts := *((*C.dpiTimestamp)(unsafe.Pointer(&d.value)))
	tz := r.conn.Timezone()
	if tz == nil {
		tz = time.Local
	}
	return time.Date(
		int(ts.year), time.Month(ts.month), int(ts.day),
		int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
		tz,
	)
}
