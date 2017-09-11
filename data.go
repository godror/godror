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
import (
	"fmt"
	"time"
	"unsafe"
)

type Data struct {
	NativeTypeNum C.dpiNativeTypeNum
	dpiData       *C.dpiData
}

func (d *Data) IsNull() bool {
	return d.dpiData.isNull == 1
}
func (d *Data) GetBool() bool {
	return C.dpiData_getBool(d.dpiData) == 1
}
func (d *Data) SetBool(b bool) {
	var i C.int
	if b {
		i = 1
	}
	C.dpiData_setBool(d.dpiData, i)
}
func (d *Data) GetBytes() []byte {
	if d.IsNull() {
		return nil
	}
	b := C.dpiData_getBytes(d.dpiData)
	return ((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length]
}
func (d *Data) SetBytes(b []byte) {
	if b == nil {
		d.dpiData.isNull = 1
		return
	}
	C.dpiData_setBytes(d.dpiData, (*C.char)(unsafe.Pointer(&b[0])), C.uint32_t(len(b)))
}
func (d *Data) GetFloat32() float32 {
	return float32(C.dpiData_getFloat(d.dpiData))
}
func (d *Data) SetFloat32(f float32) {
	C.dpiData_setFloat(d.dpiData, C.float(f))
}

func (d *Data) GetFloat64() float64 {
	return float64(C.dpiData_getDouble(d.dpiData))
}
func (d *Data) SetFloat64(f float64) {
	C.dpiData_setDouble(d.dpiData, C.double(f))
}
func (d *Data) GetInt64() int64 {
	return int64(C.dpiData_getInt64(d.dpiData))
}
func (d *Data) SetInt64(i int64) {
	C.dpiData_setInt64(d.dpiData, C.int64_t(i))
}
func (d *Data) GetIntervalDS() time.Duration {
	ds := C.dpiData_getIntervalDS(d.dpiData)
	return time.Duration(ds.days)*24*time.Hour +
		time.Duration(ds.hours)*time.Hour +
		time.Duration(ds.minutes)*time.Minute +
		time.Duration(ds.seconds)*time.Second +
		time.Duration(ds.fseconds)
}
func (d *Data) SetIntervalDS(dur time.Duration) {
	C.dpiData_setIntervalDS(d.dpiData,
		C.int32_t(int64(dur.Hours())/24),
		C.int32_t(int64(dur.Hours())%24), C.int32_t(dur.Minutes()), C.int32_t(dur.Seconds()),
		C.int32_t(dur.Nanoseconds()),
	)
}
func (d *Data) GetIntervalYM() IntervalYM {
	ym := C.dpiData_getIntervalYM(d.dpiData)
	return IntervalYM{Years: int(ym.years), Months: int(ym.months)}
}
func (d *Data) SetIntervalYM(ym IntervalYM) {
	C.dpiData_setIntervalYM(d.dpiData, C.int32_t(ym.Years), C.int32_t(ym.Months))
}
func (d *Data) GetLob() *Lob {
	var L Lob
	if !d.IsNull() {
		L.Reader = &dpiLobReader{dpiLob: C.dpiData_getLOB(d.dpiData)}
	}
	return &L
}
func (d *Data) GetObject() *Object {
	return &Object{dpiObject: C.dpiData_getObject(d.dpiData)}
}
func (d *Data) SetObject(o *Object) {
	C.dpiData_setObject(d.dpiData, o.dpiObject)
}
func (d *Data) GetStmt() *statement {
	return &statement{dpiStmt: C.dpiData_getStmt(d.dpiData)}
}
func (d *Data) SetStmt(s *statement) {
	C.dpiData_setStmt(d.dpiData, s.dpiStmt)
}
func (d *Data) GetTime() time.Time {
	ts := C.dpiData_getTimestamp(d.dpiData)
	tz := time.Local
	if ts.tzHourOffset != 0 || ts.tzMinuteOffset != 0 {
		tz = time.FixedZone(
			fmt.Sprintf("%02d:%02d", ts.tzHourOffset, ts.tzMinuteOffset),
			int(ts.tzHourOffset)*3600+int(ts.tzMinuteOffset)*60,
		)
	}
	return time.Date(
		int(ts.year), time.Month(ts.month), int(ts.day),
		int(ts.hour), int(ts.minute), int(ts.second), int(ts.fsecond),
		tz)

}
func (d *Data) SetTime(t time.Time) {
	_, z := t.Zone()
	C.dpiData_setTimestamp(d.dpiData,
		C.int16_t(t.Year()), C.uint8_t(t.Month()), C.uint8_t(t.Day()),
		C.uint8_t(t.Hour()), C.uint8_t(t.Minute()), C.uint8_t(t.Second()), C.uint32_t(t.Nanosecond()),
		C.int8_t(z/3600), C.int8_t((z%3600)/60),
	)
}
func (d *Data) GetUint64() uint64 {
	return uint64(C.dpiData_getUint64(d.dpiData))
}
func (d *Data) SetUint64(u uint64) {
	C.dpiData_setUint64(d.dpiData, C.uint64_t(u))
}

type IntervalYM struct {
	Years, Months int
}

func (d *Data) Get() interface{} {
	switch d.NativeTypeNum {
	case C.DPI_NATIVE_TYPE_BOOLEAN:
		return d.GetBool()
	case C.DPI_NATIVE_TYPE_BYTES:
		return d.GetBytes()
	case C.DPI_NATIVE_TYPE_DOUBLE:
		return d.GetFloat64()
	case C.DPI_NATIVE_TYPE_FLOAT:
		return d.GetFloat32()
	case C.DPI_NATIVE_TYPE_INT64:
		return d.GetInt64()
	case C.DPI_NATIVE_TYPE_INTERVAL_DS:
		return d.GetIntervalDS()
	case C.DPI_NATIVE_TYPE_INTERVAL_YM:
		return d.GetIntervalYM()
	case C.DPI_NATIVE_TYPE_LOB:
		return d.GetLob()
	case C.DPI_NATIVE_TYPE_OBJECT:
		return d.GetObject()
	case C.DPI_NATIVE_TYPE_STMT:
		return d.GetStmt()
	case C.DPI_NATIVE_TYPE_TIMESTAMP:
		return d.GetTime()
	case C.DPI_NATIVE_TYPE_UINT64:
		return d.GetUint64()
	default:
		panic(fmt.Sprintf("unknown NativeTypeNum=%d", d.NativeTypeNum))
	}
	return nil
}
