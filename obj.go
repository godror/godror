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
	"errors"
	"fmt"
	"time"
	"unsafe"
)

type Data struct {
	NativeTypeNum C.dpiNativeTypeNum
	Data          *C.dpiData
}

func (d *Data) IsNull() bool {
	return d.Data.isNull == 1
}
func (d *Data) GetBool() bool {
	return C.dpiData_getBool(d.Data) == 1
}
func (d *Data) SetBool(b bool) {
	var i C.int
	if b {
		i = 1
	}
	C.dpiData_setBool(d.Data, i)
}
func (d *Data) GetBytes() []byte {
	if d.IsNull() {
		return nil
	}
	b := C.dpiData_getBytes(d.Data)
	return ((*[32767]byte)(unsafe.Pointer(b.ptr)))[:b.length:b.length]
}
func (d *Data) SetBytes(b []byte) {
	if b == nil {
		d.Data.isNull = 1
		return
	}
	C.dpiData_setBytes(d.Data, (*C.char)(unsafe.Pointer(&b[0])), C.uint32_t(len(b)))
}
func (d *Data) GetFloat32() float32 {
	return float32(C.dpiData_getFloat(d.Data))
}
func (d *Data) SetFloat32(f float32) {
	C.dpiData_setFloat(d.Data, C.float(f))
}

func (d *Data) GetFloat64() float64 {
	return float64(C.dpiData_getDouble(d.Data))
}
func (d *Data) SetFloat64(f float64) {
	C.dpiData_setDouble(d.Data, C.double(f))
}
func (d *Data) GetInt64() int64 {
	return int64(C.dpiData_getInt64(d.Data))
}
func (d *Data) SetInt64(i int64) {
	C.dpiData_setInt64(d.Data, C.int64_t(i))
}
func (d *Data) GetIntervalDS() time.Duration {
	ds := C.dpiData_getIntervalDS(d.Data)
	return time.Duration(ds.days)*24*time.Hour +
		time.Duration(ds.hours)*time.Hour +
		time.Duration(ds.minutes)*time.Minute +
		time.Duration(ds.seconds)*time.Second +
		time.Duration(ds.fseconds)
}
func (d *Data) SetIntervalDS(dur time.Duration) {
	C.dpiData_setIntervalDS(d.Data,
		C.int32_t(int64(dur.Hours())/24),
		C.int32_t(int64(dur.Hours())%24), C.int32_t(dur.Minutes()), C.int32_t(dur.Seconds()),
		C.int32_t(dur.Nanoseconds()),
	)
}
func (d *Data) GetIntervalYM() IntervalYM {
	ym := C.dpiData_getIntervalYM(d.Data)
	return IntervalYM{Years: int(ym.years), Months: int(ym.months)}
}
func (d *Data) SetIntervalYM(ym IntervalYM) {
	C.dpiData_setIntervalYM(d.Data, C.int32_t(ym.Years), C.int32_t(ym.Months))
}
func (d *Data) GetLob() *Lob {
	var L Lob
	if !d.IsNull() {
		L.Reader = &dpiLobReader{dpiLob: C.dpiData_getLOB(d.Data)}
	}
	return &L
}
func (d *Data) GetObject() *Object {
	return &Object{dpiObject: C.dpiData_getObject(d.Data)}
}
func (d *Data) SetObject(o *Object) {
	C.dpiData_setObject(d.Data, o.dpiObject)
}
func (d *Data) GetStmt() *statement {
	return &statement{dpiStmt: C.dpiData_getStmt(d.Data)}
}
func (d *Data) SetStmt(s *statement) {
	C.dpiData_setStmt(d.Data, s.dpiStmt)
}
func (d *Data) GetTime() time.Time {
	ts := C.dpiData_getTimestamp(d.Data)
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
	C.dpiData_setTimestamp(d.Data,
		C.int16_t(t.Year()), C.uint8_t(t.Month()), C.uint8_t(t.Day()),
		C.uint8_t(t.Hour()), C.uint8_t(t.Minute()), C.uint8_t(t.Second()), C.uint32_t(t.Nanosecond()),
		C.int8_t(z/3600), C.int8_t((z%3600)/60),
	)
}
func (d *Data) GetUint64() uint64 {
	return uint64(C.dpiData_getUint64(d.Data))
}
func (d *Data) SetUint64(u uint64) {
	C.dpiData_setUint64(d.Data, C.uint64_t(u))
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

type Object struct {
	dpiObject *C.dpiObject
	*ObjectType
}

func (O *Object) GetAttribute(data *Data, i int) error {
	attr := O.attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	if C.dpiObject_getAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.Data) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}
func (O *Object) SetAttribute(i int, data *Data) error {
	attr := O.attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	if C.dpiObject_setAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.Data) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

type ObjectCollection struct {
	Object
}

var ErrNotCollection = errors.New("not collection")
var ErrNotExist = errors.New("not exist")

func (O *ObjectCollection) Append(data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.info.(objectCollectionInfo).nativeTypeNum
	}
	if C.dpiObject_appendElement(O.dpiObject, data.NativeTypeNum, data.Data) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}
func (O *ObjectCollection) Delete(i int) error {
	if C.dpiObject_deleteElementByIndex(O.dpiObject, C.int32_t(i)) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}
func (O *ObjectCollection) Get(data *Data, i int) error {
	idx := C.int32_t(i)
	var exists C.int
	if C.dpiObject_getElementExistsByIndex(O.dpiObject, idx, &exists) == C.DPI_FAILURE {
		return O.getError()
	}
	if exists == 0 {
		return ErrNotExist
	}
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.info.(objectCollectionInfo).nativeTypeNum
	}
	if C.dpiObject_getElementValueByIndex(O.dpiObject, idx, data.NativeTypeNum, data.Data) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

func (O *ObjectCollection) Set(i int, data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.info.(objectCollectionInfo).nativeTypeNum
	}
	if C.dpiObject_setElementValueByIndex(O.dpiObject, C.int32_t(i), data.NativeTypeNum, data.Data) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

func (O *ObjectCollection) First() (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getFirstIndex(O.dpiObject, &idx, &exists) == C.DPI_FAILURE {
		return 0, O.getError()
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}
func (O *ObjectCollection) Last() (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getLastIndex(O.dpiObject, &idx, &exists) == C.DPI_FAILURE {
		return 0, O.getError()
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}
func (O *ObjectCollection) Next(i int) (int, error) {
	var exists C.int
	var idx C.int32_t
	if C.dpiObject_getNextIndex(O.dpiObject, C.int32_t(i), &idx, &exists) == C.DPI_FAILURE {
		return 0, O.getError()
	}
	if exists == 1 {
		return int(idx), nil
	}
	return 0, ErrNotExist
}
func (O *ObjectCollection) Len() (int, error) {
	var size C.int32_t
	if C.dpiObject_getSize(O.dpiObject, &size) == C.DPI_FAILURE {
		return 0, O.getError()
	}
	return int(size), nil
}
func (O *ObjectCollection) Trim(n int) error {
	if C.dpiObject_trim(O.dpiObject, C.uint32_t(n)) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

type ObjectType struct {
	*drv
	dpiObjectType *C.dpiObjectType
	info          ObjectInfo
	attributes    []ObjectAttribute
}

func (c *conn) GetObjectType(name string) (*ObjectType, error) {
	cName := C.CString(name)
	defer func() { C.free(unsafe.Pointer(cName)) }()
	objType := (*C.dpiObjectType)(C.malloc(C.sizeof_void))
	if C.dpiConn_getObjectType(c.dpiConn, cName, C.uint32_t(len(name)), (**C.dpiObjectType)(unsafe.Pointer(&objType))) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(objType))
		return nil, c.getError()
	}
	return &ObjectType{dpiObjectType: objType}, nil
}

func (t *ObjectType) NewObject() (*Object, error) {
	obj := Object{ObjectType: t}
	if C.dpiObjectType_createObject(t.dpiObjectType, (**C.dpiObject)(unsafe.Pointer(&obj.dpiObject))) == C.DPI_FAILURE {
		return nil, t.getError()
	}
	return &obj, nil
}
func (t *ObjectType) Attributes() ([]ObjectAttribute, error) {
	if t.attributes != nil {
		return t.attributes, nil
	}
	info, err := t.Info()
	if err != nil {
		return nil, err
	}
	if info.NumAttributes() == 0 {
		t.attributes = []ObjectAttribute{}
		return t.attributes, nil
	}
	t.attributes = make([]ObjectAttribute, int(info.NumAttributes()))
	attrs := make([]*C.dpiObjectAttr, len(t.attributes))
	if C.dpiObjectType_getAttributes(t.dpiObjectType,
		C.uint16_t(len(attrs)),
		(**C.dpiObjectAttr)(unsafe.Pointer(&attrs[0])),
	) == C.DPI_FAILURE {
		return nil, t.getError()
	}
	for i, attr := range attrs {
		var attrInfo C.dpiObjectAttrInfo
		if C.dpiObjectAttr_getInfo(attr, &attrInfo) == C.DPI_FAILURE {
			return t.attributes, t.getError()
		}
		t.attributes[i] = ObjectAttribute{
			drv:           t.drv,
			dpiObjectAttr: attr,
			Name:          C.GoStringN(attrInfo.name, C.int(attrInfo.nameLength)),
			OracleTypeNum: attrInfo.oracleTypeNum,
			NativeTypeNum: attrInfo.defaultNativeTypeNum,
		}
		if attrInfo.objectType != nil {
			t.attributes[i].ObjectType = &ObjectType{dpiObjectType: attrInfo.objectType}
		}
	}
	return t.attributes, nil
}
func (t *ObjectType) Info() (ObjectInfo, error) {
	if t.info.Name() != "" {
		return t.info, nil
	}
	var info C.dpiObjectTypeInfo
	if C.dpiObjectType_getInfo(t.dpiObjectType, &info) == C.DPI_FAILURE {
		return t.info, t.getError()
	}
	oInfo := objectInfo{
		schema: C.GoStringN(info.schema, C.int(info.schemaLength)),
		name:   C.GoStringN(info.name, C.int(info.nameLength)),
	}
	t.info = oInfo
	if info.isCollection == 0 {
		return t.info, nil
	}
	cInfo := objectCollectionInfo{
		objectInfo:    oInfo,
		oracleTypeNum: info.elementOracleTypeNum,
		nativeTypeNum: info.elementDefaultNativeTypeNum,
	}
	if info.elementObjectType != nil {
		cInfo.objectType = &ObjectType{dpiObjectType: info.elementObjectType}
	}
	t.info = cInfo
	return t.info, nil
}

type ObjectInfo interface {
	Schema() string
	Name() string
	NumAttributes() int
	IsCollection() bool
}
type ObjectCollectionInfo interface {
	OracleTypeNum() C.dpiOracleTypeNum
	NativeTypeNum() C.dpiNativeTypeNum
	ObjectType() *ObjectType
}

type objectCollectionInfo struct {
	objectInfo
	oracleTypeNum C.dpiOracleTypeNum
	nativeTypeNum C.dpiNativeTypeNum
	objectType    *ObjectType
}

func (c objectCollectionInfo) OracleTypeNum() C.dpiOracleTypeNum { return c.oracleTypeNum }
func (c objectCollectionInfo) NativeTypeNum() C.dpiNativeTypeNum { return c.nativeTypeNum }
func (c objectCollectionInfo) ObjectType() *ObjectType           { return c.objectType }
func (c objectCollectionInfo) IsCollection() bool                { return true }

type objectInfo struct {
	schema, name  string
	numAttributes int
}

func (i objectInfo) Schema() string     { return i.schema }
func (i objectInfo) Name() string       { return i.name }
func (i objectInfo) NumAttributes() int { return i.numAttributes }
func (i objectInfo) IsCollection() bool { return false }

type ObjectAttribute struct {
	*drv
	dpiObjectAttr *C.dpiObjectAttr
	Name          string
	OracleTypeNum C.dpiOracleTypeNum
	NativeTypeNum C.dpiNativeTypeNum
	ObjectType    *ObjectType
}

func (A ObjectAttribute) Close() error {
	attr := A.dpiObjectAttr
	if attr == nil {
		return nil
	}

	A.dpiObjectAttr = nil
	if C.dpiObjectAttr_release(attr) == C.DPI_FAILURE {
		return A.getError()
	}
	return nil
}
