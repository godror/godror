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
	"unsafe"
)

type Object struct {
	dpiObject *C.dpiObject
	*ObjectType
}

func (O *Object) GetAttribute(data *Data, i int) error {
	attr := O.attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	if C.dpiObject_getAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}
func (O *Object) SetAttribute(i int, data *Data) error {
	attr := O.attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	if C.dpiObject_setAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
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
	if C.dpiObject_appendElement(O.dpiObject, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
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
	if C.dpiObject_getElementValueByIndex(O.dpiObject, idx, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

func (O *ObjectCollection) Set(i int, data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.info.(objectCollectionInfo).nativeTypeNum
	}
	if C.dpiObject_setElementValueByIndex(O.dpiObject, C.int32_t(i), data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
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
