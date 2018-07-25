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
	"unsafe"

	"github.com/pkg/errors"
)

// Object represents a dpiObject.
type Object struct {
	dpiObject *C.dpiObject
	ObjectType
}

func (O *Object) getError() error { return O.drv.getError() }

// GetAttribute gets the i-th attribute into data.
func (O *Object) GetAttribute(data *Data, i int) error {
	attr := O.Attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	wasNull := data.dpiData == nil
	if wasNull {
		data.dpiData = (*C.dpiData)(C.malloc(C.sizeof_void))
	}
	if C.dpiObject_getAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		if wasNull {
			C.free(unsafe.Pointer(data.dpiData))
		}
		return O.getError()
	}
	return nil
}

// SetAttribute sets the i-th attribute with data.
func (O *Object) SetAttribute(i int, data *Data) error {
	attr := O.Attributes[i]
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = attr.NativeTypeNum
	}
	if C.dpiObject_setAttributeValue(O.dpiObject, attr.dpiObjectAttr, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// ObjectCollection represents a Collection of Objects - itself an Object, too.
type ObjectCollection struct {
	Object
}

// ErrNotCollection is returned when the Object is not a collection.
var ErrNotCollection = errors.New("not collection")

// ErrNotExist is returned when the collection's requested element does not exist.
var ErrNotExist = errors.New("not exist")

// Append data to the collection.
func (O *ObjectCollection) Append(data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.NativeTypeNum
	}
	if C.dpiObject_appendElement(O.dpiObject, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// Delete i-th element of the collection.
func (O *ObjectCollection) Delete(i int) error {
	if C.dpiObject_deleteElementByIndex(O.dpiObject, C.int32_t(i)) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// Get the i-th element of the collection into data.
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
		data.NativeTypeNum = O.NativeTypeNum
	}
	if C.dpiObject_getElementValueByIndex(O.dpiObject, idx, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// Set the i-th element of the collection with data.
func (O *ObjectCollection) Set(i int, data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.NativeTypeNum
	}
	if C.dpiObject_setElementValueByIndex(O.dpiObject, C.int32_t(i), data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// First returns the first element's index of the collection.
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

// Last returns the index of the last element.
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

// Next returns the succeeding index of i.
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

// Len returns the length of the collection.
func (O *ObjectCollection) Len() (int, error) {
	var size C.int32_t
	if C.dpiObject_getSize(O.dpiObject, &size) == C.DPI_FAILURE {
		return 0, O.getError()
	}
	return int(size), nil
}

// Trim the collection to n.
func (O *ObjectCollection) Trim(n int) error {
	if C.dpiObject_trim(O.dpiObject, C.uint32_t(n)) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// ObjectType holds type info of an Object.
type ObjectType struct {
	dpiObjectType *C.dpiObjectType

	Schema, Name string
	CollectionOf *ObjectType

	OracleTypeNum                       C.dpiOracleTypeNum
	NativeTypeNum                       C.dpiNativeTypeNum
	DBSize, ClientSizeInBytes, CharSize int
	Precision                           int16
	Scale                               int8
	FsPrecision                         uint8

	Attributes []ObjectAttribute

	drv *drv
}

func (t ObjectType) getError() error { return t.drv.getError() }

func (t ObjectType) FullName() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// GetObjectType returns the ObjectType of a name.
func (c *conn) GetObjectType(name string) (ObjectType, error) {
	cName := C.CString(name)
	defer func() { C.free(unsafe.Pointer(cName)) }()
	objType := (*C.dpiObjectType)(C.malloc(C.sizeof_void))
	if C.dpiConn_getObjectType(c.dpiConn, cName, C.uint32_t(len(name)), &objType) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(objType))
		return ObjectType{}, errors.Wrapf(c.getError(), "getObjectType(%q) conn=%p", name, c.dpiConn)
	}
	t := ObjectType{drv: c.drv, dpiObjectType: objType}
	return t, t.init()
}

// NewObject returns a new Object with ObjectType type.
func (t ObjectType) NewObject() (*Object, error) {
	obj := (*C.dpiObject)(C.malloc(C.sizeof_void))
	if C.dpiObjectType_createObject(t.dpiObjectType, &obj) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(obj))
		return nil, t.getError()
	}
	return &Object{ObjectType: t, dpiObject: obj}, nil
}

func (t *ObjectType) init() error {
	if t.Name != "" && t.Attributes != nil {
		return nil
	}
	if t.dpiObjectType == nil {
		return nil
	}
	var info C.dpiObjectTypeInfo
	if C.dpiObjectType_getInfo(t.dpiObjectType, &info) == C.DPI_FAILURE {
		return t.getError()
	}
	t.Schema = C.GoStringN(info.schema, C.int(info.schemaLength))
	t.Name = C.GoStringN(info.name, C.int(info.nameLength))
	t.CollectionOf = nil
	numAttributes := int(info.numAttributes)

	if info.isCollection == 1 {
		t.CollectionOf = &ObjectType{drv: t.drv}
		if err := t.CollectionOf.fromDataTypeInfo(info.elementTypeInfo); err != nil {
			return err
		}
		if t.CollectionOf.Name == "" {
			t.CollectionOf.Schema = t.Schema
			t.CollectionOf.Name = t.Name
		}
	}
	if numAttributes == 0 {
		t.Attributes = []ObjectAttribute{}
		return nil
	}
	t.Attributes = make([]ObjectAttribute, numAttributes)
	attrs := make([]*C.dpiObjectAttr, len(t.Attributes))
	if C.dpiObjectType_getAttributes(t.dpiObjectType,
		C.uint16_t(len(attrs)),
		(**C.dpiObjectAttr)(unsafe.Pointer(&attrs[0])),
	) == C.DPI_FAILURE {
		return t.getError()
	}
	for i, attr := range attrs {
		var attrInfo C.dpiObjectAttrInfo
		if C.dpiObjectAttr_getInfo(attr, &attrInfo) == C.DPI_FAILURE {
			return t.getError()
		}
		typ := attrInfo.typeInfo
		sub, err := objectTypeFromDataTypeInfo(t.drv, typ)
		if err != nil {
			return err
		}
		t.Attributes[i] = ObjectAttribute{
			drv:           t.drv,
			dpiObjectAttr: attr,
			Name:          C.GoStringN(attrInfo.name, C.int(attrInfo.nameLength)),
			ObjectType:    sub,
		}
	}
	return nil
}

func (t *ObjectType) fromDataTypeInfo(typ C.dpiDataTypeInfo) error {
	t.dpiObjectType = typ.objectType

	t.OracleTypeNum = typ.oracleTypeNum
	t.NativeTypeNum = typ.defaultNativeTypeNum
	t.DBSize = int(typ.dbSizeInBytes)
	t.ClientSizeInBytes = int(typ.clientSizeInBytes)
	t.CharSize = int(typ.sizeInChars)
	t.Precision = int16(typ.precision)
	t.Scale = int8(typ.scale)
	t.FsPrecision = uint8(typ.fsPrecision)
	return t.init()
}
func objectTypeFromDataTypeInfo(drv *drv, typ C.dpiDataTypeInfo) (ObjectType, error) {
	t := ObjectType{drv: drv}
	err := t.fromDataTypeInfo(typ)
	return t, err
}

// ObjectAttribute is an attribute of an Object.
type ObjectAttribute struct {
	drv           *drv
	dpiObjectAttr *C.dpiObjectAttr
	Name          string
	ObjectType
}

// Close the ObjectAttribute.
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

// GetObjectType returns the ObjectType for the name.
func GetObjectType(ex Execer, typeName string) (ObjectType, error) {
	c, err := getConn(ex)
	if err != nil {
		return ObjectType{}, err
	}
	return c.GetObjectType(typeName)
}
