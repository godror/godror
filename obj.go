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
	*ObjectType
}

// GetAttribute gets the i-th attribute into data.
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

// SetAttribute sets the i-th attribute with data.
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
		data.NativeTypeNum = O.info.(objectCollectionInfo).NativeTypeNum()
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
		data.NativeTypeNum = O.info.(objectCollectionInfo).NativeTypeNum()
	}
	if C.dpiObject_getElementValueByIndex(O.dpiObject, idx, data.NativeTypeNum, data.dpiData) == C.DPI_FAILURE {
		return O.getError()
	}
	return nil
}

// Set the i-th element of the collection with data.
func (O *ObjectCollection) Set(i int, data *Data) error {
	if data.NativeTypeNum == 0 {
		data.NativeTypeNum = O.info.(objectCollectionInfo).NativeTypeNum()
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
	*drv
	dpiObjectType *C.dpiObjectType
	info          ObjectInfo
	attributes    []ObjectAttribute
}

// GetObjectType returns the ObjectType of a name.
func (c *conn) GetObjectType(name string) (*ObjectType, error) {
	cName := C.CString(name)
	defer func() { C.free(unsafe.Pointer(cName)) }()
	objType := (*C.dpiObjectType)(C.malloc(C.sizeof_void))
	if C.dpiConn_getObjectType(c.dpiConn, cName, C.uint32_t(len(name)), (**C.dpiObjectType)(unsafe.Pointer(&objType))) == C.DPI_FAILURE {
		C.free(unsafe.Pointer(objType))
		return nil, errors.Wrapf(c.getError(), "getObjectType(%q) conn=%p", name, c.dpiConn)
	}
	return &ObjectType{dpiObjectType: objType}, nil
}

// NewObject returns a new Object with ObjectType type.
func (t *ObjectType) NewObject() (*Object, error) {
	obj := Object{ObjectType: t}
	if C.dpiObjectType_createObject(t.dpiObjectType, (**C.dpiObject)(unsafe.Pointer(&obj.dpiObject))) == C.DPI_FAILURE {
		return nil, t.getError()
	}
	return &obj, nil
}

// Attributes of the ObjectType.
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
	t.attributes = make([]ObjectAttribute, info.NumAttributes())
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
		typ := attrInfo.typeInfo
		t.attributes[i] = ObjectAttribute{
			drv:           t.drv,
			dpiObjectAttr: attr,
			Name:          C.GoStringN(attrInfo.name, C.int(attrInfo.nameLength)),
			DataTypeInfo:  newDataTypeInfo(typ),
		}
	}
	return t.attributes, nil
}

func newDataTypeInfo(typ C.dpiDataTypeInfo) DataTypeInfo {
	dti := DataTypeInfo{OracleTypeNum: typ.oracleTypeNum,
		NativeTypeNum:     typ.defaultNativeTypeNum,
		DBSize:            int(typ.dbSizeInBytes),
		ClientSizeInBytes: int(typ.clientSizeInBytes),
		CharSize:          int(typ.sizeInChars),
		Precision:         int16(typ.precision),
		Scale:             int8(typ.scale),
		FsPrecision:       uint8(typ.fsPrecision),
	}
	if typ.objectType != nil {
		dti.ObjectType = &ObjectType{dpiObjectType: typ.objectType}
	}
	return dti
}

// Info returns ObjectInfo of the ObjectType.
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
	t.info = objectCollectionInfo{
		objectInfo:   oInfo,
		DataTypeInfo: newDataTypeInfo(info.elementTypeInfo),
	}
	return t.info, nil
}

// ObjectInfo holds Object type info.
type ObjectInfo interface {
	Schema() string
	Name() string
	NumAttributes() int
	IsCollection() bool
}

// ObjectCollectionInfo holds type info of the collection.
type ObjectCollectionInfo interface {
	OracleTypeNum() C.dpiOracleTypeNum
	NativeTypeNum() C.dpiNativeTypeNum
	ObjectType() *ObjectType
}

type objectCollectionInfo struct {
	objectInfo
	DataTypeInfo
}

// OracleTypeNum returns the Oracle Type of the elements of the collection.
func (c objectCollectionInfo) OracleTypeNum() C.dpiOracleTypeNum { return c.DataTypeInfo.OracleTypeNum }

// NativeTypeNum returns the native type number of the elements of the collection.
func (c objectCollectionInfo) NativeTypeNum() C.dpiNativeTypeNum { return c.DataTypeInfo.NativeTypeNum }

// ObjectType of the collection.
func (c objectCollectionInfo) ObjectType() *ObjectType { return c.DataTypeInfo.ObjectType }

// IsCollection returns whether the object is a collection.
func (c objectCollectionInfo) IsCollection() bool { return true }

type objectInfo struct {
	schema, name  string
	numAttributes int
}

func (i objectInfo) Schema() string     { return i.schema }
func (i objectInfo) Name() string       { return i.name }
func (i objectInfo) NumAttributes() int { return i.numAttributes }
func (i objectInfo) IsCollection() bool { return false }

// ObjectAttribute is an attribute of an Object.
type ObjectAttribute struct {
	*drv
	dpiObjectAttr *C.dpiObjectAttr
	Name          string
	DataTypeInfo
}

// DataTypeInfo holds type info for a Data.
type DataTypeInfo struct {
	OracleTypeNum                       C.dpiOracleTypeNum
	NativeTypeNum                       C.dpiNativeTypeNum
	ObjectType                          *ObjectType
	DBSize, ClientSizeInBytes, CharSize int
	Precision                           int16
	Scale                               int8
	FsPrecision                         uint8
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
func GetObjectType(ex Execer, typeName string) (*ObjectType, error) {
	c, err := getConn(ex)
	if err != nil {
		return nil, err
	}
	return c.GetObjectType(typeName)
}
