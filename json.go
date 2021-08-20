package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"

// All CGO functions are used because handling unions, indexing,
// nested mallocs for substructures is hard to handle in Go code


int godror_allocate_dpiNode(dpiJsonNode **dpijsonnode) {
	*dpijsonnode = (dpiJsonNode *)(malloc(sizeof(dpiJsonNode)));
	dpiDataBuffer *dpijsonDataBuffer = (dpiDataBuffer *)(malloc(sizeof(dpiDataBuffer)));
	(*dpijsonnode)->value = dpijsonDataBuffer;
}

int godror_free_dpiNode(dpiJsonNode *node) {
    free(node->value);
    node->value = NULL;
    free(node);
    node = NULL;
}

void godror_setObjectFields(dpiJsonObject * jsonobj, int i, dpiJsonNode **jnode)
{
    *jnode = &(jsonobj->fields[i]);
    jsonobj->fields[i].value = &jsonobj->fieldValues[i];
}

int godror_dpiJsonObject_setKey(dpiJsonNode *dpijsonnode, int index, const char *key, uint32_t keyLength) {
    dpiJsonObject *dpijsonobj = &(dpijsonnode->value->asJsonObject);
    dpijsonobj->fieldNames[index] = malloc(sizeof(char) * (keyLength + 1));
    memcpy(dpijsonobj->fieldNames[index], key, keyLength);
    dpijsonobj->fieldNames[index][keyLength] = '\0';
    dpijsonobj->fieldNameLengths[index] = keyLength;
}

int godror_dpiasJsonObject(dpiJsonNode *dpijsonnode, dpiJsonObject **dpijsonobj)
{
    *dpijsonobj = &(dpijsonnode->value->asJsonObject);
}

int godror_dpiasJsonArray(dpiJsonNode *dpijsonnode, dpiJsonArray **dpijsonobj)
{
    *dpijsonobj = &(dpijsonnode->value->asJsonArray);
}

void godror_setArrayElements(dpiJsonArray * jsonarr, int i, dpiJsonNode **jnode)
{
    *jnode = &(jsonarr->elements[i]);
    jsonarr->elements[i].value = &jsonarr->elementValues[i];
}

int godror_dpiJson_setDouble(dpiJsonNode *topNode, double value) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE;
    topNode->value->asDouble = value;
}

int godror_dpiJson_setInt64(dpiJsonNode *topNode, int64_t value) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_INT64;
    topNode->value->asInt64 = value;
}

int godror_dpiJson_setUint64(dpiJsonNode *topNode, uint64_t value) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_UINT64;
    topNode->value->asUint64 = value;
}

int godror_dpiJson_setTime(dpiJsonNode *topNode, dpiData *data) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_TIMESTAMP;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP;
    topNode->value->asTimestamp = data->value.asTimestamp;
}

int godror_dpiJson_setBytes(dpiJsonNode *topNode, dpiData *data) {
    uint32_t size = data->value.asBytes.length;
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_RAW;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    topNode->value->asBytes.ptr = malloc(size);
    memcpy(topNode->value->asBytes.ptr, data->value.asBytes.ptr, size);
    topNode->value->asBytes.length = size;
}

int godror_dpiJson_setIntervalDS(dpiJsonNode *topNode, dpiData *data) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_DS;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_DS;
    topNode->value->asIntervalDS = data->value.asIntervalDS;
}

int godror_dpiJson_setBool(dpiJsonNode *topNode, dpiData *data) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN;
    topNode->value->asBoolean = data->value.asBoolean;
}

int godror_dpiJson_setString(dpiJsonNode *topNode, dpiData *data) {
    uint32_t size = data->value.asBytes.length;
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    // make a copy before passing to C?
    topNode->value->asBytes.ptr = malloc(size);
    memcpy(topNode->value->asBytes.ptr, data->value.asBytes.ptr, size);
    topNode->value->asBytes.length = size;
}

int godror_dpiJsonObject_initialize(dpiJsonNode **dpijsonnode, uint32_t numfields) {
    dpiJsonObject dpijsonobjtmp;
    dpiJsonObject *dpijsonobj = &dpijsonobjtmp;
    (*dpijsonnode)->oracleTypeNum = DPI_ORACLE_TYPE_JSON_OBJECT;
    (*dpijsonnode)->nativeTypeNum = DPI_NATIVE_TYPE_JSON_OBJECT;
    dpijsonobj->fieldNames = (malloc(numfields * sizeof(char *)));
    dpijsonobj->fields = (dpiJsonNode *)(malloc(numfields * sizeof(dpiJsonNode)));
    dpijsonobj->fieldNameLengths = malloc(numfields * sizeof(uint32_t));
    dpijsonobj->fieldValues = (dpiDataBuffer *)malloc(numfields * sizeof(dpiDataBuffer));
	dpijsonobj->numFields = numfields;
    (*dpijsonnode)->value->asJsonObject = *dpijsonobj;
    return 0;
}

int godror_dpiJsonArray_initialize(dpiJsonNode **dpijsonnode, uint32_t numelem) {
    dpiJsonArray dpijsonarrtmp;
    dpiJsonArray *dpijsonarr = &dpijsonarrtmp;
    (*dpijsonnode)->oracleTypeNum = DPI_ORACLE_TYPE_JSON_ARRAY;
    (*dpijsonnode)->nativeTypeNum = DPI_NATIVE_TYPE_JSON_ARRAY;
    dpijsonarr->elements = malloc(numelem * sizeof(dpiJsonNode));
    dpijsonarr->elementValues = (dpiDataBuffer *)malloc(numelem * sizeof(dpiDataBuffer));
	dpijsonarr->numElements = numelem;
    (*dpijsonnode)->value->asJsonArray = *dpijsonarr;
    return 0;
}

void godror_dpiJsonNodeFree(dpiJsonNode *node)
{
    dpiJsonArray *array;
    dpiJsonObject *obj;
    uint32_t i;

    if (node == NULL)
    {
        return;
    }

    switch (node->nativeTypeNum) {
        case DPI_NATIVE_TYPE_BYTES:
            if(node->value->asBytes.ptr) {
                free(node->value->asBytes.ptr);
                node->value->asBytes.ptr = NULL;
            }
            break;
        case DPI_NATIVE_TYPE_JSON_ARRAY:
            array = &node->value->asJsonArray;
            if (array->elements) {
                for (i = 0; i < array->numElements; i++) {
                    if (array->elements[i].value) {
                        godror_dpiJsonNodeFree(&array->elements[i]);
                    }
                }
                free(array->elements);
                array->elements = NULL;
            }
            if (array->elementValues) {
                free(array->elementValues);
                array->elementValues = NULL;
            }
            break;
        case DPI_NATIVE_TYPE_JSON_OBJECT:
            obj = &node->value->asJsonObject;
            if (obj->fields) {
                for (i = 0; i < obj->numFields; i++) {
                    if (obj->fields[i].value)
                        godror_dpiJsonNodeFree(&obj->fields[i]);
                }
                free(obj->fields);
                obj->fields = NULL;
            }
            if (obj->fieldNames) {
                for (i = 0; i < obj->numFields; i++) {
                    if (obj->fieldNames[i]) {
                        free(obj->fieldNames[i]);
                        obj->fieldNames[i] = NULL;
                    }
                }
                free(obj->fieldNames);
                obj->fieldNames = NULL;
            }
            if (obj->fieldNameLengths) {
                free(obj->fieldNameLengths);
                obj->fieldNameLengths = NULL;
            }
            if (obj->fieldValues) {
                free(obj->fieldValues);
                obj->fieldValues = NULL;
            }
            break;
    }
}

void godror_dpiJsonfreeMem(dpiJsonNode *node) {
    godror_dpiJsonNodeFree(node);
    godror_free_dpiNode(node);
}

*/
import "C"

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type JSONOption uint8

var ErrInvalidJSON = errors.New("Invalid JSON Document")
var ErrInvalidType = errors.New("Invalid JSON Scalar Type")

const (
	JSONOptDefault        = JSONOption(C.DPI_JSON_OPT_DEFAULT)
	JSONOptNumberAsString = JSONOption(C.DPI_JSON_OPT_NUMBER_AS_STRING)
	JSONOptDateAsDouble   = JSONOption(C.DPI_JSON_OPT_DATE_AS_DOUBLE)
)

const (
	MAXVARCHAR2 = 32767 // to be removed. only for testing
)

// Represents JSON string format, if its in std, ORACLE extended types,
// BSON extended types, etc
type JSONStringFlags uint

const (
	JSONFormatExtnTypes       JSONStringFlags = C.DPI_JSON_USE_EXTENSION_TYPES
	JSONFormatBSONTypes                       = C.DPI_JSON_BSON_TYPE_PATTERNS
	JSONFormatBSONTypePattern                 = C.DPI_JSON_USE_BSON_TYPES
)

type JSONString struct {
	Flags JSONStringFlags // standard , extended types for OSON, BSON
	Value string          // JSON input
}

// JSONValue is the interface implemented by structs encapsulating
// various JSON inputs.
// JSON, JSONObject, JSONArray and JSONScalar implement it.
type JSONValue interface {
	// JSONType is native type stored.
	GetJSONType() C.dpiNativeTypeNum
}

// JSONScalar holds the JSON data to/from Oracle.
// It includes all scalar values such as int, float, bool, time.Time,
// time.Duration, byte[] , map and arrays.
type JSONScalar struct {
	dpiJsonNode *C.dpiJsonNode
	isMemOwned  bool // true indicates associated C memory needs to be freed.
}

func (j JSONScalar) GetValue() (val interface{}, err error) {
	var d Data
	jsonNodeToData(&d, j.dpiJsonNode)
	val = d.Get()
	if j.dpiJsonNode.oracleTypeNum == C.DPI_ORACLE_TYPE_VARCHAR {
		val = string((d.Get()).([]byte))
	}
	err = nil
	return
}

func (j JSONScalar) GetJSONType() C.dpiNativeTypeNum {
	var d Data
	jsonNodeToData(&d, j.dpiJsonNode)
	return d.NativeTypeNum
}

func (js *JSONScalar) Close() error {
	if js.isMemOwned {
		C.godror_dpiJsonfreeMem(js.dpiJsonNode)
	}
	return nil
}

// JSON holds the JSON data to/from Oracle.
// It is like a root node in JSON tree.
type JSON struct {
	dpiJson       *C.dpiJson
	isMemOwned    bool // true indicates associated C memory needs to be freed.
	nativeTypeNum C.dpiNativeTypeNum
}

// During Fetch the JSON struct is created.
// Explicit creation is not supported.
func createJSON(js *C.dpiJson, mutable bool) (JSON, error) {
	ntype := C.dpiNativeTypeNum(js.topNode.nativeTypeNum)
	json := JSON{dpiJson: js, isMemOwned: mutable, nativeTypeNum: ntype}
	return json, nil
}

func (j JSON) GetJSONType() C.dpiNativeTypeNum {
	return j.nativeTypeNum
}

func (j JSON) Get(data *Data, opts JSONOption) error {
	var node *C.dpiJsonNode
	if C.dpiJson_getValue(j.dpiJson, C.uint32_t(opts), (**C.dpiJsonNode)(unsafe.Pointer(&node))) == C.DPI_FAILURE {
		return ErrInvalidJSON
	}
	jsonNodeToData(data, node)
	return nil
}

// Returns JSONObject from JSON
func (j JSON) GetJSONObject(jsobj *JSONObject, opts JSONOption) error {
	var node *C.dpiJsonNode
	var d Data
	if C.dpiJson_getValue(j.dpiJson, C.uint32_t(opts), (**C.dpiJsonNode)(unsafe.Pointer(&node))) == C.DPI_FAILURE {
		return ErrInvalidJSON
	}
	jsonNodeToData(&d, node)
	if C.dpiNativeTypeNum(node.nativeTypeNum) != C.DPI_NATIVE_TYPE_JSON_OBJECT {
		return ErrInvalidType
	}

	*jsobj = JSONObject{dpiJsonNode: node,
		dpiJsonObject: C.dpiData_getJsonObject(&(d.dpiData)),
		isMemOwned:    j.isMemOwned}
	return nil
}

// Returns JSONArray from JSON
func (j JSON) GetJSONArray(jsarr *JSONArray, opts JSONOption) error {
	var node *C.dpiJsonNode
	if C.dpiJson_getValue(j.dpiJson, C.uint32_t(opts), (**C.dpiJsonNode)(unsafe.Pointer(&node))) == C.DPI_FAILURE {
		return ErrInvalidJSON
	}
	var d Data
	jsonNodeToData(&d, node)
	if C.dpiNativeTypeNum(node.nativeTypeNum) != C.DPI_NATIVE_TYPE_JSON_ARRAY {
		return ErrInvalidType
	}
	*jsarr = JSONArray{dpiJsonNode: node,
		dpiJsonArray: C.dpiData_getJsonArray(&(d.dpiData)),
		isMemOwned:   j.isMemOwned}
	return nil
}

// Returns JSONScalar from JSON
func (j JSON) GetJSONScalar(js *JSONScalar, opts JSONOption) error {
	var node *C.dpiJsonNode
	if C.dpiJson_getValue(j.dpiJson, C.uint32_t(opts), (**C.dpiJsonNode)(unsafe.Pointer(&node))) == C.DPI_FAILURE {
		return ErrInvalidJSON
	}
	*js = JSONScalar{dpiJsonNode: node,
		isMemOwned: j.isMemOwned}
	return nil
}

// Returns JSON formatted string. flags control extended attributes
func (j JSON) ToJSONString(flags JSONStringFlags) (string, error) {
	var cBuf *C.char
	cBuf = C.CString(strings.Repeat("0", MAXVARCHAR2))
	defer C.free(unsafe.Pointer(cBuf))

	var cLen C.uint64_t
	cLen = C.uint64_t(MAXVARCHAR2) // tbd remove hardcoding
	if C.dpiJson_setToText(j.dpiJson, cBuf, &cLen, C.uint(flags)) ==
		C.DPI_FAILURE {
		return "", ErrInvalidJSON
	}
	jdstr := C.GoStringN(cBuf, C.int(cLen))
	return jdstr, nil
}

// Returns JSON formatted standard string
func (j JSON) String() string {
	var cBuf *C.char
	cBuf = C.CString(strings.Repeat("0", MAXVARCHAR2))
	defer C.free(unsafe.Pointer(cBuf))

	var jsonFlags uint = 0
	var cLen C.uint64_t
	cLen = C.uint64_t(MAXVARCHAR2) // tbd remove hardcoding
	if C.dpiJson_setToText(j.dpiJson, cBuf, &cLen, C.uint(jsonFlags)) ==
		C.DPI_FAILURE {
		return ""
	}
	jdstr := C.GoStringN(cBuf, C.int(cLen))
	return jdstr
}

func jsonNodeToData(data *Data, node *C.dpiJsonNode) {
	if node.value == nil {
		data.dpiData.isNull = 1
		return
	}
	data.dpiData.value = *node.value
	data.NativeTypeNum = node.nativeTypeNum
}

// It represents the array input.
type JSONArray struct {
	dpiJsonNode  *C.dpiJsonNode
	dpiJsonArray *C.dpiJsonArray
	isMemOwned   bool // true indicates associated C memory needs to be freed
}

func (j JSONArray) GetJSONType() C.dpiNativeTypeNum {
	return C.DPI_NATIVE_TYPE_JSON_ARRAY
}

func (j JSONArray) Len() int { return int(j.dpiJsonArray.numElements) }
func (j JSONArray) GetElement(i int) Data {
	n := int(j.dpiJsonArray.numElements)
	elts := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonArray.elements)))[:n:n]
	var d Data
	jsonNodeToData(&d, &elts[i])
	return d

}

func (j JSONArray) Get(nodes []Data) []Data {
	n := int(j.dpiJsonArray.numElements)
	elts := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonArray.elements)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &elts[i])
		nodes = append(nodes, d)
	}
	return nodes
}

// Returns the Go type, []interface{} from JSONArray
func (j JSONArray) GetValue() (nodes []interface{}, err error) {
	n := int(j.dpiJsonArray.numElements)
	elts := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonArray.elements)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &elts[i])
		if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_OBJECT {

			jsobj := JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(d.dpiData))}
			m, err := jsobj.GetValue()
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, m)
		} else if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_ARRAY {
			jsarr := JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(d.dpiData))}
			ua, err := jsarr.GetValue()
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, ua)
		} else {
			if elts[i].oracleTypeNum == C.DPI_ORACLE_TYPE_VARCHAR {
				keyval, err := d.getStringFromDPIBytes()
				if err == nil {
					nodes = append(nodes, keyval)
				} else {
					return nil, err
				}
			} else {
				nodes = append(nodes, d.Get())
			}
		}
	}
	return nodes, nil
}

// Frees the C memory allocated
func (jsarr *JSONArray) Close() error {
	if jsarr.isMemOwned {
		C.godror_dpiJsonfreeMem(jsarr.dpiJsonNode)
	}
	return nil
}

// It represents the map input.
type JSONObject struct {
	dpiJsonNode   *C.dpiJsonNode
	dpiJsonObject *C.dpiJsonObject
	isMemOwned    bool
}

func (j JSONObject) GetJSONType() C.dpiNativeTypeNum {
	return C.DPI_NATIVE_TYPE_JSON_OBJECT
}

// populates dpiJsonNode from user inputs.
// It creates a seperate memory for the new output value, jsonnode.
// memory from user input, in is not shared with jsonnode.
// Caller has to explicitly free using godror_dpiJsonfreeMem
func populateJsonNode(in interface{}, jsonnode *C.dpiJsonNode) error {
	switch x := in.(type) {
	case []interface{}:
		arr, _ := in.([]interface{})
		C.godror_dpiJsonArray_initialize((**C.dpiJsonNode)(unsafe.Pointer(&jsonnode)), C.uint32_t(len(arr)))

		var dpijsonarr *C.dpiJsonArray
		C.godror_dpiasJsonArray(jsonnode, (**C.dpiJsonArray)(unsafe.Pointer(&dpijsonarr)))
		for index, entry := range arr {
			var jsonnodelocal *C.dpiJsonNode
			C.godror_setArrayElements(dpijsonarr, C.int(index), (**C.dpiJsonNode)(unsafe.Pointer(&jsonnodelocal)))
			err := populateJsonNode(entry, jsonnodelocal)
			if err != nil {
				return err
			}
		}
	case map[string]interface{}:
		m, _ := in.(map[string]interface{})
		// Initialize dpiJsonObjectNode
		C.godror_dpiJsonObject_initialize((**C.dpiJsonNode)(unsafe.Pointer(&jsonnode)), C.uint32_t(len(m)))

		var dpijsonobj *C.dpiJsonObject
		C.godror_dpiasJsonObject(jsonnode, (**C.dpiJsonObject)(unsafe.Pointer(&dpijsonobj)))

		var i C.int = 0
		var cKey *C.char

		for k, v := range m {
			cKey = C.CString(k)
			C.godror_dpiJsonObject_setKey(jsonnode, i, cKey, C.uint32_t(len(k)))
			var jsonnodelocal *C.dpiJsonNode
			C.free(unsafe.Pointer(cKey))
			C.godror_setObjectFields(dpijsonobj, i, (**C.dpiJsonNode)(unsafe.Pointer(&jsonnodelocal)))
			err := populateJsonNode(v, jsonnodelocal)
			if err != nil {
				return err
			}
			i = i + 1
		}

	case int:
		C.godror_dpiJson_setInt64(jsonnode, C.int64_t(x))
	case int8:
		C.godror_dpiJson_setInt64(jsonnode, C.int64_t(x))
	case int16:
		C.godror_dpiJson_setInt64(jsonnode, C.int64_t(x))
	case int32:
		C.godror_dpiJson_setInt64(jsonnode, C.int64_t(x))
	case int64:
		C.godror_dpiJson_setInt64(jsonnode, C.int64_t(x))
	case uint:
		C.godror_dpiJson_setUint64(jsonnode, C.uint64_t(x))
	case uint8:
		C.godror_dpiJson_setUint64(jsonnode, C.uint64_t(x))
	case uint16:
		C.godror_dpiJson_setUint64(jsonnode, C.uint64_t(x))
	case uint32:
		C.godror_dpiJson_setUint64(jsonnode, C.uint64_t(x))
	case uint64:
		C.godror_dpiJson_setUint64(jsonnode, C.uint64_t(x))
	case float32:
		C.godror_dpiJson_setDouble(jsonnode, C.double(x))
	case float64:
		C.godror_dpiJson_setDouble(jsonnode, C.double(x))
	case string:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setString(jsonnode, &(data.dpiData))
	case time.Time:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setTime(jsonnode, &(data.dpiData))
	case time.Duration:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setIntervalDS(jsonnode, &(data.dpiData))
	case []byte:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setBytes(jsonnode, &(data.dpiData))
	case bool:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setBool(jsonnode, &(data.dpiData))
	default:
		return fmt.Errorf("Unsupported type %T\n", in)
	}
	return nil
}

// Creates a JSONInt from user input []interface{}
func newJSONScalar(val interface{}, jscalar *JSONScalar) error {
	var dpijsonnode *C.dpiJsonNode
	C.godror_allocate_dpiNode((**C.dpiJsonNode)(unsafe.Pointer(&dpijsonnode)))
	err := populateJsonNode(val, dpijsonnode)
	if err != nil {
		C.godror_dpiJsonfreeMem(dpijsonnode)
		return err
	}
	dpidataw := new(Data)
	jsonNodeToData(dpidataw, dpijsonnode)
	*jscalar = JSONScalar{dpiJsonNode: dpijsonnode, isMemOwned: true}
	return nil
}

// Creates a JSONArray from user input []interface{}
func newJSONArray(arr []interface{}, jsarr *JSONArray) error {
	var dpijsonnode *C.dpiJsonNode
	C.godror_allocate_dpiNode((**C.dpiJsonNode)(unsafe.Pointer(&dpijsonnode)))
	err := populateJsonNode(arr, dpijsonnode)
	if err != nil {
		C.godror_dpiJsonfreeMem(dpijsonnode)
		return err
	}
	dpidataw := new(Data)
	jsonNodeToData(dpidataw, dpijsonnode)
	*jsarr = JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(dpidataw.dpiData)), dpiJsonNode: dpijsonnode}
	return nil
}

// Creates a JSONObject from user input map[string]interface{}
func newJSONObject(m map[string]interface{}, jsobj *JSONObject) error {
	var dpijsonnode *C.dpiJsonNode
	C.godror_allocate_dpiNode((**C.dpiJsonNode)(unsafe.Pointer(&dpijsonnode)))
	err := populateJsonNode(m, dpijsonnode)
	if err != nil {
		C.godror_dpiJsonfreeMem(dpijsonnode)
		return err
	}
	dpidataw := new(Data)
	jsonNodeToData(dpidataw, dpijsonnode)
	*jsobj = JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(dpidataw.dpiData)), dpiJsonNode: dpijsonnode, isMemOwned: true}
	return nil
}

// Frees the C memory associated with JSONObject
func (jsobj *JSONObject) Close() error {
	C.godror_dpiJsonfreeMem(jsobj.dpiJsonNode)
	return nil
}

func (j JSONObject) Len() int { return int(j.dpiJsonObject.numFields) }
func (j JSONObject) Get(m map[string]Data) {
	n := int(j.dpiJsonObject.numFields)
	names := ((*[maxArraySize]*C.char)(unsafe.Pointer(j.dpiJsonObject.fieldNames)))[:n:n]
	nameLengths := ((*[maxArraySize]C.uint32_t)(unsafe.Pointer(j.dpiJsonObject.fieldNameLengths)))[:n:n]
	fields := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonObject.fields)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &fields[i])
		m[C.GoStringN(names[i], C.int(nameLengths[i]))] = d
	}
}

// Returns the Go type map[string]interface{} from JSONObject
func (j JSONObject) GetValue() (m map[string]interface{}, err error) {
	m = make(map[string]interface{})
	n := int(j.dpiJsonObject.numFields)
	names := ((*[maxArraySize]*C.char)(unsafe.Pointer(j.dpiJsonObject.fieldNames)))[:n:n]
	nameLengths := ((*[maxArraySize]C.uint32_t)(unsafe.Pointer(j.dpiJsonObject.fieldNameLengths)))[:n:n]
	fields := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonObject.fields)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &fields[i])
		if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_OBJECT {
			jsobj := JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(d.dpiData))}
			um, err := jsobj.GetValue()
			if err != nil {
				return nil, err
			}
			m[C.GoStringN(names[i], C.int(nameLengths[i]))] = um
		} else if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_ARRAY {
			jsobj := JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(d.dpiData))}
			ua, err := jsobj.GetValue()
			if err != nil {
				return nil, err
			}
			m[C.GoStringN(names[i], C.int(nameLengths[i]))] = ua
		} else if fields[i].oracleTypeNum == C.DPI_ORACLE_TYPE_VARCHAR {
			keyval, err := d.getStringFromDPIBytes()
			if err == nil {
				m[C.GoStringN(names[i], C.int(nameLengths[i]))] = keyval
			} else {
				return nil, err
			}
		} else {
			m[C.GoStringN(names[i], C.int(nameLengths[i]))] = d.Get()
		}
	}
	return m, nil
}

func (j JSONObject) GetInto(v interface{}) {
	rv := reflect.ValueOf(v).Elem()
	n := int(j.dpiJsonObject.numFields)
	names := ((*[maxArraySize]*C.char)(unsafe.Pointer(j.dpiJsonObject.fieldNames)))[:n:n]
	nameLengths := ((*[maxArraySize]C.uint32_t)(unsafe.Pointer(j.dpiJsonObject.fieldNameLengths)))[:n:n]
	fields := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonObject.fields)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &fields[i])
		rv.FieldByName(C.GoStringN(names[i], C.int(nameLengths[i]))).Set(reflect.ValueOf(d.Get()))
	}
}

// Converts Timestamp to compatible eJSON string
func (t Timestamp) MarshalJSON() ([]byte, error) {

	str := "{\"$oracleTimestampTZ\":\"" + time.Time(t).Format(time.RFC3339Nano) + "\"}"
	return []byte(str), nil
}

// Converts IntervalYMJson to compatible eJSON string
func (t IntervalYMJson) MarshalJSON() ([]byte, error) {
	str := "{\"$intervalYearMonth\":\"" + t + "\"}"
	return []byte(str), nil
}

// NewJSONValue will take user input and returns an interface which
// abstracts one of these: JSONObject, JSONArray, JSONArray, ...
func NewJSONValue(in interface{}) (JSONValue, error) {
	v := reflect.ValueOf(in)
	t := v.Type()
	switch t.Kind() {
	case reflect.Map:
		// Add Json Map
		//	outByte, _ := json.Marshal(&in)
		//	return JSONString(outByte), nil
		var jsonobj JSONObject
		newJSONObject(in.(map[string]interface{}), &jsonobj)
		return jsonobj, nil
	case reflect.Ptr:
		// Add Json Map
		var jsonobj JSONObject
		newJSONObject(in.(map[string]interface{}), &jsonobj)
		return jsonobj, nil
	case reflect.String:
		var jsonscl JSONScalar
		//newJSONScalar(in.(int8), &jsonscl)
		newJSONScalar(in, &jsonscl)
		return jsonscl, nil
	case reflect.Slice:
		var jsonarr JSONArray
		newJSONArray(in.([]interface{}), &jsonarr)
		return jsonarr, nil
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		var jsonscl JSONScalar
		newJSONScalar(in, &jsonscl)
		return jsonscl, nil
	default:
		return nil, fmt.Errorf("Unsupported JSON doc type %#v: ", t.Name())
	}
}
