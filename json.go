package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"

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

int godror_dpiJson_setInt64(dpiJsonNode *topNode, double value) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_INT64;
    topNode->value->asDouble = value;
}

int godror_dpiJson_setTime(dpiJsonNode *topNode, dpiData *data) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_TIMESTAMP;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP;
    topNode->value->asTimestamp = data->value.asTimestamp;
}

int godror_dpiJson_setBool(dpiJsonNode *topNode, dpiData *data) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN;
    topNode->value->asBoolean = data->value.asBoolean;
}

int godror_dpiJson_setString(dpiJsonNode *topNode, const char *value, uint32_t keyLength) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    topNode->value->asBytes.ptr = strndup(value, keyLength);
    topNode->value->asBytes.length = keyLength;
}

int godror_dpiJson_setFloat64(dpiJsonNode *topNode, float value) {
    topNode->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    topNode->nativeTypeNum = DPI_NATIVE_TYPE_FLOAT;
    topNode->value->asFloat = value;
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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type JsonValue interface {
	//	Unmarshal(out interface{}) (err error)
	//	Marshal(in interface{}) error
}

type JSONString string

// JSON holds the JSON data to/from Oracle.
type JSON struct {
	dpiJson     *C.dpiJson
	dpiJsonNode *C.dpiJsonNode
}
type JSONOption uint8

var ErrSthHappened = errors.New("something happened")

/*
 * if Default is passed and we have json element "age":25 we
 * get back age as double as data type in data.Get() ,
 * if numberasstring is passed, we get age value as bytes
 */
const (
	JSONOptDefault        = JSONOption(C.DPI_JSON_OPT_DEFAULT)
	JSONOptNumberAsString = JSONOption(C.DPI_JSON_OPT_NUMBER_AS_STRING)
	JSONOptDateAsDouble   = JSONOption(C.DPI_JSON_OPT_DATE_AS_DOUBLE)
)

func (j JSON) Get(data *Data, opts JSONOption) error {
	var node *C.dpiJsonNode
	if C.dpiJson_getValue(j.dpiJson, C.uint32_t(opts), (**C.dpiJsonNode)(unsafe.Pointer(&node))) == C.DPI_FAILURE {
		return ErrSthHappened
	}
	jsonNodeToData(data, node)
	j.dpiJsonNode = node
	return nil
}

func (j JSON) GetJsonObject(jsobj *JSONObject, opts JSONOption) error {
	var datajsobj Data
	err := j.Get(&datajsobj, opts)
	*jsobj = JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(datajsobj.dpiData))}
	return err
}

func (j JSON) GetJsonArray(jsarr *JSONArray, opts JSONOption) error {
	var datajsobj Data
	err := j.Get(&datajsobj, opts)
	*jsarr = JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(datajsobj.dpiData))}
	return err
}

func (j JSON) String() string {
	var cBuf *C.char
	cBuf = C.CString(strings.Repeat("0", 1024))
	defer C.free(unsafe.Pointer(cBuf))

	var cLen C.uint64_t
	cLen = C.uint64_t(1024) // tbd remove hardcoding
	if C.dpiJson_jsonToTextBuffer(j.dpiJson, cBuf, &cLen) ==
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

type JSONArray struct {
	dpiJsonArray *C.dpiJsonArray
	dpiJsonNode  *C.dpiJsonNode
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

func (j JSONArray) GetUserArray() (nodes []interface{}, err error) {
	n := int(j.dpiJsonArray.numElements)
	elts := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonArray.elements)))[:n:n]
	for i := 0; i < n; i++ {
		var d Data
		jsonNodeToData(&d, &elts[i])
		if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_OBJECT {

			jsobj := JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(d.dpiData))}
			m, err := jsobj.GetUserMap()
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, m)
		} else if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_ARRAY {
			jsarr := JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(d.dpiData))}
			ua, err := jsarr.GetUserArray()
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, ua)
		} else {
			if elts[i].oracleTypeNum == C.DPI_ORACLE_TYPE_VARCHAR {
				keyval, err := d.getString()
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

func (jsarr *JSONArray) Close() error {
	C.godror_dpiJsonfreeMem(jsarr.dpiJsonNode)
	return nil
}

type JSONObject struct {
	dpiJsonObject *C.dpiJsonObject
	// tbd null check for dpijsonnode
	dpiJsonNode *C.dpiJsonNode
}

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
		C.godror_dpiJson_setInt64(jsonnode, C.double(x))
	case float64:
		C.godror_dpiJson_setDouble(jsonnode, C.double(x))
		//C.godror_dpiJson_setFloat64(jsonnode, C.float(x))
	case string:
		cval := C.CString(x)
		C.godror_dpiJson_setString(jsonnode, cval, C.uint32_t(len(x)))
		C.free(unsafe.Pointer(cval))
	case time.Time:
		data, err := NewData(x)
		if err != nil {
			return err
		}
		C.godror_dpiJson_setTime(jsonnode, &(data.dpiData))
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
	*jsobj = JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(dpidataw.dpiData)), dpiJsonNode: dpijsonnode}
	return nil
}

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

func (j JSONObject) GetUserMap() (m map[string]interface{}, err error) {
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
			um, err := jsobj.GetUserMap()
			if err != nil {
				return nil, err
			}
			m[C.GoStringN(names[i], C.int(nameLengths[i]))] = um
		} else if d.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_ARRAY {
			jsobj := JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(d.dpiData))}
			ua, err := jsobj.GetUserArray()
			if err != nil {
				return nil, err
			}
			m[C.GoStringN(names[i], C.int(nameLengths[i]))] = ua
		} else if fields[i].oracleTypeNum == C.DPI_ORACLE_TYPE_VARCHAR {
			keyval, err := d.getString()
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

func (t Timestamp) MarshalJSON() ([]byte, error) {

	str := "{\"$oracleTimestampTZ\":\"" + time.Time(t).Format(time.RFC3339Nano) + "\"}"
	return []byte(str), nil
}

func (t IntervalYMJson) MarshalJSON() ([]byte, error) {
	str := "{\"$intervalYearMonth\":\"" + t + "\"}"
	return []byte(str), nil
}

func NewJsonValue(in interface{}) (JsonValue, error) {
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
	case reflect.Struct:
		// Add Json Map
		outByte, _ := json.Marshal(&in)
		return JSONString(outByte), nil
	case reflect.Ptr:
		// Add Json Map
		var jsonobj JSONObject
		newJSONObject(in.(map[string]interface{}), &jsonobj)
		return jsonobj, nil
	case reflect.String:
		return JSONString(in.(string)), nil
	case reflect.Slice:
		var jsonarr JSONArray
		newJSONArray(in.([]interface{}), &jsonarr)
		return jsonarr, nil
	default:
		return nil, fmt.Errorf("Unsupported JSON doc type %#v: ", t.Name())
	}
}
