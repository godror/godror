package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"

int godror_allocate_dpiNode(dpiJsonNode **dpijsonnode) {
	*dpijsonnode = (dpiJsonNode *)(malloc(sizeof(dpiJsonNode)));
	dpiDataBuffer *dpijsonDataBuffer = (dpiDataBuffer *)(malloc(sizeof(dpiDataBuffer)));
	(*dpijsonnode)->value = dpijsonDataBuffer;
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
    // tbd free this ptr
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
    free(node->value);
    node->value = NULL;
    free(node);
    node = NULL;
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

type JsonString string

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
	// dpiJsonNode gets freed along with j.dpiJson
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
	var objmap = make(map[string]Data)
	jsobj.Get(objmap)
	return err
}

func (j JSON) UnmarshalJsonNode(m map[string]interface{}) {
	// if it has object
	dec := json.NewDecoder(strings.NewReader(j.String()))
	dec.Decode(&m)
}

func (j JSON) String() string {
	var cBuf *C.char
	cBuf = C.CString(strings.Repeat("0", 1024))
	defer C.free(unsafe.Pointer(cBuf))

	var cLen C.uint64_t
	cLen = C.uint64_t(1024)
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
			populateJsonNode(entry, jsonnodelocal)
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
			populateJsonNode(v, jsonnodelocal)
			i = i + 1
		}

	case int:
		C.godror_dpiJson_setDouble(jsonnode, C.double(x))
	case float64:
		C.godror_dpiJson_setDouble(jsonnode, C.double(x))
		//C.godror_dpiJson_setFloat64(jsonnode, C.float(x))
	case string:
		cval := C.CString(x)
		C.godror_dpiJson_setString(jsonnode, cval, C.uint32_t(len(x)))
		C.free(unsafe.Pointer(cval))
	case time.Time:
		data, _ := NewData(x)
		C.godror_dpiJson_setTime(jsonnode, &(data.dpiData))
	case bool:
		data, _ := NewData(x)
		C.godror_dpiJson_setBool(jsonnode, &(data.dpiData))
	default:
		fmt.Printf("unknown type %T\n", in)
	}
    return nil
}

func NewJsonArray(arr []interface{}, jsarr *JSONArray) error {
	var dpijsonnode *C.dpiJsonNode
	C.godror_allocate_dpiNode((**C.dpiJsonNode)(unsafe.Pointer(&dpijsonnode)))
	populateJsonNode(arr, dpijsonnode)
	dpidataw := new(Data)
	jsonNodeToData(dpidataw, dpijsonnode)
	*jsarr = JSONArray{dpiJsonArray: C.dpiData_getJsonArray(&(dpidataw.dpiData)), dpiJsonNode: dpijsonnode}
	return nil
}

func NewJsonObject(m map[string]interface{}, jsobj *JSONObject) error {
	var dpijsonnode *C.dpiJsonNode
	C.godror_allocate_dpiNode((**C.dpiJsonNode)(unsafe.Pointer(&dpijsonnode)))
	populateJsonNode(m, dpijsonnode)
	dpidataw := new(Data)
	jsonNodeToData(dpidataw, dpijsonnode)
	*jsobj = JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(dpidataw.dpiData)), dpiJsonNode: dpijsonnode}
	return nil
}

func (jsobj *JSONObject) Close() error {
	C.godror_dpiJsonfreeMem(jsobj.dpiJsonNode)
	return nil
}

func (j JSONObject) DumpMap(m map[string]Data) {
	var space = " "
	for k, v := range m {
		if v.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_OBJECT {
			var objmap = make(map[string]Data)
			v.GetJSONObject().Get(objmap)
			fmt.Printf("{ \"%v\": \n", k)
			space = space + "\t"
			j.DumpMap(objmap)
			fmt.Printf("}\n")
		} else {
			fmt.Printf("%v %v : %v\n", space, k, v.Get())
		}
	}
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

func (j JSONObject) SetValue(key string, dtvalue *Data) {
	n := int(j.dpiJsonObject.numFields)
	names := ((*[maxArraySize]*C.char)(unsafe.Pointer(j.dpiJsonObject.fieldNames)))[:n:n]
	nameLengths := ((*[maxArraySize]C.uint32_t)(unsafe.Pointer(j.dpiJsonObject.fieldNameLengths)))[:n:n]
	fields := ((*[maxArraySize]C.dpiJsonNode)(unsafe.Pointer(j.dpiJsonObject.fields)))[:n:n]
	for i := 0; i < n; i++ {
		k := C.GoStringN(names[i], C.int(nameLengths[i]))
		if k == key {
			fields[i].value = &((dtvalue).dpiData.value)
			fields[i].oracleTypeNum = C.DPI_ORACLE_TYPE_NUMBER
			fields[i].nativeTypeNum = C.DPI_NATIVE_TYPE_INT64
		}
	}
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

//Define Marshal/UnMarshal for each oracle types like intervalyearmonth, etc
/*
func (t *Timestamp) UnmarshalJSON(b []byte) error {
// Convert to string and remove quotes
    s := strings.Trim(string(b), "\"")

    // Parse the time using the layout
    t, err := time.Parse(layout, s)
    if err != nil {
        return err
    }
    t = b
    return nil
}
*/

func (t Timestamp) MarshalJSON() ([]byte, error) {

	str := "{\"$oracleTimestampTZ\":\"" + time.Time(t).Format(time.RFC3339Nano) + "\"}"
	return []byte(str), nil
}

func (t IntervalYMJson) MarshalJSON() ([]byte, error) {
	str := "{\"$intervalYearMonth\":\"" + t + "\"}"
	return []byte(str), nil
}

//var NullJsonObj = JSONObject{}
//var NullJsonArr = JSONArray{}

func NewJsonValue(in interface{}) (JsonValue, error) {
	v := reflect.ValueOf(in)
	t := v.Type()
	switch t.Kind() {
	case reflect.Map:
		// Add Json Map
		//	outByte, _ := json.Marshal(&in)
		//	return JsonString(outByte), nil
		var writablejsoobj JSONObject
		NewJsonObject(in.(map[string]interface{}), &writablejsoobj)
		return writablejsoobj, nil
	case reflect.Struct:
		// Add Json Map
		outByte, _ := json.Marshal(&in)
		return JsonString(outByte), nil
	case reflect.Ptr:
		// Add Json Map
		var writablejsoobj JSONObject
		NewJsonObject(in.(map[string]interface{}), &writablejsoobj)
		return writablejsoobj, nil
	case reflect.String:
		return JsonString(in.(string)), nil
	case reflect.Slice:
		var writablejsonarr JSONArray
		NewJsonArray(in.([]interface{}), &writablejsonarr)
		return writablejsonarr, nil
	default:
		panic("unsupported doc type: " + t.Name())
	}
	return nil, fmt.Errorf("unsupported key type: ")
}
