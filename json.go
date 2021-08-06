package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"

int setDPIJson(int * intvalues, dpiDataBuffer *dpijsondatabuffer, dpiJsonObject *jobj) {
//	 dpijsonobj := (*C.dpiJsonObject)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_dpiJsonObject{}))))
jobj->fieldValues = (dpiDataBuffer*)(malloc(2 * sizeof(dpiDataBuffer)));
jobj->fieldValues[0].asInt64 = intvalues[0];
jobj->fieldValues[1].asInt64 = intvalues[0];
dpijsondatabuffer->asJsonObject = *jobj;
return 0;
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

type JSONObject struct {
	dpiJsonObject *C.dpiJsonObject
}

func NewJsonObject(m map[string]Data, jsnode *JSON) error {
	dpijsonnode := new(C.dpiJsonNode)

	NewJsonObjectNode(nil, dpijsonnode)
	jsnode.dpiJsonNode = dpijsonnode
	return nil
}

func NewJsonObjectNode(m map[string]Data, inNodes *C.dpiJsonNode) error {

	inNodeData := new(C.dpiDataBuffer)
	//    var inNodeDatavalues[2] C.dpiDataBuffer
	var intvalues [2]C.int
	dpijsonobj := (*C.dpiJsonObject)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_dpiJsonObject{}))))
	inNodes.value = inNodeData
	var fieldNames [2]*C.char
	fieldNames[0] = C.CString("Fred")
	fieldNames[1] = C.CString("George")
	var fieldNameLengths [2]C.uint32_t
	fieldNameLengths[0] = C.uint32_t(C.strlen(fieldNames[0]))
	fieldNameLengths[1] = C.uint32_t(C.strlen(fieldNames[1]))

	intvalues[0] = C.int(-99)
	intvalues[1] = C.int(-98)
	inNodes.oracleTypeNum = C.DPI_ORACLE_TYPE_JSON_OBJECT
	inNodes.nativeTypeNum = C.DPI_NATIVE_TYPE_JSON_OBJECT
	dpijsonobj.numFields = C.uint32_t(len(m))
	dpijsonobj.fieldNames = (**C.char)(unsafe.Pointer(&fieldNames[0]))
	dpijsonobj.fieldNameLengths = *(**C.uint32_t)(unsafe.Pointer(&fieldNameLengths[0]))
	//dpijsonobj.fieldValues = &inNodeDatavalues[0];
	dpijsonobj.fields = inNodes
	C.setDPIJson((*C.int)(unsafe.Pointer(&intvalues[0])), (*(**C.dpiDataBuffer)(unsafe.Pointer(&inNodeData))), (*(**C.dpiJsonObject)(unsafe.Pointer(&dpijsonobj))))
	var dpidataw Data
	jsonNodeToData(&dpidataw, inNodes)

	jsobj := JSONObject{dpiJsonObject: C.dpiData_getJsonObject(&(dpidataw.dpiData))}
	var objmap = make(map[string]Data)
	jsobj.Get(objmap)
	return nil
}

func (j JSONObject) dumpMap(m map[string]Data) {
	var space = " "
	for k, v := range m {
		if v.NativeTypeNum == C.DPI_NATIVE_TYPE_JSON_OBJECT {
			var objmap = make(map[string]Data)
			v.GetJSONObject().Get(objmap)
			fmt.Printf("{ \"%v\": \n", k)
			space = space + "\t"
			j.dumpMap(objmap)
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
	j.dumpMap(m)
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
		outByte, _ := json.Marshal(&in)
		return JsonString(outByte), nil
	case reflect.Struct:
		// Add Json Map
		outByte, _ := json.Marshal(&in)
		return JsonString(outByte), nil
	case reflect.Ptr:
		// Add Json Map
		outByte, _ := json.Marshal(&in)
		return JsonString(outByte), nil
	case reflect.String:
		return JsonString(in.(string)), nil
	case reflect.Slice:
		//return NullJsonArr, JSONArray(in), nil
		return nil, fmt.Errorf("unsupported key type: %v", t)
	default:
		panic("unsupported doc type: " + t.Name())
	}
	return nil, fmt.Errorf("unsupported key type: ")
}
