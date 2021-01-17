package wasmudfutil

import (
	"fmt"
	"hash/crc64"
	"io/ioutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/wasmerio/wasmer-go/wasmer"
)

type WasmType byte

const (
	WasmTypeI64 WasmType = 'I'
	WasmTypeF64 WasmType = 'F'
)

const EntryFnName = "udf_main"
const PersistPath = "/Users/breezewish/Work/PingCAP/runtime-wasm/"

type WasmFnSignature struct {
	RetType    WasmType
	ParamsType []WasmType
}

var Engine *wasmer.Engine
var Store *wasmer.Store

func init() {
	config := wasmer.NewConfig()
	config.UseLLVMCompiler()
	config.UseNativeEngine()
	Engine = wasmer.NewEngineWithConfig(config)
	Store = wasmer.NewStore(Engine)
}

func EvalTypeFromWasmType(t WasmType) types.EvalType {
	switch t {
	case WasmTypeI64:
		return types.ETInt
	case WasmTypeF64:
		return types.ETReal
	default:
		panic("Unsupported WasmType")
	}
}

func FieldTypeFromWasmType(t WasmType) *types.FieldType {
	switch t {
	case WasmTypeI64:
		return types.NewFieldType(mysql.TypeLonglong)
	case WasmTypeF64:
		return types.NewFieldType(mysql.TypeDouble)
	}
	panic("Unsupported WasmType")
}

func NewWasmFnSignatureFromSer(serializedRet, serializedParams string) WasmFnSignature {
	var r WasmFnSignature
	r.RetType = WasmType(serializedRet[0])
	r.ParamsType = make([]WasmType, 0)
	for _, p := range serializedParams {
		r.ParamsType = append(r.ParamsType, WasmType(p))
	}
	return r
}

func (sig *WasmFnSignature) SerializeRet() string {
	return string(sig.RetType)
}

func (sig *WasmFnSignature) SerializeParams() string {
	var r string
	for _, p := range sig.ParamsType {
		r += string(p)
	}
	return r
}

func NewWasmType(vk wasmer.ValueKind) (WasmType, error) {
	switch vk {
	case wasmer.I64:
		return WasmTypeI64, nil
	case wasmer.F64:
		return WasmTypeF64, nil
	default:
		return WasmTypeI64, fmt.Errorf("unsupported WASM data type %s", vk.String())
	}
}

func ParseByteCodeSignatures(bytes []byte) (*WasmFnSignature, error) {
	module, err := wasmer.NewModule(Store, bytes)
	if err != nil {
		return nil, fmt.Errorf("invalid WASM bytecode: %s", err.Error())
	}

	var entryFn *wasmer.FunctionType
	exports := module.Exports()
	for _, ex := range exports {
		if ex.Type().Kind().String() == "func" && ex.Name() == EntryFnName {
			entryFn = ex.Type().IntoFunctionType()
			break
		}
	}
	if entryFn == nil {
		return nil, fmt.Errorf("UDF entry function `%s` not found", EntryFnName)
	}

	var sig WasmFnSignature
	{
		results := entryFn.Results()
		if len(results) == 0 {
			return nil, fmt.Errorf("void return value is not supported")
		} else if len(results) > 1 {
			return nil, fmt.Errorf("multiple return values is not supported")
		} else {
			t, err := NewWasmType(results[0].Kind())
			if err != nil {
				return nil, err
			}
			sig.RetType = t
		}
	}
	{
		sig.ParamsType = make([]WasmType, 0)
		params := entryFn.Params()
		for _, p := range params {
			t, err := NewWasmType(p.Kind())
			if err != nil {
				return nil, err
			}
			sig.ParamsType = append(sig.ParamsType, t)
		}
	}
	return &sig, nil
}

func Checksum(byteCode []byte) uint64 {
	crc64q := crc64.MakeTable(crc64.ECMA)
	return crc64.Checksum(byteCode, crc64q)
}

func PersistByteCode(byteCode []byte) error {
	checksum := Checksum(byteCode)
	path := fmt.Sprintf("%s/%d.wasm", PersistPath, checksum)
	err := ioutil.WriteFile(path, byteCode, 0644)
	if err != nil {
		return errors.Errorf("failed to persist wasm byte code to %s", path)
	}
	return nil
}
