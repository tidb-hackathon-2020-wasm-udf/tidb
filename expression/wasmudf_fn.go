package expression

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/wasmudf"
	"github.com/pingcap/tidb/wasmudfutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wasmerio/wasmer-go/wasmer"
)

func newWasmFunctionSig(f *wasmudf.WASMFn, ctx sessionctx.Context, args []Expression) (*wasmFunctionSig, error) {
	l := len(args)
	if l != len(f.Signature.ParamsType) {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(f.NameLower)
	}
	evalTypes := make([]types.EvalType, 0, l)
	for _, pt := range f.Signature.ParamsType {
		evalTypes = append(evalTypes, wasmudfutil.EvalTypeFromWasmType(pt))
	}
	retType := wasmudfutil.EvalTypeFromWasmType(f.Signature.RetType)
	bf, err := newBaseBuiltinFuncWithTp(ctx, f.NameLower, args, retType, evalTypes...)
	if err != nil {
		return nil, err
	}
	// TODO: Set PbCode
	//sig.setPbCode(tipb.ScalarFuncSig_MD5)
	importObject := wasmer.NewImportObject()
	instance, err := wasmer.NewInstance(f.Module, importObject)
	if err != nil {
		return nil, errors.Errorf("failed to initialize WASM module: %s", err.Error())
	}
	entry, err := instance.Exports.GetFunction(wasmudfutil.EntryFnName)
	if err != nil {
		return nil, errors.Errorf("failed to find WASM entry: %s", err.Error())
	}
	sig := &wasmFunctionSig{bf, f.Signature, entry}
	sig.setPbCode(tipb.ScalarFuncSig_WasmUdf)
	sig.setWasmId(f.CRC)
	return sig, nil
}

type wasmFunctionSig struct {
	baseBuiltinFunc
	sig wasmudfutil.WasmFnSignature
	fn  func(...interface{}) (interface{}, error)
}

func (b *wasmFunctionSig) Clone() builtinFunc {
	newSig := &wasmFunctionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.sig = b.sig
	newSig.fn = b.fn
	return newSig
}

func evalArgsToInterface(b baseBuiltinFunc, row chunk.Row, paramTypes []wasmudfutil.WasmType) ([]interface{}, bool, error) {
	args := make([]interface{}, 0, len(paramTypes))
	for idx, pt := range paramTypes {
		switch pt {
		case wasmudfutil.WasmTypeI64:
			arg, isNull, err := b.args[idx].EvalInt(b.ctx, row)
			if isNull || err != nil {
				return nil, isNull, err
			}
			args = append(args, arg)
		case wasmudfutil.WasmTypeF64:
			arg, isNull, err := b.args[idx].EvalReal(b.ctx, row)
			if isNull || err != nil {
				return nil, isNull, err
			}
			args = append(args, arg)
		default:
			panic(fmt.Sprintf("Unsupported ParamsType %c", pt))
		}
	}
	return args, false, nil
}

func (b *wasmFunctionSig) evalInt(row chunk.Row) (int64, bool, error) {
	if b.sig.RetType != wasmudfutil.WasmTypeI64 {
		return 0, false, errors.Errorf("wasmFunctionSig.evalInt() should never be called")
	}

	args, isNull, err := evalArgsToInterface(b.baseBuiltinFunc, row, b.sig.ParamsType)
	if isNull || err != nil {
		return 0, isNull, err
	}

	ret, err := b.fn(args...)
	if err != nil {
		return 0, false, err
	}

	v, ok := ret.(int64)
	if !ok {
		return 0, false, errors.Errorf("invalid WASM return type")
	}

	return v, false, nil
}

func (b *wasmFunctionSig) evalReal(row chunk.Row) (float64, bool, error) {
	if b.sig.RetType != wasmudfutil.WasmTypeF64 {
		return 0, false, errors.Errorf("wasmFunctionSig.evalReal() should never be called")
	}

	args, isNull, err := evalArgsToInterface(b.baseBuiltinFunc, row, b.sig.ParamsType)
	if isNull || err != nil {
		return 0, isNull, err
	}

	ret, err := b.fn(args...)
	if err != nil {
		return 0, false, err
	}

	v, ok := ret.(float64)
	if !ok {
		return 0, false, errors.Errorf("invalid WASM return type")
	}

	return v, false, nil
}

func (b *wasmFunctionSig) evalString(row chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf("wasmFunctionSig.evalString() should never be called")
}

func (b *wasmFunctionSig) vectorized() bool {
	return false
}
