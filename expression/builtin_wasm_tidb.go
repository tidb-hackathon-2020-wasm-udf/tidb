// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ functionClass = &wasmTidbFunctionClass{}
)

var (
	_ builtinFunc = &builtinWasmTidbSig{}
)

type wasmTidbFunctionClass struct {
	baseFunctionClass
}

func (c *wasmTidbFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	log.Infof("tidb wasm %v", args)
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.tp)

	sig = &builtinWasmTidbSig{bf}
	return sig, nil
}

type builtinWasmTidbSig struct {
	baseBuiltinFunc
}

func (b *builtinWasmTidbSig) Clone() builtinFunc {
	newSig := &builtinWasmTidbSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinWasmTidbSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	wasm, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	sql, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	os.Setenv("PATH", "/Users/hawkingrei/.wasmer/bin:"+os.ExpandEnv("PATH"))
	result, err := exec.Command("wasmer", "run", string(wasm), string(sql)).Output()
	if err != nil {
		return d, isNull, err
	}
	return string(result), isNull, err
}
