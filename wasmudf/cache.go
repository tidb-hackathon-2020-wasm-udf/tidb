package wasmudf

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/wasmudfutil"
	"github.com/wasmerio/wasmer-go/wasmer"
)

type WASMFn struct {
	ID               int64
	DBLower          string
	NameLower        string
	Module           *wasmer.Module
	ByteCodeCompiled []byte
	Signature        wasmudfutil.WasmFnSignature
}

type WASMFunctions struct {
	Functions []*WASMFn
	FnByID    map[int64]*WASMFn
	FnByName  map[string]map[string]*WASMFn
}

func (fTable *WASMFunctions) LoadAll(sctx sessionctx.Context) error {
	fTable.Functions = make([]*WASMFn, 0)
	fTable.FnByID = make(map[int64]*WASMFn)
	fTable.FnByName = make(map[string]map[string]*WASMFn)
	if err := fTable.loadTable(sctx); err != nil {
		return err
	}
	fTable.buildLookupMap()
	return nil
}

func (fTable *WASMFunctions) buildLookupMap() {
	for _, fn := range fTable.Functions {
		fnP := fn
		fTable.FnByID[fn.ID] = fnP
		if _, ok := fTable.FnByName[fn.DBLower]; !ok {
			fTable.FnByName[fn.DBLower] = make(map[string]*WASMFn)
		}
		fTable.FnByName[fn.DBLower][fn.NameLower] = fnP
	}
}

func (fTable *WASMFunctions) GetFunction(dbLower string, nameLower string) *WASMFn {
	m1, ok := fTable.FnByName[dbLower]
	if !ok {
		return nil
	}
	m2, ok := m1[nameLower]
	if !ok {
		return nil
	}
	return m2
}

func (fTable *WASMFunctions) loadTable(sctx sessionctx.Context) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, "select ID, DB, Name, ByteCode, RetType, ParamsType from mysql.wasm_functions;")
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	req := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			var value WASMFn
			var rStr, pStr string
			for i, f := range fs {
				switch f.ColumnAsName.L {
				case "id":
					value.ID = row.GetInt64(i)
				case "db":
					value.DBLower = row.GetString(i)
				case "name":
					value.NameLower = row.GetString(i)
				case "bytecode":
					module, err := wasmer.NewModule(wasmudfutil.Store, row.GetBytes(i))
					if err != nil {
						return errors.Errorf("failed to load WASM module: %s", err.Error())
					}
					value.Module = module
				case "rettype":
					rStr = row.GetString(i)
				case "paramstype":
					pStr = row.GetString(i)
				}
			}
			value.Signature = wasmudfutil.NewWasmFnSignatureFromSer(rStr, pStr)
			fTable.Functions = append(fTable.Functions, &value)
		}
		req = chunk.Renew(req, sctx.GetSessionVars().MaxChunkSize)
	}
}

type Handle struct {
	p atomic.Value
}

func NewHandle() *Handle {
	return &Handle{}
}

func (h *Handle) Get() *WASMFunctions {
	return h.p.Load().(*WASMFunctions)
}

func (h *Handle) Update(ctx sessionctx.Context) error {
	var fTable WASMFunctions
	err := fTable.LoadAll(ctx)
	if err != nil {
		return err
	}
	h.p.Store(&fTable)
	return nil
}

var WASMHandle *Handle = nil
