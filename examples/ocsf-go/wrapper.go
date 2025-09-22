//go:generate go tool wit-bindgen-go generate --world process --out internal ../../wit/tangent:logs@0.1.0.wasm

package main

import (
	"encoding/json"
	"errors"
	"ocsf-go/internal/tangent/logs/processor"

	"github.com/minio/simdjson-go"
	"go.bytecodealliance.org/cm"
)

var pjReuse *simdjson.ParsedJson
var outBuf []byte

func grow(b []byte, need int) []byte {
	c := cap(b)
	if c >= need {
		return b
	}
	if c < 4096 {
		c = 4096
	}
	for c < need {
		c <<= 1
	}
	nb := make([]byte, len(b), c)
	copy(nb, b)
	return nb
}

func appendBytes(dst []byte, p []byte) []byte {
	dst = grow(dst, len(dst)+len(p))
	dst = dst[:len(dst)+len(p)]
	copy(dst[len(dst)-len(p):], p)
	return dst
}

func appendString(dst []byte, s string) []byte {
	dst = grow(dst, len(dst)+len(s))
	dst = dst[:len(dst)+len(s)]
	copy(dst[len(dst)-len(s):], s)
	return dst
}

type Handler interface {
	// Input: slice of objects decoded with json.Number for numbers.
	// Output: slice of objects to emit as NDJSON (each encoded on its own line).
	ProcessLogs(logs []map[string]any) ([]any, error)
}

func parse(input string, dst []map[string]any) ([]map[string]any, error) {
	// TODO: fallback to json.Unmarshal
	if !simdjson.SupportedCPU() {
		return nil, errors.New("SIMD JSON not supported on this CPU")
	}

	data := []byte(input)

	var pj *simdjson.ParsedJson
	var err error
	pj, err = simdjson.ParseND(data, pjReuse)
	if err != nil {
		return nil, err
	}
	pjReuse = pj

	it := pj.Iter()
	dst = dst[:0]

	var root simdjson.Iter
	var obj simdjson.Object
	for {
		t, r, err := it.Root(&root)
		if err != nil {
			return nil, err
		}
		if t == simdjson.TypeNone {
			break
		}
		if t != simdjson.TypeObject {
			continue
		}
		if _, err = r.Object(&obj); err != nil {
			return nil, err
		}
		m, err := obj.Map(nil)
		if err != nil {
			return nil, err
		}
		dst = append(dst, m)
	}

	return dst, nil
}

func serializeNDJSON(out []byte, objs []any) ([]byte, error) {
	out = out[:0]
	for i := range objs {
		b, err := json.Marshal(objs[i])
		if err != nil {
			return out, err
		}
		out = appendBytes(out, b)
		out = append(out, '\n')
	}
	return out, nil
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input string) (r cm.Result[string, string, string]) {
		var logs []map[string]any
		var err error
		logs, err = parse(input, logs)
		if err != nil {
			r.SetErr(err.Error())
			return
		}

		output, err := h.ProcessLogs(logs)
		if err != nil {
			r.SetErr(err.Error())
			return
		}

		outBuf, err = serializeNDJSON(outBuf, output)
		if err != nil {
			r.SetErr(err.Error())
			return
		}

		r.SetOK(string(outBuf))
		return
	}
}

func main() {}
