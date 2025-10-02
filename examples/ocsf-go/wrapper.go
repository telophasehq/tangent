//go:generate go tool wit-bindgen-go generate --world process --out internal ../../wit/tangent:logs@0.1.0.wasm

package main

import (
	"bytes"
	"errors"
	"io"
	"ocsf-go/internal/tangent/logs/processor"
	"sync"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	outBuf  []byte
	decPool = sync.Pool{New: func() any { return json.NewDecoder(bytes.NewReader(nil)) }}
	encPool = sync.Pool{New: func() any { return json.NewEncoder(io.Discard) }}
	rdrPool = sync.Pool{New: func() any { return new(bytes.Reader) }}
	mapPool = sync.Pool{
		New: func() any { return make(map[string]any, 64) },
	}
)

type byteSliceWriter struct{ p *[]byte }

func (w byteSliceWriter) Write(b []byte) (int, error) {
	*w.p = append(*w.p, b...)
	return len(b), nil
}

type Handler interface {
	// Input: slice of objects decoded.
	// Output: slice of objects to emit.
	ProcessLog(log map[string]any) ([]any, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		in := input.Slice()

		outBuf = outBuf[:0]
		if cap(outBuf) < len(in) {
			outBuf = make([]byte, 0, len(in))
		}

		rdr := rdrPool.Get().(*bytes.Reader)
		rdr.Reset(in)
		defer rdrPool.Put(rdr)

		dec := json.NewDecoder(rdr)
		dec.UseNumber()

		enc := json.NewEncoder(byteSliceWriter{&outBuf})
		enc.SetEscapeHTML(false)

		for {
			m := mapPool.Get().(map[string]any)
			for k := range m {
				delete(m, k)
			}

			if err := dec.Decode(&m); err != nil {
				if errors.Is(err, io.EOF) {
					mapPool.Put(m)
					break
				}
				r.SetErr(err.Error())
				mapPool.Put(m)
				return
			}

			out, err := h.ProcessLog(m)
			for k := range m {
				delete(m, k)
			}
			mapPool.Put(m)

			if err != nil {
				r.SetErr(err.Error())
				return
			}

			for i := range out {
				if err := enc.Encode(out[i]); err != nil {
					r.SetErr(err.Error())
					return
				}
			}
		}

		r.SetOK(cm.ToList(outBuf))
		return
	}
}

func init() {
	Wire(Processor{})
}

func main() {}
