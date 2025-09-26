//go:generate go tool wit-bindgen-go generate --world process --out internal ../../wit/tangent:logs@0.1.0.wasm

package main

import (
	"bytes"
	"errors"
	"io"
	"ocsf-go/internal/tangent/logs/processor"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var outBuf []byte

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
		outBuf = outBuf[:0]

		in := input.Slice()
		dec := json.NewDecoder(bytes.NewReader(in))
		dec.UseNumber()

		enc := json.NewEncoder(byteSliceWriter{&outBuf})
		enc.SetEscapeHTML(false)

		for {
			var m map[string]any
			if err := dec.Decode(&m); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				r.SetErr(err.Error())
				return
			}
			out, err := h.ProcessLog(m)
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
