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

type Handler interface {
	// Input: slice of objects decoded.
	// Output: slice of objects to emit.
	ProcessLog(log map[string]any) ([]any, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		outBuf = outBuf[:0]

		in := input.Slice()
		var bb bytes.Buffer
		bb.Grow(len(in) / 2)
		enc := json.NewEncoder(&bb)
		enc.SetEscapeHTML(false)

		dec := json.NewDecoder(bytes.NewReader(in))
		dec.UseNumber()

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
