//go:generate go tool wit-bindgen-go generate --world process --out internal ../../wit/tangent:logs@0.1.0.wasm

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"ocsf-go/internal/tangent/logs/processor"
	"ocsf-go/row"

	"github.com/vmihailenco/msgpack/v5"
	"go.bytecodealliance.org/cm"
)

var outBuf []byte
var br bytes.Buffer

type byteSliceWriter struct{ p *[]byte }

func (w byteSliceWriter) Write(b []byte) (int, error) {
	*w.p = append(*w.p, b...)
	return len(b), nil
}

type Handler interface {
	ProcessRow(row.Row) ([]any, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		br.Reset()
		dec := msgpack.NewDecoder(&br)

		outBuf = outBuf[:0]
		enc := json.NewEncoder(byteSliceWriter{&outBuf})
		enc.SetEscapeHTML(false)

		for {
			var m map[string]msgpack.RawMessage
			if err := dec.Decode(&m); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				r.SetErr(err.Error())
				return
			}
			out, err := h.ProcessRow(row.Row{Raw: m})
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
