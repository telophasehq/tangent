//go:generate go tool wit-bindgen-go generate --world process --out internal ./tangent:logs@0.1.0.wasm

package main

import (
	"basic-go/internal/tangent/logs/processor"
	"basic-go/internal/wasi/io/streams"
	"bytes"
	"encoding/json"
	"unsafe"

	"go.bytecodealliance.org/cm"
)

const (
	chunkSize = 256 * 1024
	ringCap   = 2 * 1024 * 1024
)

var ring = make([]byte, ringCap)
var w int
var obj = make(map[string]any, 64)

type Handler interface {
	ProcessLogs(log map[string]any) error
}

func Wire(h Handler) {
	processor.Exports.ProcessStream = func(input streams.InputStream) (r cm.Result[string, struct{}, string]) {
		defer input.ResourceDrop()
		w = 0

		for {
			res := input.BlockingRead(chunkSize)
			if res.IsErr() {
				r.SetErr(streamErrToString(res.Err()))
				return
			}
			b := res.OK().Slice()
			if len(b) == 0 {
				break // EOF
			}

			if w+len(b) > len(ring) {
				for k := range obj {
					delete(obj, k)
				}
				dec := json.NewDecoder(bytes.NewReader(ring[:w]))
				dec.UseNumber()
				if err := dec.Decode(&obj); err != nil {
					r.SetErr(err.Error())
					return
				}

				if err := h.ProcessLogs(obj); err != nil {
					r.SetErr(err.Error())
					return
				}
				w = 0
			}
			copy(ring[w:], b) // 1 copy, no new allocs
			w += len(b)

			// optional: flush on newline boundaries to keep latency low
			// find last '\n' and process up to there
			if i := lastNL(ring[:w]); i >= 0 {
				for k := range obj {
					delete(obj, k)
				}
				dec := json.NewDecoder(bytes.NewReader(ring[:i+1]))
				dec.UseNumber()
				if err := dec.Decode(&obj); err != nil {
					r.SetErr(err.Error())
					return
				}
				if err := h.ProcessLogs(obj); err != nil {
					r.SetErr(err.Error())
					return
				}
				// move tail down
				tail := w - (i + 1)
				copy(ring[0:], ring[i+1:w])
				w = tail
			}
		}

		// flush any remainder
		if w > 0 {
			for k := range obj {
				delete(obj, k)
			}
			dec := json.NewDecoder(bytes.NewReader(ring[:w]))
			dec.UseNumber()
			if err := dec.Decode(&obj); err != nil {
				r.SetErr(err.Error())
				return
			}
			if err := h.ProcessLogs(obj); err != nil {
				r.SetErr(err.Error())
				return
			}
		}

		r.SetOK(struct{}{}) // if your handler increments internally, return that
		return
	}
}

func lastNL(b []byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == '\n' {
			return i
		}
	}
	return -1
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func streamErrToString(se *streams.StreamError) string {
	if se == nil {
		return "stream error: <nil>"
	}
	if e := se.LastOperationFailed(); e != nil {
		msg := e.ToDebugString()
		e.ResourceDrop()
		return "last-operation-failed: " + msg
	}
	if se.Closed() {
		return "closed"
	}
	return se.String()
}

func main() {}
