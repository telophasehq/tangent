//go:generate go tool wit-bindgen-go generate --world process --out internal ../../wit/tangent:logs@0.1.0.wasm

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"go.bytecodealliance.org/cm"

	"ocsf-go/internal/tangent/logs/processor"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type sinkKey struct {
	name   string
	prefix cm.Option[string]
}
type sinkState struct {
	key sinkKey
	buf *bytes.Buffer
}

type LogOutput struct {
	Sinks []processor.Sink
	Items []json.RawMessage
}

func S3(name string, prefix *string) processor.Sink {
	if prefix != nil {
		return processor.SinkS3(processor.S3Sink{Name: name, KeyPrefix: cm.Some(*prefix)})
	}
	return processor.SinkS3(processor.S3Sink{Name: name, KeyPrefix: cm.None[string]()})
}

type Handler interface {
	// Input: slice of objects decoded.
	// Output: slice of objects to emit.
	ProcessLog(log []byte) (*LogOutput, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[processor.Output], cm.List[processor.Output], string]) {
		in := input.Slice()

		if len(in) == 0 {
			return
		}

		states := make(map[sinkKey]*sinkState, 4)

		start := 0
		for start < len(in) {
			i := bytes.IndexByte(in[start:], '\n')
			if i < 0 {
				if err := processBatch(h, states, in[start:]); err != nil {
					r.SetErr(err.Error())

					return
				}
				break
			}
			if err := processBatch(h, states, in[start:start+i]); err != nil {
				r.SetErr(err.Error())
				return
			}
			start += i + 1
		}

		outputs := make([]processor.Output, 0, len(states))
		for _, st := range states {
			data := st.buf.Bytes()
			sinks := []processor.Sink{processor.SinkS3(processor.S3Sink{
				Name:      st.key.name,
				KeyPrefix: st.key.prefix,
			})}
			outputs = append(outputs, processor.Output{
				Data:  cm.ToList(data),
				Sinks: cm.ToList(sinks),
			})
		}

		for _, st := range states {
			st.buf.Reset()
			bufPool.Put(st.buf)
		}

		r.SetOK(cm.ToList(outputs))
		return
	}
}

func processBatch(h Handler, states map[sinkKey]*sinkState, in []byte) error {
	out, err := h.ProcessLog(in)

	if err != nil {
		return err
	}

	if out == nil || len(out.Sinks) == 0 {
		return nil
	}

	for _, s := range out.Sinks {
		var k sinkKey
		if s3 := s.S3(); s3 != nil {
			k = sinkKey{name: s3.Name, prefix: s3.KeyPrefix}
		} else {
			return fmt.Errorf("unknown sink type")
		}

		st, ok := states[k]
		if !ok {
			buf := bufPool.Get().(*bytes.Buffer)
			buf.Reset()
			st = &sinkState{key: k, buf: buf}
			states[k] = st
		}

		for _, item := range out.Items {
			st.buf.Write(item)
			st.buf.WriteByte('\n')
		}
	}

	return nil
}

func init() {
	Wire(Processor{})
}

func main() {}
