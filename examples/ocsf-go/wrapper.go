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
	rdrPool = sync.Pool{New: func() any { return new(bytes.Reader) }}
	mapPool = sync.Pool{
		New: func() any { return make(map[string]any, 64) },
	}
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type LogOutput struct {
	Sinks []processor.Sink
	Data  any
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
	ProcessLog(log map[string]any) (*LogOutput, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[processor.Output], cm.List[processor.Output], string]) {
		in := input.Slice()

		rdr := rdrPool.Get().(*bytes.Reader)
		rdr.Reset(in)
		defer rdrPool.Put(rdr)

		dec := json.NewDecoder(rdr)
		dec.UseNumber()

		type sinkKey struct {
			name   string
			prefix cm.Option[string]
		}
		type sinkState struct {
			key sinkKey
			buf *bytes.Buffer
			enc *json.Encoder
		}

		states := make(map[sinkKey]*sinkState, 4)

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
			clear(m)
			mapPool.Put(m)

			if err != nil {
				r.SetErr(err.Error())
				return
			}

			if out == nil || len(out.Sinks) == 0 {
				continue
			}

			for _, s := range out.Sinks {
				var k sinkKey
				if s3 := s.S3(); s3 != nil {
					k = sinkKey{name: s3.Name, prefix: s3.KeyPrefix}
				} else {
					r.SetErr("unknown sink type")
					return
				}

				st, ok := states[k]
				if !ok {
					buf := bufPool.Get().(*bytes.Buffer)
					buf.Reset()
					enc := json.NewEncoder(buf)
					enc.SetEscapeHTML(false)
					st = &sinkState{key: k, buf: buf, enc: enc}
					states[k] = st
				}

				if err := st.enc.Encode(out.Data); err != nil {
					r.SetErr(err.Error())
					return
				}
			}

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

func init() {
	Wire(Processor{})
}

func main() {}
