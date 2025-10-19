package main

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"zeek/internal/tangent/logs/processor"

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
			name     string
			prefix   cm.Option[string]
			sinkType string
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

			if out == nil {
				continue
			}

			if len(out.Sinks) == 0 {
				k := sinkKey{sinkType: "default"}

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

			for _, s := range out.Sinks {
				var k sinkKey
				if s3 := s.S3(); s3 != nil {
					k = sinkKey{name: s3.Name, prefix: s3.KeyPrefix, sinkType: "s3"}
				} else if f := s.File(); f != nil {
					k = sinkKey{name: f.Name, sinkType: "file"}
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
			var sink processor.Sink
			switch st.key.sinkType {
			case "default":
				sink = processor.SinkDefault(processor.DefaultSink{})
			case "s3":
				sink = processor.SinkS3(processor.S3Sink{
					Name:      st.key.name,
					KeyPrefix: st.key.prefix,
				})
			case "file":
				sink = processor.SinkFile(processor.FileSink{
					Name: st.key.name,
				})
			}
			outputs = append(outputs, processor.Output{
				Data: cm.ToList(data),
				Sink: sink,
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
