package main

import (
	"bytes"
	"fmt"
	"sync"
	"zeek/internal/tangent/logs/processor"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type sinkKey struct {
	name     string
	prefix   cm.Option[string]
	sinkType string
}
type sinkState struct {
	key sinkKey
	buf *bytes.Buffer
}

type LogOutput struct {
	Sinks []processor.Sink
	Items []any
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
			case "blackhole":
				sink = processor.SinkBlackhole(processor.BlackholeSink{
					Name: st.key.name,
				})
			}
			outputs = append(outputs, processor.Output{
				Data: cm.ToList(st.buf.Bytes()),
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

func sinkKeyFrom(s processor.Sink) (sinkKey, error) {

	if s3 := s.S3(); s3 != nil {
		return sinkKey{name: s3.Name, prefix: s3.KeyPrefix, sinkType: "s3"}, nil
	} else if fileSink := s.File(); fileSink != nil {
		return sinkKey{name: fileSink.Name, sinkType: "file"}, nil
	} else if blackhole := s.Blackhole(); blackhole != nil {
		return sinkKey{name: blackhole.Name, sinkType: "blackhole"}, nil
	} else if s.Default() != nil {
		return sinkKey{sinkType: "default"}, nil
	}

	return sinkKey{}, fmt.Errorf("unknown sink type")
}

func processBatch(h Handler, states map[sinkKey]*sinkState, in []byte) error {
	out, err := h.ProcessLog(in)

	if err != nil {
		return err
	}

	if out == nil {
		return nil
	}

	sinks := out.Sinks
	if len(sinks) == 0 {
		sinks = []processor.Sink{processor.SinkDefault(processor.DefaultSink{})}
	}

	for _, s := range sinks {
		k, err := sinkKeyFrom(s)
		if err != nil {
			return err
		}

		st, ok := states[k]
		if !ok {
			buf := bufPool.Get().(*bytes.Buffer)
			buf.Reset()
			st = &sinkState{key: k, buf: buf}
			states[k] = st
		}

		enc := json.NewEncoder(st.buf)
		enc.SetEscapeHTML(false)

		for _, item := range out.Items {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
	}

	return nil
}

func init() {
	Wire(Processor{})
}

func main() {}
