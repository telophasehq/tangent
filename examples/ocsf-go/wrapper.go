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
)

type byteSliceWriter struct{ p *[]byte }

func (w byteSliceWriter) Write(b []byte) (int, error) {
	*w.p = append(*w.p, b...)
	return len(b), nil
}

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

		type bucket struct {
			sinks []processor.Sink
			buf   []byte
			enc   *json.Encoder
		}
		buckets := make(map[string]*bucket, 64)

		getKeyAndSink := func(s processor.Sink) (key string, single []processor.Sink, ok bool) {
			if s3 := s.S3(); s3 != nil {
				key = s3.Name
				if s3.KeyPrefix.Some() != nil {
					key = s3.Name + "\x00" + s3.KeyPrefix.Value()
				}
				single = []processor.Sink{processor.SinkS3(processor.S3Sink{
					Name: s3.Name, KeyPrefix: s3.KeyPrefix,
				})}
				return key, single, true
			}
			return "", nil, false
		}

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

			if out == nil || len(out.Sinks) == 0 {
				continue
			}

			for _, s := range out.Sinks {
				key, single, ok := getKeyAndSink(s)
				if !ok {
					continue
				}
				b := buckets[key]
				if b == nil {
					b = &bucket{sinks: single}
					b.enc = json.NewEncoder(byteSliceWriter{&b.buf})
					b.enc.SetEscapeHTML(false)
					buckets[key] = b
				}
				if err := b.enc.Encode(out.Data); err != nil {
					r.SetErr(err.Error())
					return
				}
			}
		}

		outSlice := make([]processor.Output, 0, len(buckets))
		for _, b := range buckets {
			outSlice = append(outSlice, processor.Output{
				Sinks: cm.ToList(b.sinks),
				Data:  cm.ToList(b.buf),
			})
		}

		r.SetOK(cm.ToList(outSlice))
		return
	}
}

func init() {
	Wire(Processor{})
}

func main() {}
