package main

import (
	"bytes"
	"sync"
	"golang/internal/tangent/logs/log"
	"golang/internal/tangent/logs/mapper"
    "golang/tangenthelpers"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type ExampleOutput struct {
	Msg      string   `json:"message"`
	Level    string   `json:"level"`
	Seen     int64    `json:"seen"`
	Duration float64  `json:"duration"`
	Service  string   `json:"service"`
	Tags     []string `json:"tags"`
}

func Wire() {
    // Metadata is for naming and versioning your plugin.
	mapper.Exports.Metadata = func() mapper.Meta {
		return mapper.Meta{
			Name:    "golang",
			Version: "0.1.0",
		}
	}

    // Probe allows the mapper to subscribe to logs with specific fields.
	mapper.Exports.Probe = func() cm.List[mapper.Selector] {
		return cm.ToList([]mapper.Selector{
			{
				Any: cm.ToList([]mapper.Pred{}),
				All: cm.ToList([]mapper.Pred{
					mapper.PredEq(
						cm.Tuple[string, mapper.Scalar]{
							F0: "source.name",
							F1: log.ScalarStr("myservice"),
						},
					)}),
				None: cm.ToList([]mapper.Pred{}),
			},
		})
	}

    // ProcessLogs takes a batch of logs, transforms, and outputs bytes.
	mapper.Exports.ProcessLogs = func(input cm.List[log.Logview]) (res cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

        // Copy out the slice so we own the backing array.
        // The cm.List view may be backed by a transient buffer that
        // can be reused or mutated after this call, so we take an owned copy.
		var items []log.Logview
		items = append(items, input.Slice()...)
		for idx := range items {
			var out ExampleOutput

			lv := log.Logview(items[idx])

			// Get String
			msg := tangenthelpers.GetString(lv, "msg")
			if msg != nil {
				out.Msg = *msg
			}

			// get dot path
			lvl := tangenthelpers.GetString(lv, "msg.level")
			if lvl != nil {
				out.Level = *lvl
			}

			// get int
			seen := tangenthelpers.GetInt64(lv, "seen")
			if seen != nil {
				out.Seen = *seen
			}

			// get float
			duration := tangenthelpers.GetFloat64(lv, "duration")
			if duration != nil {
				out.Duration = *duration
			}

			// get value from nested json
			service := tangenthelpers.GetString(lv, "source.name")
			if service != nil {
				out.Service = *service
			}

			// get string list
			tags, ok := tangenthelpers.GetStringList(lv, "tags")
			if ok {
				out.Tags = tags
			}

			// Serialize with Segment's encoding/json
			err := json.NewEncoder(buf).Encode(out)
			if err != nil {
				res.SetErr(err.Error()) // error out the entire batch
				return
			}
		}

		res.SetOK(cm.ToList(buf.Bytes()))
		bufPool.Put(buf)
		return
	}
}

func init() {
	Wire()
}

func main() {}
