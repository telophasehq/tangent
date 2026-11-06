package main

import (
	tangent_sdk "github.com/ethanblackburn/tangent-sdk-go"
)

//easyjson:json
type ExampleOutput struct {
	Msg      string   `json:"message"`
	Level    string   `json:"level"`
	Seen     int64    `json:"seen"`
	Duration float64  `json:"duration"`
	Service  string   `json:"service"`
	Tags     []string `json:"tags"`
}

var Metadata = tangent_sdk.Metadata{
	Name:    "golang",
	Version: "0.2.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("source.name", "myservice"),
		},
	},
}

func ExampleMapper(lv tangent_sdk.Log) (ExampleOutput, error) {
	var out ExampleOutput
	// Get String
	msg := lv.GetString("msg")
	if msg != nil {
		out.Msg = *msg
	}

	// A path is first read as one key: the input's "msg.level" is a key
	// with a dot in it. Without such a key it walks into objects
	// (source.name below).
	lvl := lv.GetString("msg.level")
	if lvl != nil {
		out.Level = *lvl
	}

	// get int
	seen := lv.GetInt64("seen")
	if seen != nil {
		out.Seen = *seen
	}

	// get float
	duration := lv.GetFloat64("duration")
	if duration != nil {
		out.Duration = *duration
	}

	// get value from nested json
	service := lv.GetString("source.name")
	if service != nil {
		out.Service = *service
	}

	// get string list
	tags, ok := lv.GetStringList("tags")
	if ok {
		out.Tags = tags
	}

	return out, nil
}

func init() {
	tangent_sdk.Wire[ExampleOutput](
		Metadata,
		selectors,
		ExampleMapper,
		nil,
	)
}

func main() {}
