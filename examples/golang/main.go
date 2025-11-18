package main

import (
	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
	"github.com/telophasehq/tangent-sdk-go/helpers"
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
	msg := helpers.GetString(lv, "msg")
	if msg != nil {
		out.Msg = *msg
	}

	// get dot path
	lvl := helpers.GetString(lv, "msg.level")
	if lvl != nil {
		out.Level = *lvl
	}

	// get int
	seen := helpers.GetInt64(lv, "seen")
	if seen != nil {
		out.Seen = *seen
	}

	// get float
	duration := helpers.GetFloat64(lv, "duration")
	if duration != nil {
		out.Duration = *duration
	}

	// get value from nested json
	service := helpers.GetString(lv, "source.name")
	if service != nil {
		out.Service = *service
	}

	// get string list
	tags, ok := helpers.GetStringList(lv, "tags")
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
	)
}

func main() {}
