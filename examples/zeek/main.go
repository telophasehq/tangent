package main

import (
	"errors"
	"fmt"
	"zeek/mappers"
	"zeek/tangenthelpers"

	"github.com/segmentio/encoding/json"
)

type Processor struct{}

func (p Processor) ProcessLog(log []byte) (*LogOutput, error) {
	if len(log) == 0 {
		return nil, nil
	}

	if tangenthelpers.Has(log, "conn_state") {
		mapped, err := mappers.MapZeekConn(log)
		if err != nil {
			return nil, err
		}
		return &LogOutput{
			Items: []any{mapped},
		}, nil
	} else if tangenthelpers.Has(log, "answers") {
		mapped, err := mappers.MapZeekDNS(log)
		if err != nil {
			return nil, err
		}
		return &LogOutput{
			Items: []any{mapped},
		}, nil
	}

	var v map[string]any
	json.Unmarshal(log, &v)
	fmt.Println(v)

	return nil, errors.New("unknown log")
}
