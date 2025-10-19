package main

import (
	"errors"
	"zeek/mappers"
)

type Processor struct{}

func (p Processor) ProcessLog(log map[string]any) (*LogOutput, error) {
	if _, ok := log["conn_state"]; ok {
		return &LogOutput{
			Data: mappers.MapZeekConn(log),
		}, nil
	} else if _, ok := log["answers"]; ok {
		return &LogOutput{
			Data: mappers.MapZeekDNS(log),
		}, nil
	}

	return nil, errors.New("unknown log")
}
